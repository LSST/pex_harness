#! /usr/bin/env python

from lsst.pex.harness.Queue import Queue
from lsst.pex.harness.Stage import Stage
from lsst.pex.harness.Clipboard import Clipboard
from lsst.pex.harness.Directories import Directories
from lsst.pex.harness.harnessLib import TracingLog
from lsst.pex.logging import Log, LogRec, cout, Prop
from lsst.pex.harness import harnessLib as pipeline

import lsst.pex.policy as policy

import lsst.pex.exceptions
from lsst.pex.exceptions import *

import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.daf.persistence as dafPersist
from lsst.daf.persistence import *


import lsst.ctrl.events as events

import os, sys, re, traceback
import threading

from cStringIO import StringIO

"""
Pipeline class manages the operation of a multi-stage parallel pipeline.
The Pipeline is configured by reading a Policy file.   This Python Pipeline
class imports the C++ Pipeline class via a python extension module in order 
to setup and manage the MPI environment.
Pipeline has a __main__ portion as it serves as the main executable program 
('glue layer') for running a Pipeline. The Pipeline spawns Slice workers 
using an MPI-2 Spawn operation. 
"""

class Pipeline:
    '''Python Pipeline class implementation. Contains main pipeline workflow'''

    def __init__(self, runId='-1', pipelinePolicyName=None, name="unnamed"):
        """
        Initialize the Pipeline: create empty Queue and Stage lists;
        import the C++ Pipeline instance; initialize the MPI environment
        """
        # log message levels
        self.TRACE = TracingLog.TRACE
        self.VERB1 = self.TRACE
        self.VERB2 = self.VERB1 - 1
        self.VERB3 = self.VERB2 - 1
        self.log = None
        self.logthresh = None

        self._pipelineName = name
        
        self.queueList = []
        self.stageList = []
        self.stageClassList = []
        self.stagePolicyList = []
        self.eventTopicList = []
        self.eventReceiverList = []
        self.shareDataList = []
        self.clipboardList = []
        self.executionMode = 0
        self.cppPipeline = pipeline.Pipeline(self._pipelineName)
        self.cppPipeline.setRunId(runId)
        self.cppPipeline.setPolicyName(pipelinePolicyName)
        self.cppPipeline.initialize()
        self.universeSize = self.cppPipeline.getUniverseSize()
        self._runId = runId
        self.pipelinePolicyName = pipelinePolicyName


    def __del__(self):
        """
        Delete the Pipeline object: clean up
        """
        if self.log is not None:
            self.log.log(self.VERB1, 'Python Pipeline being deleted')

    def configurePipeline(self):
        """
        Configure the Pipeline by reading a Policy File
        """

        if(self.pipelinePolicyName == None):
            self.pipelinePolicyName = "pipeline_policy.paf"
        dictName = "pipeline_dict.paf"
        topPolicy = policy.Policy.createPolicy(self.pipelinePolicyName)

        if (topPolicy.exists('execute')):
            p = topPolicy.get('execute')
        else:
            p = policy.Policy.createPolicy(self.pipelinePolicyName)

        # do some juggling to capture the actual stage policy names.  We'll
        # use these to assign some logical names to the stages for logging
        # purposes.  Note, however, that it is only convention that the
        # stage policies will be specified as separate files; thus, we need
        # a fallback.  
        stgcfg = p.getArray("appStage")
        self.stageNames = []
        for item in stgcfg:
            self.stageNames.append(self.makeStageName(item))
        p.loadPolicyFiles()

        # Obtain the working directory space locators
        psLookup = lsst.daf.base.PropertySet()
        if (p.exists('dir')):
            dirPolicy = p.get('dir')
            shortName = p.get('shortName')
            if shortName == None:
                shortName = self.pipelinePolicyName.split('.')[0]
            dirs = Directories(dirPolicy, shortName, self._runId)
            psLookup = dirs.getDirs()
        if (p.exists('database.url')):
            psLookup.set('dbUrl', p.get('database.url'))

        # Check for eventBrokerHost 
        if (p.exists('eventBrokerHost')):
            self.eventBrokerHost = p.getString('eventBrokerHost')
        else:
            self.eventBrokerHost = "lsst8.ncsa.uiuc.edu"   # default value
        self.cppPipeline.setEventBrokerHost(self.eventBrokerHost);

        doLogFile = p.getBool('localLogMode')
        self.cppPipeline.initializeLogger(doLogFile)

        # The log for use in the Python Pipeline
        self.log = self.cppPipeline.getLogger()
        if self.logthresh is None:
            self.logthresh = p.get('logThreshold')
        if self.logthresh is not None:
            self.log.setThreshold(self.logthresh)
        else:
            self.logthresh = self.log.getThreshold()
        self.log.addDestination(cout, Log.DEBUG);

        log = Log(self.log, "configurePipeline")
        log.log(Log.INFO,
                "Logging messages using threshold=%i" % log.getThreshold())
        LogRec(log, self.VERB1) << "Configuring pipeline"        \
                                << Prop("universeSize", self.universeSize) \
                                << Prop("runID", self._runId) \
                                << Prop("rank", -1)   \
                                << LogRec.endr;
        
        # Configure persistence logical location map with values for directory 
        # work space locators
        dafPersist.LogicalLocation.setLocationMap(psLookup)

        # Check for eventTimeout
        if (p.exists('eventTimeout')):
            self.eventTimeout = p.getInt('eventTimeout')
        else:
            self.eventTimeout = 10000000   # default value

        # Check if inter-Slice communication, i.e., data sharing, is on
        self.isDataSharingOn = False;
        if (p.exists('shareDataOn')):
            self.isDataSharingOn = p.getBool('shareDataOn')

        if self.isDataSharingOn:
            log.log(self.VERB2, "Data sharing is on")
        else:
            log.log(self.VERB2, "Data sharing is off")

        # Process Application Stages
        fullStageList = p.getArray("appStage")
        self.nStages = len(fullStageList)
        log.log(self.VERB2, "Found %d stages" % len(fullStageList))

        fullStageNameList = [ ]
        self.stagePolicyList = [ ]
        for stagei in xrange(self.nStages):
            item = fullStageList[stagei]
            fullStageNameList.append(item.getString("stageName"))
            self.stagePolicyList.append(item.get("stagePolicy"))
            if self.stageNames[stagei] is None:
                self.stageNames[stagei] = fullStageNameList[-1].split('.')[-1]
            log.log(self.VERB3,
                    "Stage %d: %s: %s" % (stagei+1, self.stageNames[stagei],
                                          fullStageNameList[-1]))
        for astage in fullStageNameList:
            fullStage = astage.strip()
            tokenList = astage.split('.')
            classString = tokenList.pop()
            classString = classString.strip()

            package = ".".join(tokenList)

            # For example  package -> lsst.pex.harness.App1Stage  classString -> App1Stage
            AppStage = __import__(package, globals(), locals(), [classString], -1)
            StageClass = getattr(AppStage, classString)
            self.stageClassList.append(StageClass) 


        # Process Event Topics
        self.eventTopicList = []
        for item in fullStageList:
            self.eventTopicList.append(item.getString("eventTopic"))

        # Process Share Data Schedule
        self.shareDataList = []
        for item in fullStageList:
            shareDataStage = False
            if (item.exists('shareData')):
                shareDataStage = item.getBool('shareData')
            self.shareDataList.append(shareDataStage)

        log.log(self.VERB3, "Loading in %d trigger topics" % \
                len(filter(lambda x: x != "None", self.eventTopicList)))

        for iStage in xrange(len(self.eventTopicList)):
            item = self.eventTopicList[iStage]
            if self.eventTopicList[iStage] != "None":
                log.log(self.VERB3, "eventTopic%d: %s" % (iStage+1, item))
            else:
                log.log(Log.DEBUG, "eventTopic%d: %s" % (iStage+1, item))

        # Make a List of corresponding eventReceivers for the eventTopics
        # eventReceiverList    
        for topic in self.eventTopicList:
            if (topic == "None"):
                self.eventReceiverList.append(None)
            else:
                eventReceiver = events.EventReceiver(self.eventBrokerHost, topic)
                self.eventReceiverList.append(eventReceiver)

        # Check for executionMode of oneloop 
        if (p.exists('executionMode') and (p.getString('executionMode') == "oneloop")):
            self.executionMode = 1 

        # Check for shutdownTopic 
        if (p.exists('shutdownTopic')):
            self.shutdownTopic = p.getString('shutdownTopic')
        else:
            self.shutdownTopic = "triggerShutdownEvent"

        # Check for exitTopic 
        if (p.exists('exitTopic')):
            self.exitTopic = p.getString('exitTopic')
        else:
            self.exitTopic = None

        # Log the input topology
        if (p.exists('topology')):
            # Retrieve the topology policy and set it in C++
            topologyPolicy = p.get('topology')

            # Diagnostic print
            self.topology   =  topologyPolicy.getString('type')
            log.log(self.VERB3, "Using topology type: %s" % self.topology)

        log.log(self.VERB1, "Pipeline configuration complete");

    def initializeQueues(self):
        """
        Initialize the Queue List for the Pipeline
        """
        for iQueue in range(1, self.nStages+1+1):
            queue = Queue()
            self.queueList.append(queue)

    def initializeStages(self):
        """
        Initialize the Stage List for the Pipeline
        """
        log = self.log.traceBlock("initializeStages", self.TRACE-2)

        for iStage in range(1, self.nStages+1):
            # Make a Policy object for the Stage Policy file
            stagePolicy = self.stagePolicyList[iStage-1] 
            # Make an instance of the specifies Application Stage
            # Use a constructor with the Policy as an argument 
            StageClass = self.stageClassList[iStage-1]
            if (stagePolicy != "None"):
                stageObject = StageClass(iStage, stagePolicy)
            else:
                stageObject = StageClass(iStage)
            inputQueue  = self.queueList[iStage-1]
            outputQueue = self.queueList[iStage]
            stageObject.setUniverseSize(self.universeSize)
            stageObject.setRun(self._runId)
            stageObject.setEventBrokerHost(self.eventBrokerHost)
            # stageObject.setLookup(self._lookup)
            stageObject.initialize(outputQueue, inputQueue)
            self.stageList.append(stageObject)

        log.done()

    def startSlices(self):
        """
        Initialize the Queue by defining an initial dataset list
        """
        log = self.log.traceBlock("startSlices", self.TRACE-2)
        self.cppPipeline.startSlices()
        log.done()

    def startInitQueue(self):
        """
        Place an empty Clipboard in the first Queue
        """
        clipboard = Clipboard()
        #self.clipboardList.append(clipboard)

        #print "Python Pipeline Clipboard check \n"
        #acount=0
        #for clip in self.clipboardList:
        #    acount+=1
        #    print acount
        #    print str(clip)

        queue1 = self.queueList[0]
        queue1.addDataset(clipboard)

    def startStagesLoop(self): 
        """
        Method to execute loop over Stages
        """
        startStagesLoopLog = self.log.traceBlock("startStagesLoop", self.TRACE)
        looplog = TracingLog(self.log, "visit", self.TRACE)
        stagelog = TracingLog(looplog, "stage", self.TRACE-1)

        eventReceiver = events.EventReceiver(self.eventBrokerHost, self.shutdownTopic)
        visitcount = 0 
        while True:

            val = eventReceiver.receive(100)
            if ((val != None) or ((self.executionMode == 1) and (visitcount == 1))):
                LogRec(looplog, Log.INFO)  << "terminating slices "
                self.cppPipeline.invokeShutdown()
                break
            else:
                visitcount += 1
                looplog.setPreamblePropertyInt("loopnum", visitcount)
                looplog.start()
                stagelog.setPreamblePropertyInt("loopnum", visitcount)

                self.cppPipeline.invokeContinue()
                self.startInitQueue()    # place an empty clipboard in the first Queue

                self.errorFlagged = 0
                for iStage in range(1, self.nStages+1):
                    stagelog.setPreamblePropertyInt("stageId", iStage)
                    stagelog.start(self.stageNames[iStage-1] + " loop")

                    stage = self.stageList[iStage-1]

                    self.handleEvents(iStage, stagelog)

                    self.tryPreProcess(iStage, stage, stagelog)

                    if(self.isDataSharingOn):
                        self.invokeSyncSlices(iStage, stagelog)

                    self.cppPipeline.invokeProcess(iStage)

                    self.tryPostProcess(iStage, stage, stagelog)

                    stagelog.done()

                else:
                    looplog.log(self.VERB2, "Completed Stage Loop")

            # Uncomment to print a list of Citizens after each visit 
            # print datap.Citizen_census(0,0), "Objects:"
            # print datap.Citizen_census(datap.cout,0)

            looplog.log(Log.DEBUG, 'Retrieving finalClipboard for deletion')
            finalQueue = self.queueList[self.nStages]
            finalClipboard = finalQueue.getNextDataset()
            looplog.log(Log.DEBUG, "deleting final clipboard")
            # delete entries on the clipboard
            finalClipboard.close()
            del finalClipboard

        startStagesLoopLog.log(Log.INFO, "Shutting down pipeline");
        self.shutdown()
        startStagesLoopLog.done()

    def shutdown(self): 
        """
        Shutdown the Pipeline execution: delete the MPI environment
        Send the Exit Event if required
        """
        if self.exitTopic == None:
            pass
        else:
            oneEventTransmitter = events.EventTransmitter(self.eventBrokerHost, self.exitTopic)
            psPtr = dafBase.PropertySet()
            psPtr.setString("message", str("exiting_") + self._runId )

            oneEventTransmitter.publish(psPtr)

        self.cppPipeline.shutdown()


    def invokeSyncSlices(self, iStage, stagelog):
        """
        If needed, calls the C++ Pipeline invokeSyncSlices
        """
        invlog = stagelog.traceBlock("invokeSyncSlices", self.TRACE-1)
        if(self.shareDataList[iStage-1]):
            self.cppPipeline.invokeSyncSlices(); 
        invlog.done()

    def tryPreProcess(self, iStage, stage, stagelog):
        """
        Executes the try/except construct for Stage preprocess() call 
        """
        prelog = stagelog.traceBlock("tryPreProcess", self.TRACE-2);

        # Important try - except construct around stage preprocess() 
        try:
            # If no error/exception has been flagged, run preprocess()
            # otherwise, simply pass along the Clipboard 
            if (self.errorFlagged == 0):
                processlog = stagelog.traceBlock("preprocess", self.TRACE)
                stage.preprocess()
                processlog.done()
            else:
                prelog.log(self.TRACE, "Skipping process due to error")
                self.transferClipboard(iStage)

        except:
            trace = "".join(traceback.format_exception(
                    sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            prelog.log(Log.FATAL, trace)

            # Flag that an exception occurred to guide the framework to skip processing
            self.errorFlagged = 1
            # Post the cliphoard that the Stage failed to transfer to the output queue
            self.postOutputClipboard(iStage)

        prelog.done()
        # Done try - except around stage preprocess 

    def tryPostProcess(self, iStage, stage, stagelog):
        """
        Executes the try/except construct for Stage postprocess() call 
        """
        postlog = stagelog.traceBlock("tryPostProcess",self.TRACE-2);

        # Important try - except construct around stage postprocess() 
        try:
            # If no error/exception has been flagged, run postprocess()
            # otherwise, simply pass along the Clipboard 
            if (self.errorFlagged == 0):
                processlog = stagelog.traceBlock("postprocess", self.TRACE)
                stage.postprocess()
                processlog.done()
            else:
                postlog.log(self.TRACE, "Skipping process due to error")
                self.transferClipboard(iStage)

        except:
            trace = "".join(traceback.format_exception(
                    sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            postlog.log(Log.FATAL, trace)

            # Flag that an exception occurred to guide the framework to skip processing
            self.errorFlagged = 1
            # Post the cliphoard that the Stage failed to transfer to the output queue
            self.postOutputClipboard(iStage)

        # Done try - except around stage preprocess
        postlog.done()

    def handleEvents(self, iStage, stagelog):
        """
        Handles Events: transmit or receive events as specified by Policy
        """
        log = stagelog.traceBlock("handleEvents", self.TRACE-2)

        thisTopic = self.eventTopicList[iStage-1]
        thisTopic = thisTopic.strip()

        if (thisTopic != "None"):
            log.log(self.VERB3, "Processing topic: " + thisTopic)
            fileStr = StringIO()
            fileStr.write(thisTopic)
            fileStr.write("_slice")
            sliceTopic = fileStr.getvalue()
            eventReceiver    = self.eventReceiverList[iStage-1]
            eventTransmitter = events.EventTransmitter(self.eventBrokerHost, sliceTopic)

            waitlog = log.traceBlock("eventwait", self.TRACE,
                                     "wait for event...")
            inputParamPropertySetPtr = eventReceiver.receive(self.eventTimeout)
            waitlog.done()

            if (inputParamPropertySetPtr != None):
                log.log(self.VERB2, "received event; sending it to Slices")

                # Pipeline  does not disassemble the payload of the event.
                # It knows nothing of the contents.
                # It simply places the payload on the clipboard with key of the eventTopic
                self.populateClipboard(inputParamPropertySetPtr, iStage, thisTopic)
                eventTransmitter.publish(inputParamPropertySetPtr)

                log.log(self.VERB2, "event sent")
        else:
            log.log(Log.DEBUG, 'No event to handle')

        log.done()

    def populateClipboard(self, inputParamPropertySetPtr, iStage, eventTopic):
        """
        Place the event payload onto the Clipboard 
        """
        log = self.log.traceBlock("populateClipboard", self.TRACE-2);

        queue = self.queueList[iStage-1]
        clipboard = queue.element()

        # Pipeline does not disassemble the payload of the event.
        # It knows nothing of the contents.
        # It simply places the payload on the clipboard with key of the eventTopic
        clipboard.put(eventTopic, inputParamPropertySetPtr)

        # print 'Python populateClipboard  : clipboard ', clipboard

        log.done()

    def postOutputClipboard(self, iStage):
        """
        Place an empty Clipboard in the output queue for designated stage
        """
        clipboard = Clipboard()
        queue2 = self.queueList[iStage]
        queue2.addDataset(clipboard)

    def transferClipboard(self, iStage):
        """
        Move the Clipboard from the input queue to output queue for the designated stage
        """
        # clipboard = Clipboard()
        queue1 = self.queueList[iStage-1]
        queue2 = self.queueList[iStage]
        clipboard = queue1.getNextDataset()
        if (clipboard != None):
            queue2.addDataset(clipboard)

# print __doc__

    #------------------------------------------------------------------------
    def getRun(self):
        """
        get method for the runId
        """
        return self._runId

    #------------------------------------------------------------------------
    def setRun(self, run):
        """
        set method for the runId
        """
        self._runId = run

    def getLogThreshold(self):
        """
        return the default message importance threshold being used for 
        recording messages.  The returned value reflects the threshold 
        associated with the default root (system-wide) logger (or what it will
        be after logging is initialized).  Some underlying components may 
        override this threshold.
        """
        if self.log is None:
            return self.logthresh
        else:
            return Log.getDefaultLog().getThreshold()

    def setLogThreshold(self, level):
        """
        set the default message importance threshold to be used for 
        recording messages.  This will value be applied to the default
        root (system-wide) logger (or what it will be after logging is 
        initialized) so that all software components are affected.
        """
        if self.log is not None:
            Log.getDefaultLog().setThreshold(level)
            self.log.log(Log.INFO, 
                         "Upating Root Log Message Threshold to %i" % level)
        self.logthresh = level

    def makeStageName(self, appStagePolicy):
        if appStagePolicy.getValueType("stagePolicy") == appStagePolicy.FILE:
            pfile = os.path.splitext(os.path.basename(
                        appStagePolicy.getFile("stagePolicy").getPath()))[0]
            return trailingpolicy.sub('', pfile)
        else:
            return None
        
trailingpolicy = re.compile(r'_*(policy|dict)$', re.IGNORECASE)

if (__name__ == '__main__'):
    """
     Pipeline Main method 
    """

    pyPipeline = Pipeline()

    pyPipeline.configurePipeline()   

    pyPipeline.initializeQueues()  

    pyPipeline.initializeStages()    

    pyPipeline.startSlices()  

    pyPipeline.startInitQueue()    # place an empty clipboard in the first Queue 

    pyPipeline.startStagesLoop()

    pyPipeline.shutdown()


