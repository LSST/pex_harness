#! /usr/bin/env python

from lsst.pex.harness.Queue import Queue
from lsst.pex.harness.Stage import Stage
from lsst.pex.harness.Clipboard import Clipboard
from lsst.pex.harness.harnessLib import TracingLog
from lsst.pex.harness.Directories import Directories
from lsst.pex.logging import Log, LogRec, Prop
from lsst.pex.harness import harnessLib as slice

import lsst.pex.policy as policy
import lsst.pex.exceptions as ex

import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.daf.persistence as dafPersist
from lsst.daf.persistence import *


import lsst.ctrl.events as events
import lsst.pex.exceptions
from lsst.pex.exceptions import *

import os, sys, re, traceback
import threading


"""
Slice represents a single parallel worker program.  
Slice executes the loop of Stages for processing a portion of an Image (e.g.,
single ccd or amplifier). The processing is synchonized with serial processing
in the main Pipeline via MPI communications.  This Python Slice class accesses 
the C++ Slice class via a python extension module to obtain access to the MPI 
environment. 
A Slice obtains its configuration by reading a policy file. 
Slice has a __main__ portion as it serves as the executable program
"spawned" within the MPI-2 Spawn of parallel workers in the C++ Pipeline 
implementation. 
"""

class Slice:
    '''Slice: Python Slice class implementation. Wraps C++ Slice'''

    #------------------------------------------------------------------------
    def __init__(self, runId="TEST", pipelinePolicyName=None):
        """
        Initialize the Slice: create an empty Queue List and Stage List;
        Import the C++ Slice  and initialize the MPI environment
        """
        # log message levels
        self.TRACE = TracingLog.TRACE
        self.VERB1 = self.TRACE
        self.VERB2 = self.VERB1 - 1
        self.VERB3 = self.VERB2 - 1
        self.log = None
        self.logthresh = None
        
        self.queueList = []
        self.stageList = []
        self.stageClassList = []
        self.stagePolicyList = []
        self.sliceEventTopicList = []
        self.eventTopicList = []
        self.eventReceiverList = []
        self.shareDataList = []
        self.shutdownTopic = "triggerShutdownEvent_slice"
        self._runId = runId
        self.pipelinePolicyName = pipelinePolicyName
        self.cppSlice = slice.Slice()
        self.cppSlice.setRunId(runId)
        self.cppSlice.initialize()
        self._rank = self.cppSlice.getRank()
        self.universeSize = self.cppSlice.getUniverseSize()


    def __del__(self):
        """
        Delete the Slice object: cleanup 
        """
        if self.log is not None:
            self.log.log(self.VERB1, 'Python Slice being deleted')

    def configureSlice(self):
        """
        Configure the slice via reading a Policy file 
        """

        if(self.pipelinePolicyName == None):
            self.pipelinePolicyName = "pipeline_policy.paf"
        dictName = "pipeline_dict.paf"
        topPolicy = policy.Policy.createPolicy(self.pipelinePolicyName, False)

        if (topPolicy.exists('execute')):
            p = topPolicy.get('execute')
        else:
            p = policy.Policy.createPolicy(self.pipelinePolicyName, False)

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
        self.cppSlice.setEventBrokerHost(self.eventBrokerHost);

        doLogFile = p.getBool('localLogMode')
        self.cppSlice.initializeLogger(doLogFile)

        # The log for use in the Python Pipeline
        self.log = self.cppSlice.getLogger()
        if self.logthresh is None:
            self.logthresh = p.get('logThreshold')
        if self.logthresh is not None:
            self.log.setThreshold(self.logthresh)
        else:
            self.logthresh = self.log.getThreshold()

        log = Log(self.log, "configurePipeline")
        log.log(Log.INFO,
                "Logging messages using threshold=%i" % log.getThreshold())
        LogRec(log, self.VERB1) << "Configuring Slice"        \
                                << Prop("universeSize", self.universeSize) \
                                << Prop("runID", self._runId) \
                                << Prop("rank", self._rank)   \
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

        # extract the stage class name and associated policy file.  
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

        log.log(self.VERB2, "Imported Stage Classes")

        # Process Event Topics
        self.eventTopicList = [ ]
        self.sliceEventTopicList = [ ]
        for item in fullStageList:
            self.eventTopicList.append(item.getString("eventTopic"))
            self.sliceEventTopicList.append(item.getString("eventTopic"))

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

        count = 0
        for item in self.eventTopicList:
            newitem = item + "_slice"
            self.sliceEventTopicList[count] = newitem
            count += 1

        # Make a List of corresponding eventReceivers for the eventTopics
        # eventReceiverList    
        for topic in self.sliceEventTopicList:
            if (topic == "None_slice"):
                self.eventReceiverList.append(None)
            else:
                eventReceiver = events.EventReceiver(self.eventBrokerHost, topic)
                self.eventReceiverList.append(eventReceiver)

        # Process topology policy
        if (p.exists('topology')):
            # Retrieve the topology policy and set it in C++
            topologyPolicy = p.get('topology')
            self.cppSlice.setTopology(topologyPolicy);

            # Diagnostic print
            self.topology   =  topologyPolicy.getString('type')
            log.log(self.VERB3, "Using topology type: %s" % self.topology)

            # Calculate this Slice's neighbors 
            self.cppSlice.calculateNeighbors();

        log.log(self.VERB1, "Slice configuration complete");

    def initializeQueues(self):
        """
        Initialize the Queue List
        """
        for iQueue in range(1, self.nStages+1+1):
            queue = Queue()
            self.queueList.append(queue)

    def initializeStages(self):
        """
        Initialize the Stage List
        """
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
            stageObject.setRank(self._rank)
            stageObject.setUniverseSize(self.universeSize)
            stageObject.setRun(self._runId)
            stageObject.initialize(outputQueue, inputQueue)
            # stageObject.setLookup(self._lookup)
            self.stageList.append(stageObject)

    def startInitQueue(self):
        """
        Place an empty Clipboard in the first Queue
        """
        clipboard = Clipboard()
        queue1 = self.queueList[0]
        queue1.addDataset(clipboard)

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
        queue2.addDataset(clipboard)

    def startStagesLoop(self): 
        """
        Execute the Stage loop. The loop progressing in step with 
        the analogous stage loop in the central Pipeline by means of
        MPI Bcast and Barrier calls.
        """
        startStagesLoopLog = self.log.traceBlock("startStagesLoop", self.TRACE)
        looplog = TracingLog(self.log, "visit", self.TRACE)
        stagelog = TracingLog(looplog, "stage", self.TRACE-1)

        visitcount = 0
        while True:
            visitcount += 1
            looplog.setPreamblePropertyInt("loopnum", visitcount)
            looplog.start()
            stagelog.setPreamblePropertyInt("loopnum", visitcount)

            self.cppSlice.invokeShutdownTest()
            looplog.log(self.VERB3, "Tested for Shutdown")

            self.startInitQueue()    # place an empty clipboard in the first Queue

            self.errorFlagged = 0
            for iStage in range(1, self.nStages+1):
                stagelog.setPreamblePropertyInt("stageId", iStage)
                stagelog.start(self.stageNames[iStage-1] + " loop")

                stageObject = self.stageList[iStage-1]
                self.handleEvents(iStage, stagelog)

                if(self.isDataSharingOn):
                    self.syncSlices(iStage, stagelog) 

                self.tryProcess(iStage, stageObject, stagelog)

                stagelog.done()

            looplog.log(self.VERB2, "Completed Stage Loop")

            # If no error/exception was flagged, 
            # then clear the final Clipboard in the final Queue

            if self.errorFlagged == 0:
                looplog.log(Log.DEBUG,
                            "Retrieving final Clipboard for deletion")
                finalQueue = self.queueList[self.nStages]
                finalClipboard = finalQueue.getNextDataset()
                finalClipboard.close()
                del finalClipboard
                looplog.log(Log.DEBUG, "Deleted final Clipboard")
            else:
                looplog.log(self.VERB3, "Error flagged on this visit")
            looplog.done()

        startStagesLoopLog.done()

    def shutdown(self): 
        """
        Shutdown the Slice execution
        """
        shutlog = Log(self.log, "shutdown", Log.INFO);
        shutlog.log(Log.INFO, "Shutting down Slice")
        self.cppSlice.shutdown()

    def syncSlices(self, iStage, stageLog):
        """
        If needed, performs interSlice communication prior to Stage process
        """
        synclog = stageLog.traceBlock("syncSlices", self.TRACE-1);

        if(self.shareDataList[iStage-1]):
            synclog.log(Log.DEBUG, "Sharing Clipboard data")

            queue = self.queueList[iStage-1]
            clipboard = queue.getNextDataset()
            sharedKeys = clipboard.getSharedKeys()

            synclog.log(Log.DEBUG, "Obtained %d sharedKeys" % len(sharedKeys))

            for skey in sharedKeys:

                synclog.log(Log.DEBUG,
                        "Executing C++ syncSlices for keyToShare: " + skey)

                psPtr = clipboard.get(skey)
                newPtr = self.cppSlice.syncSlices(psPtr)
                valuesFromNeighbors = newPtr.toString(False)

                neighborList = self.cppSlice.getRecvNeighborList()
                for element in neighborList:
                    neighborKey = skey + "-" + str(element)
                    nKey = "neighbor-" + str(element)
                    propertySetPtr = newPtr.getAsPropertySetPtr(nKey)
                    testString = propertySetPtr.toString(False)
                    clipboard.put(neighborKey, propertySetPtr, False)
                    synclog.log(Log.DEBUG,
                                "Added to Clipboard: %s: %s" % (neighborKey,
                                                                testString))
                    
                synclog.log(Log.DEBUG,
                        "Received PropertySet: " + valuesFromNeighbors)

            queue.addDataset(clipboard)

        synclog.done()

    def tryProcess(self, iStage, stage, stagelog):
        """
        Executes the try/except construct for Stage process() call 
        """
        # Important try - except construct around stage process() 
        proclog = stagelog.traceBlock("tryProcess", self.TRACE-2);

        stageObject = self.stageList[iStage-1]
        proclog.log(self.VERB3, "Getting process signal from Pipeline")
        self.cppSlice.invokeBcast(iStage)

        # Important try - except construct around stage process() 
        try:
            # If no error/exception has been flagged, run process()
            # otherwise, simply pass along the Clipboard 
            if (self.errorFlagged == 0):
                processlog = stagelog.traceBlock("process", self.TRACE)
                stageObject.process()
                processlog.done()
            else:
                proclog.log(self.TRACE, "Skipping process due to error")
                self.transferClipboard(iStage)
  
        ### raise lsst.pex.exceptions.LsstException("Terrible Test Exception")
        except:
            trace = "".join(traceback.format_exception(
                sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            proclog.log(Log.FATAL, trace)

            # Flag that an exception occurred to guide the framework to skip processing
            self.errorFlagged = 1
            # Post the cliphoard that the Stage failed to transfer to the output queue
            self.postOutputClipboard(iStage)


        proclog.log(self.VERB3, "Getting end of process signal from Pipeline")
        self.cppSlice.invokeBarrier(iStage)
        proclog.done()

    def handleEvents(self, iStage, stagelog):
        """
        Handles Events: transmit or receive events as specified by Policy
        """
        log = stagelog.traceBlock("handleEvents", self.TRACE-2)

        thisTopic = self.eventTopicList[iStage-1]

        if (thisTopic != "None"):
            log.log(self.VERB3, "Processing topic: " + thisTopic)
            sliceTopic = self.sliceEventTopicList[iStage-1]
            # x = events.EventReceiver(self.eventBrokerHost, sliceTopic)
            x  = self.eventReceiverList[iStage-1]

            waitlog = log.traceBlock("eventwait", self.TRACE,
                                     "wait for event...")
            inputParamPropertySetPtr = x.receive(self.eventTimeout)
            waitlog.done()

            self.populateClipboard(inputParamPropertySetPtr, iStage, thisTopic)
            log.log(self.VERB3, 'Received event; added payload to clipboard')
        else:
            log.log(Log.DEBUG, 'No event to handle')

        log.done()

    def populateClipboard(self, inputParamPropertySetPtr, iStage, eventTopic):
        """
        Place the event payload onto the Clipboard
        """
        log = Log(self.log, "populateClipboard");
        log.log(Log.DEBUG,'Python Pipeline populateClipboard');

        queue = self.queueList[iStage-1]
        clipboard = queue.element()

        # Slice does not disassemble the payload of the event. 
        # It knows nothing of the contents. 
        # It simply places the payload on the clipboard with key of the eventTopic
        clipboard.put(eventTopic, inputParamPropertySetPtr)

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
        @return int   the threshold value as would be returned by 
                         Log.getThreshold()
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
        @param level   the threshold level as expected by Log.setThreshold().
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
    Slice Main execution 
    """

    pySlice = Slice()

    pySlice.configureSlice()   

    pySlice.initializeQueues()     

    pySlice.initializeStages()   

    pySlice.startInitQueue()

    pySlice.startStagesLoop()

    pySlice.shutdown()

