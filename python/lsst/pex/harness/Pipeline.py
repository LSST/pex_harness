#! /usr/bin/env python

from lsst.pex.harness.Queue import Queue
from lsst.pex.harness.Stage import Stage
from lsst.pex.harness.Clipboard import Clipboard
from lsst.pex.harness.Directories import Directories
from lsst.pex.logging import Log, LogRec, cout
from lsst.pex.harness import harnessLib as pipeline

import lsst.pex.policy as policy

import lsst.pex.exceptions
from lsst.pex.exceptions import *

import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.daf.persistence as dafPersist
from lsst.daf.persistence import *


import lsst.ctrl.events as events

import os
import sys
import traceback
import threading

from cStringIO import StringIO

"""
Pipeline class manages the operation of a multi-stage parallel pipeline.
The Pipeline is configured by reading a Policy file.   This Python Pipeline
class imports the C++ Pipeline class via a python extension module in order 
to setup and manage the MPI environment.
Pipeline has a __main__ portion as it serves as the main executable program 
("glue layer") for running a Pipeline. The Pipeline spawns Slice workers 
using an MPI-2 Spawn operation. 
"""

class Pipeline:
    '''Python Pipeline class implementation. Contains main pipeline workflow'''

    def __init__(self, runId='-1', pipelinePolicyName=None):
        """
        Initialize the Pipeline: create empty Queue and Stage lists;
        import the C++ Pipeline instance; initialize the MPI environment
        """
        self.queueList = []
        self.stageList = []
        self.stageClassList = []
        self.stagePolicyList = []
        self.eventTopicList = []
        self.eventReceiverList = []
        self.shareDataList = []
        self.clipboardList = []
        self.executionMode = 0
        self.cppPipeline = pipeline.Pipeline()
        self.cppPipeline.setRunId(runId)
        self.cppPipeline.setPolicyName(pipelinePolicyName)
        self.cppPipeline.initialize()
        self.universeSize = self.cppPipeline.getUniverseSize()
        self._runId = runId
        self.pipelinePolicyName = pipelinePolicyName

        # we'll use these in our logging
        self.statstart = dafBase.PropertySet()
        self.statend   = dafBase.PropertySet()

        self.statstart.setString("STATUS", str("start"))
        self.statend.setString("STATUS", str("end"))
        

    def __del__(self):
        """
        Delete the Pipeline object: clean up
        """
        self.log.log(Log.DEBUG, 'Python Pipeline being deleted')

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

        # Obtain the working directory space locators
        psLookup = lsst.daf.base.PropertySet()
        if (p.exists('dir')):
            dirPolicy = p.get('dir')
            dirs = Directories(dirPolicy, self._runId)
            psLookup = dirs.getDirs()
        if (topPolicy.exists('configuration.execute.database')):
            psLookup.set('dbUrl', topPolicy.get('configuration.execute.database.url'))

        # Check for eventBrokerHost 
        if (p.exists('eventBrokerHost')):
            self.eventBrokerHost = p.getString('eventBrokerHost')
        else:
            self.eventBrokerHost = "lsst8.ncsa.uiuc.edu"   # default value

	eventSystem = events.EventSystem.getDefaultEventSystem()
	eventSystem.createTransmitter(self.eventBrokerHost, "LSSTLogging")
        events.EventLog.createDefaultLog(self._runId, -1)

        # Check for localLogMode
        if (p.exists('localLogMode')):
            self.localLogMode = p.getBool('localLogMode')
        else:
            self.localLogMode = False   # default value

        if (self.localLogMode == True):
            # Initialize the logger in C++ to add a ofstream
            self.cppPipeline.initializeLogger(True)
        else:
            self.cppPipeline.initializeLogger(False)

        # The log for use in the Python Pipeline
        root     = Log.getDefaultLog()
        self.log = Log(root, "pex.harness.pipeline")
        self.log.setThreshold(Log.DEBUG)
        self.log.addDestination(cout, Log.DEBUG);

        psUniv  = dafBase.PropertySet()
        psRunid = dafBase.PropertySet()
        psUniv.setInt("universeSize", self.universeSize)
        psRunid.setString("runID", self._runId)

        log = Log(self.log, "configurePipeline")
        lr = LogRec(log, Log.INFO)
        lr << "Initialized the Logger" \
           << psUniv.toString(False) << psRunid.toString(False)
        lr << LogRec.endr

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

        psSharing  = dafBase.PropertySet()
        psSharing.setBool("isDataSharingOn", self.isDataSharingOn)

        lr = LogRec(log, Log.INFO)
        lr << psSharing
        lr << LogRec.endr


        # Process Application Stages
        fullStageList = p.getArray("appStage")

        lr = LogRec(log, Log.INFO)
        lr << "Read Stage list"
        count = 1
        fullStageNameList = [ ]
        for item in fullStageList:
            fullStageNameList.append(item.getString("stageName"))
            psAppStage  = dafBase.PropertySet()
            psAppStage.setString("appStage" + str(count), item.getString("stageName"))
            lr << psAppStage
            count += 1
        lr << LogRec.endr

        self.nStages = len(fullStageList)
        self.stageNames = [ ] 
        for astage in fullStageNameList:
            fullStage = astage.strip()
            tokenList = astage.split('.')
            classString = tokenList.pop()
            classString = classString.strip()
            self.stageNames.append(classString)

            package = ".".join(tokenList)

            # For example  package -> lsst.pex.harness.App1Stage  classString -> App1Stage
            AppStage = __import__(package, globals(), locals(), [classString], -1)
            StageClass = getattr(AppStage, classString)
            self.stageClassList.append(StageClass) 


        lr = LogRec(log, Log.INFO)
        lr << "Imported Stages"
        lr << LogRec.endr

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

        lr = LogRec(log, Log.INFO)
        lr << "Read event trigger topics"
        count = 1
        for item in self.eventTopicList:
            psTopic  = dafBase.PropertySet()
            psTopic.setString("eventTopic" + str(count), item)
            lr << psTopic.toString(False)
            count = count + 1
        lr << LogRec.endr


        # Make a List of corresponding eventReceivers for the eventTopics
        # eventReceiverList    
        for topic in self.eventTopicList:
            eventReceiver = events.EventReceiver(self.eventBrokerHost, topic)
            self.eventReceiverList.append(eventReceiver)

        # Process Stage Policies
        # inputStagePolicy = policy.Policy.createPolicy("policy/inputStage.policy")
        self.stagePolicyList = [ ]
        for item in fullStageList:
            self.stagePolicyList.append(item.getString("stagePolicy"))

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
            lr = LogRec(log, Log.INFO)
            lr << "Read topology"
            psTop0  = dafBase.PropertySet()
            psTop0.setString("topology_type", self.topology)
            lr << psTop0
            lr << LogRec.endr


        
        lr = LogRec(log, Log.INFO)
        lr << "End configurePipeline"
        lr << LogRec.endr

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
        log = Log(self.log, "initializeStages")
        lr = LogRec(log, Log.INFO)
        lr << "Start initializeStages" 
        lr << LogRec.endr

        for iStage in range(1, self.nStages+1):
            # Make a Policy object for the Stage Policy file
            policyFileName = self.stagePolicyList[iStage-1] 
            # Make an instance of the specifies Application Stage
            # Use a constructor with the Policy as an argument 
            StageClass = self.stageClassList[iStage-1]
            if (policyFileName != "None"):
                stagePolicy = policy.Policy.createPolicy(policyFileName)
                stageObject = StageClass(iStage, stagePolicy)
            else:
                stageObject = StageClass(iStage)
            inputQueue  = self.queueList[iStage-1]
            outputQueue = self.queueList[iStage]
            stageObject.initialize(outputQueue, inputQueue)
            stageObject.setUniverseSize(self.universeSize)
            stageObject.setRun(self._runId)
            # stageObject.setLookup(self._lookup)
            self.stageList.append(stageObject)

        lr = LogRec(log, Log.INFO)
        lr << "End initializeStages" 
        lr << LogRec.endr

    def startSlices(self):
        """
        Initialize the Queue by defining an initial dataset list
        """
        log = Log(self.log, "startSlices")
        lr = LogRec(log, Log.INFO)
        lr << "Start startSlices: spawning Slices" 
        lr << LogRec.endr

        self.cppPipeline.startSlices()

        lr = LogRec(log, Log.INFO)
        lr << "End startSlices" 
        lr << LogRec.endr

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

        eventReceiver = events.EventReceiver(self.eventBrokerHost, self.shutdownTopic)

        looplog = Log(self.log, "startStagesLoop", Log.INFO)

        visitcount = 0 
        while True:

            val = eventReceiver.receive(100)
            if ((val != None) or ((self.executionMode == 1) and (visitcount == 1))):
                LogRec(looplog, Log.INFO)  << "terminating slices "
                self.cppPipeline.invokeShutdown()
                break
            else:
                visitcount += 1
                loopnum  = dafBase.PropertySet()
                loopnum.setInt("loopnum", visitcount)
                LogRec(looplog, Log.INFO)                        \
                       << "Starting Visit Loop iteration: loopnum " + str(visitcount)  \
                       << loopnum  << self.statstart << LogRec.endr
                self.cppPipeline.invokeContinue()
                self.startInitQueue()    # place an empty clipboard in the first Queue

                self.errorFlagged = 0
                for iStage in range(1, self.nStages+1):

                    psStage  = dafBase.PropertySet()
                    psStage.setInt("iStage", iStage);

                    LogRec(looplog, Log.INFO) \
                        << "Top Stage Loop" \
                        << loopnum \
                        << psStage << LogRec.endr

                    stage = self.stageList[iStage-1]

                    self.handleEvents(iStage)

                    self.tryPreProcess(iStage, stage)

                    if(self.isDataSharingOn):
                        self.invokeSyncSlices(iStage) 

                    self.cppPipeline.invokeProcess(iStage)

                    self.tryPostProcess(iStage, stage)

                    LogRec(looplog, Log.INFO) \
                        << "Bottom Stage Loop" \
                        << loopnum \
                        << psStage << LogRec.endr
                else:
                    # Done iStage for loop
                    LogRec(looplog, Log.INFO)            \
                           << "End Visit Loop iteration"   \
                           << loopnum << self.statend << LogRec.endr

            # Uncomment to print a list of Citizens after each visit 
            # print datap.Citizen_census(0,0), "Objects:"
            # print datap.Citizen_census(datap.cout,0)

            self.log.log(Log.DEBUG, 'Retrieving finalClipboard for deletion')
            finalQueue = self.queueList[self.nStages]
            finalClipboard = finalQueue.getNextDataset()
            self.log.log(Log.DEBUG, "deleting final clipboard")
            # delete entries on the clipboard
            finalClipboard.close()
            del finalClipboard

        self.log.log(Log.INFO, "Shutting down pipeline");
        self.shutdown()


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


    def invokeSyncSlices(self, iStage):
        """
        If needed, calls the C++ Pipeline invokeSyncSlices
        """

        invlog = Log(self.log, "invokeSyncSlices", Log.INFO)

        invlog.log(Log.INFO, "Start invokeSyncSlices", self.statstart)
        if(self.shareDataList[iStage-1]):
            self.cppPipeline.invokeSyncSlices(); 
        invlog.log(Log.INFO, "End invokeSyncSlices", self.statend)

    def tryPreProcess(self, iStage, stage):
        """
        Executes the try/except construct for Stage preprocess() call 
        """

        prelog = Log(self.log, "tryPreProcess", Log.INFO)

        # Important try - except construct around stage preprocess() 
        try:
            # If no error/exception has been flagged, run preprocess()
            # otherwise, simply pass along the Clipboard 
            if (self.errorFlagged == 0):
                prelog.log(Log.INFO, "Starting preprocess", self.statstart)
                stage.preprocess()
                prelog = Log(self.log, "tryPreProcess", Log.INFO)
                prelog.log(Log.INFO, "Ending preprocess", self.statend)
            else:
                self.transferClipboard(iStage)

        except Exception, e:

            lr = LogRec(prelog, Log.FATAL)
            lr << "Exception " + "Type = " + str(type(e)) \
               << "Message = " + str(e) << LogRec.endr

            # Flag that an exception occurred to guide the framework to skip processing
            self.errorFlagged = 1
            # Post the cliphoard that the Stage failed to transfer to the output queue
            self.postOutputClipboard(iStage)

        prelog.log(Log.INFO, "Ending tryPreProcess", self.statend)
        # Done try - except around stage preprocess 

    def tryPostProcess(self, iStage, stage):
        """
        Executes the try/except construct for Stage postprocess() call 
        """

        postlog = Log(self.log, "tryPostProcess", Log.INFO)

        # Important try - except construct around stage postprocess() 
        try:
            # If no error/exception has been flagged, run postprocess()
            # otherwise, simply pass along the Clipboard 
            if (self.errorFlagged == 0):
                postlog.log(Log.INFO,"Starting postprocess",self.statstart)
                stage.postprocess()
                postlog = Log(self.log, "tryPostProcess", Log.INFO)
                postlog.log(Log.INFO, "Ending postprocess", self.statend)
            else:
                self.transferClipboard(iStage)

        except Exception, e:

            # Use str(e) or  e.args[0].what() for message  
            lr = LogRec(postlog, Log.FATAL)
            lr << "Exception " + "Type = " + str(type(e)) \
               << "Message = " + str(e) << LogRec.endr

            # Flag that an exception occurred to guide the framework to skip processing
            self.errorFlagged = 1
            # Post the cliphoard that the Stage failed to transfer to the output queue
            self.postOutputClipboard(iStage)

        # Done try - except around stage preprocess 

    def handleEvents(self, iStage):
        """
        Handles Events: transmit or receive events as specified by Policy
        """
        log = Log(self.log, "handleEvents");

        psStage  = dafBase.PropertySet()
        psStage.setInt("iStage", iStage);

        lr = LogRec(log, Log.INFO)
        lr << "Start handleEvents"
        lr << psStage
        lr << LogRec.endr

        thisTopic = self.eventTopicList[iStage-1]
        thisTopic = thisTopic.strip()

        psTopic  = dafBase.PropertySet()
        psTopic.setString("Topic", thisTopic);

        lr = LogRec(log, Log.INFO)
        lr << "Processing topic"
        lr << psTopic
        lr << LogRec.endr

        if (thisTopic != "None"):
            fileStr = StringIO()
            fileStr.write(thisTopic)
            fileStr.write("_slice")
            sliceTopic = fileStr.getvalue()
            eventReceiver    = self.eventReceiverList[iStage-1]
            eventTransmitter = events.EventTransmitter(self.eventBrokerHost, sliceTopic)

            log.log(Log.INFO, "waiting on receive...")

            inputParamPropertySetPtr = eventReceiver.receive(self.eventTimeout)

            if (inputParamPropertySetPtr != None):
                log.log(Log.INFO, "received event; sending it to Slices")

                # Pipeline  does not disassemble the payload of the event.
                # It knows nothing of the contents.
                # It simply places the payload on the clipboard with key of the eventTopic
                self.populateClipboard(inputParamPropertySetPtr, iStage, thisTopic)
                eventTransmitter.publish(inputParamPropertySetPtr)

                log.log(Log.DEBUG, "event sent")
        else:
            log.log(Log.INFO, 'No event to handle')

        lr = LogRec(log, Log.INFO)
        lr << "End handleEvents"
        lr << psStage
        lr << LogRec.endr

    def populateClipboard(self, inputParamPropertySetPtr, iStage, eventTopic):
        """
        Place the event payload onto the Clipboard 
        """
        log = Log(self.log, "populateClipboard");
        log.log(Log.DEBUG,'Python Pipeline populateClipboard');

        queue = self.queueList[iStage-1]
        clipboard = queue.getNextDataset()

        # Pipeline does not disassemble the payload of the event.
        # It knows nothing of the contents.
        # It simply places the payload on the clipboard with key of the eventTopic
        clipboard.put(eventTopic, inputParamPropertySetPtr)

        queue.addDataset(clipboard)

        print 'Python populateClipboard  : clipboard ', clipboard

        psClip = dafBase.PropertySet()
        # psClip.setInt64("clipboard", id(clipboard))
        psInput = dafBase.PropertySet()
        # psInput.setInt64("inputParamPropertySetPtr", id(inputParamPropertySetPtr))

        lr = LogRec(log, Log.INFO)
        lr << "End populateClipboard"
        # lr << psClip.toString(False)
        # lr << psInput.toString(False)
        lr << LogRec.endr

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


