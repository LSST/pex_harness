#! /usr/bin/env python

from lsst.dps.Queue import Queue
from lsst.dps.Stage import Stage
from lsst.dps.Clipboard import Clipboard
from lsst.mwi.logging import Log, LogRec, cout

import lsst.mwi.policy as policy
import lsst.mwi.exceptions as ex

import lsst.mwi.data as datap
from lsst.mwi.data import *

from lsst.mwi.exceptions import *

import lsst.events as events

import os
import sys
import traceback

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
        self.clipboardList = []
        self.executionMode = 0
        import pipeline
        self.cppPipeline = pipeline.Pipeline()
        self.cppPipeline.setRunId(runId)
        self.cppPipeline.setPolicyName(pipelinePolicyName)
        self.cppPipeline.initialize()
        self.universeSize = self.cppPipeline.getUniverseSize()
        self._runId = runId
        self.pipelinePolicyName = pipelinePolicyName

        # we'll use these in our logging
        self.statstart = DataProperty("STATUS", str("start"))
        self.statend = DataProperty("STATUS", str("end"))
        

    def __del__(self):
        """
        Delete the Pipeline object: clean up
        """
        self.log.log(Log.DEBUG, 'Python Pipeline being deleted')

    def configurePipeline(self):
        """
        Configure the Pipeline by reading a Policy File
        """
        ### self.log.log(Log.DEBUG, "configuring pipeline");
        # self.sliceTopic = "slicedata"

        # path1 = os.environ['LSST_POLICY_DIR']
        # print 'Python Pipeline path1', path1
      
        # 
        if(self.pipelinePolicyName == None):
            self.pipelinePolicyName = "pipeline_policy.paf"
        dictName = "pipeline_dict.paf"
        p = policy.Policy.createPolicy(self.pipelinePolicyName)

        # Check for activemqBroker 
        if (p.exists('activemqBroker')):
            self.activemqBroker = p.getString('activemqBroker')
        else:
            self.activemqBroker = "lsst8.ncsa.uiuc.edu"   # default value

	eventSystem = events.EventSystem.getDefaultEventSystem()
	eventSystem.createTransmitter(self.activemqBroker, "LSSTLogging")
        events.EventLog.createDefaultLog(self._runId, -1)

        self.log = Log(Log.getDefaultLog(), "dps.pipeline")
        self.log.addDestination(cout, Log.DEBUG);
        
        initlog = Log(self.log, "init")
        LogRec(initlog, Log.INFO) << "Initializing Pipeline" \
              << DataProperty("universeSize", self.universeSize) << LogRec.endr

        # cppPipeline.configure()

        # Check for eventTimeout
        if (p.exists('eventTimeout')):
            self.eventTimeout = p.getInt('eventTimeout')
        else:
            self.eventTimeout = 10000000   # default value

        # Process Application Stages
        fullStageList = p.getArray("appStages")

        lr = LogRec(self.log, Log.INFO)
        lr << "Loading stages"
        for item in fullStageList:
            lr << DataProperty("appStage", item)
        lr << LogRec.endr

        # filePolicy = open('pipeline.policy', 'r')
        # fullStageList = filePolicy.readlines()

        self.nStages = len(fullStageList)
        self.stageNames = [ ] 
        for astage in fullStageList:
            fullStage = astage.strip()
            tokenList = astage.split('.')
            classString = tokenList.pop()
            classString = classString.strip()
            self.stageNames.append(classString)

            package = ".".join(tokenList)

            # For example  package -> lsst.dps.App1Stage  classString -> App1Stage
            AppStage = __import__(package, globals(), locals(), [classString], -1)
            StageClass = getattr(AppStage, classString)
            self.stageClassList.append(StageClass) 

        # Process Event Topics
        self.eventTopicList = p.getArray("eventTopics")

        lr = LogRec(self.log, Log.INFO)
        lr << "Loading event topics"
        for item in self.eventTopicList:
            lr << DataProperty("appStage", item)
        lr << LogRec.endr

        self.log.log(Log.INFO, "runID is " + self._runId)

        # Make a List of corresponding eventReceivers for the eventTopics
        # eventReceiverList    
        for topic in self.eventTopicList:
            eventReceiver = events.EventReceiver(self.activemqBroker, topic)
            self.eventReceiverList.append(eventReceiver)

        # Process Stage Policies
        # inputStagePolicy = policy.Policy.createPolicy("policy/inputStage.policy")
        self.stagePolicyList = p.getArray("stagePolicies")

        # Check for executionMode of oneloop 
        if (p.exists('executionMode') and (p.getString('executionMode') == "oneloop")):
            self.executionMode = 1 

        # Check for executionMode of oneloop 
        if (p.exists('shutdownTopic')):
            self.shutdownTopic = p.getString('shutdownTopic')
        else:
            self.shutdownTopic = "triggerShutdownEvent"

        self.log.log(Log.DEBUG, "configuration done")

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
            self.stageList.append(stageObject)


    def startSlices(self):
        """
        Initialize the Queue by defining an initial dataset list
        """
        self.log.log(Log.DEBUG, "starting slices")
        self.cppPipeline.startSlices()
        self.log.log(Log.DEBUG, "slices started")

    def startInitQueue(self):
        """
        Place an empty Clipboard in the first Queue
        """
        clipboard = Clipboard()
        self.clipboardList.append(clipboard)

        print "Python Pipeline Clipboard check \n"
        acount=0
        for clip in self.clipboardList:
            acount+=1
            print acount
            print str(clip)

        queue1 = self.queueList[0]
        queue1.addDataset(clipboard)

    def startStagesLoop(self): 
        """
        Method to execute loop over Stages
        """

        eventReceiver = events.EventReceiver(self.activemqBroker, self.shutdownTopic)

        looplog = Log(self.log, "visit", Log.INFO);
        prelog = Log(looplog, "preprocess", Log.INFO);
        postlog = Log(looplog, "postprocess", Log.INFO);

        count = 0 
        while True:

            val = eventReceiver.receive(100)
            if ((val.get() != None) or ((self.executionMode == 1) and (count == 1))):
                LogRec(looplog, Log.INFO)  << "terminating slices "
                self.cppPipeline.invokeShutdown()
                break
            else:
                count += 1
                loopnum = DataProperty("loopNumber", count);
                LogRec(looplog, Log.INFO)                        \
                       << "starting stage loop number " + str(count)  \
                       << loopnum  << self.statstart << LogRec.endr
                self.cppPipeline.invokeContinue()
                self.startInitQueue()    # place an empty clipboard in the first Queue

                self.errorFlagged = 0
                for iStage in range(1, self.nStages+1):

                    stage = self.stageList[iStage-1]

                    self.handleEvents(iStage)

                    # Important try - except construct around stage preprocess() 
                    try:
                        # If no error/exception has been flagged, run preprocess()
                        # otherwise, simply pass along the Clipboard 
                        if (self.errorFlagged == 0):
                            prelog.log(Log.INFO, "Starting preprocess", self.statstart)
                            stage.preprocess()
                            prelog.log(Log.INFO, "Ending preprocess", self.statend)
                        else:
                            self.transferClipboard(iStage)

                    except LsstExceptionStack,e:

                        # Log / Report the Exception
                        tb = traceback.format_exception(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2])
                        prelog.log(Log.FATAL, tb[-1].strip())
                        prelog.log(Log.WARN, "".join(tb[0:-1]))
        
                        # Acquire the entire exception stack
                        stackPtr = e.getStack()
                        lastPtr = e.getLast()
        
                        stackString = stackPtr.toString("stack: ", 1)
                        lr = LogRec(prelog, Log.WARN)
                        lr << "Exception stack: " + stackString \
                           << lastPtr.get() << LogRec.endr

                        # Flag that an exception occurred to guide the framework to skip processing
                        self.errorFlagged = 1
                        # Post the cliphoard that the Stage failed to transfer to the output queue
                        self.postOutputClipboard(iStage)

                    except:
                        # Log / Report the Exception
                        tb = traceback.format_exception(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2])
                        prelog.log(Log.FATAL, tb[-1].strip())
                        prelog.log(Log.WARN, "".join(tb[0:-1]))

                        # Flag that an exception occurred to guide the framework to skip processing
                        self.errorFlagged = 1
                        # Post the cliphoard that the Stage failed to transfer to the output queue
                        self.postOutputClipboard(iStage)

                    # Done try - except around stage preprocess 

                    self.cppPipeline.invokeProcess(iStage)

                    # Important try - except construct around stage postprocess() 
                    try:
                        # If no error/exception has been flagged, run postprocess()
                        # otherwise, simply pass along the Clipboard 
                        if (self.errorFlagged == 0):
                            postlog.log(Log.INFO,"Starting postprocess",self.statstart)
                            stage.postprocess()
                            postlog.log(Log.INFO, "Ending postprocess", self.statend)
                        else:
                            self.transferClipboard(iStage)

                    except LsstExceptionStack,e:

                        # Log / Report the Exception
                        tb = traceback.format_exception(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2])
                        postlog.log(Log.FATAL, tb[-1].strip())
                        postlog.log(Log.WARN, "".join(tb[0:-1]))
        
                        # Acquire the entire exception stack
                        stackPtr = e.getStack()
                        lastPtr = e.getLast()
        
                        stackString = stackPtr.toString("stack: ", 1)
                        lr = LogRec(postlog, Log.WARN)
                        lr << "Exception stack: " + stackString \
                           << lastPtr.get() << LogRec.endr

                        # Flag that an exception occurred to guide the framework to skip processing
                        self.errorFlagged = 1
                        # Post the cliphoard that the Stage failed to transfer to the output queue
                        self.postOutputClipboard(iStage)

                    except:
                        # Log / Report the Exception
                        tb = traceback.format_exception(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2])
                        postlog.log(Log.FATAL, tb[-1].strip())
                        postlog.log(Log.WARN, "".join(tb[0:-1]))

                        # Flag that an exception occurred to guide the framework to skip processing
                        self.errorFlagged = 1
                        # Post the cliphoard that the Stage failed to transfer to the output queue
                        self.postOutputClipboard(iStage)

                    # Done try - except around stage preprocess 
           
                else:
                    LogRec(looplog, Log.INFO)                        \
                           << "starting stage loop number " + str(count)  \
                           << loopnum << self.statend << LogRec.endr

            self.log.log(Log.DEBUG, 'Retrieving finalClipboard for deletion')
            finalQueue = self.queueList[self.nStages]
            finalClipboard = finalQueue.getNextDataset()
            self.log.log(Log.DEBUG, "deleting final clipboard")
            del finalClipboard

        self.log.log(Log.INFO, "Shutting down pipeline");
        self.shutdown()


    def shutdown(self): 
        """
        Shutdown the Pipeline execution: delete the MPI environment 
        """
        self.cppPipeline.shutdown()


    def handleEvents(self, iStage):
        """
        Handles Events: transmit or receive events as specified by Policy
        """
        log = Log(self.log, "handleEvents");
        log.log(Log.INFO, "iStage %d" % iStage)

        thisTopic = self.eventTopicList[iStage-1]
        thisTopic = thisTopic.strip()
        log.log(Log.DEBUG, "processing topic " + thisTopic)

        if (thisTopic != "None"):
            fileStr = StringIO()
            fileStr.write(thisTopic)
            fileStr.write("_slice")
            sliceTopic = fileStr.getvalue()
            #  Replace 
            # eventReceiver    = events.EventReceiver(self.activemqBroker, thisTopic)
            eventReceiver    = self.eventReceiverList[iStage-1]
            eventTransmitter = events.EventTransmitter(self.activemqBroker, sliceTopic)

            log.log(Log.INFO, "waiting on receive...")

            inputParamPropertyPtrType = eventReceiver.receive(self.eventTimeout)

            if (inputParamPropertyPtrType.get() != None):
                log.log(Log.INFO, "received event; sending it to Slices")

                # Pipeline  does not disassemble the payload of the event.
                # It knows nothing of the contents.
                # It simply places the payload on the clipboard with key of the eventTopic
                self.populateClipboard(inputParamPropertyPtrType, iStage, thisTopic)
                eventTransmitter.publish("sliceevent1", inputParamPropertyPtrType)

                log.log(Log.DEBUG, "event sent")

    def populateClipboard(self, inputParamPropertyPtrType, iStage, eventTopic):
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
        clipboard.put(eventTopic, inputParamPropertyPtrType)

        # LogRec(log, Log.DEBUG) << 'Added DataPropertyPtrType to clipboard ' \
        #             << inputParamPropertyPtrType                            \
        #            << LogRec.endr

        queue.addDataset(clipboard)

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


