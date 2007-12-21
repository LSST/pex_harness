#! /usr/bin/env python

from lsst.dps.Queue import Queue
from lsst.dps.Stage import Stage
from lsst.dps.Clipboard import Clipboard
from lsst.mwi.logging import Log, LogRec

import lsst.mwi.policy as policy
import lsst.mwi.exceptions as ex

import lsst.mwi.data as datap
from lsst.mwi.data import *

from lsst.mwi.exceptions import *

import lsst.events as events

import os
import sys


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
        self.queueList = []
        self.stageList = []
        self.stageClassList = []
        self.stagePolicyList = []
        self.sliceEventTopicList = []
        self.eventTopicList = []
        self.shutdownTopic = "triggerShutdownEvent_slice"
        import slice
        self.cppSlice = slice.Slice()
        # self.cppSlice.setRunId(runId)
        self.cppSlice.initialize()
        self._runId = runId
        self.pipelinePolicyName = pipelinePolicyName
        self.universeSize = self.cppSlice.getUniverseSize()
        self._rank = self.cppSlice.getRank()


    def __del__(self):
        """
        Delete the Slice object: cleanup 
        """
        self.log.log(Log.DEBUG, 'Python Slice being deleted')

    def configureSlice(self):
        """
        Configure the slice via reading a Policy file 
        """

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
        events.EventLog.createDefaultLog(self._runId, self._rank);

        # set up the logger
        self.log = Log(Log.getDefaultLog(), "dps.slice")
        initlog = Log(self.log, "init")
        LogRec(initlog, Log.INFO) << "Initializing Slice " + str(self._runId) \
              << DataProperty("universeSize", self.universeSize)              \
              << DataProperty("runId", self._runId) << LogRec.endr

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

        for astage in fullStageList:
            fullStage = astage.strip()
            tokenList = astage.split('.')
            classString = tokenList.pop()
            classString = classString.strip()

            package = ".".join(tokenList)

            # For example  package -> lsst.dps.App1Stage  classString -> App1Stage
            AppStage = __import__(package, globals(), locals(), [classString], -1)
            StageClass = getattr(AppStage, classString)
            self.stageClassList.append(StageClass)

        # Process Event Topics
        self.eventTopicList = p.getArray("eventTopics")
        self.sliceEventTopicList = p.getArray("eventTopics")

        lr = LogRec(self.log, Log.INFO)
        lr << "Loading event topics"
        for item in self.eventTopicList:
            lr << DataProperty("eventTopic", item)
        lr << LogRec.endr

        count = 0
        for item in self.eventTopicList:
            newitem = item + "_slice"
            self.sliceEventTopicList[count] = newitem
            count += 1

        lr = LogRec(self.log, Log.INFO)
        lr << "Loaded event topics"
        for item in self.eventTopicList:
            lr << DataProperty("eventTopic", item)
        lr << LogRec.endr

        # Process Stage Policies
        self.stagePolicyList = p.getArray("stagePolicies")

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
            stageObject.setRank(self._rank)
            stageObject.setUniverseSize(self.universeSize)
            stageObject.setRun(self._runId)
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

        looplog = Log(self.log, "loop", Log.INFO);
        proclog = Log(self.log, "process", Log.INFO);

        count = 0
        while True:
            count += 1
            LogRec(looplog, Log.INFO) \
                       << "starting stage loop number " + str(count) \
                       << DataProperty("loopnum", count) << LogRec.endr

            self.cppSlice.invokeShutdownTest()

            self.startInitQueue()    # place an empty clipboard in the first Queue

            self.errorFlagged = 0
            for iStage in range(1, self.nStages+1):

                self.handleEvents(iStage)

                stageObject = self.stageList[iStage-1]
                looplog.log(Log.INFO, "Getting pre-process signal from Pipeline")
                self.cppSlice.invokeBcast(iStage)
                proclog.log(Log.INFO, "Starting process")

                # Important try - except construct around stage process() 
                try:
                    # If no error/exception has been flagged, run process()
                    # otherwise, simply pass along the Clipboard 
                    if (self.errorFlagged == 0):
                        stageObject.process()
                    else:
                        self.transferClipboard(iStage)
  
                    ### raise LsstRuntime("Gregs terrible Runtime error: ouch")
                except LsstExceptionStack,e:
                    # except LsstRuntime,e:
                    # Log / Report the Exception
                    excInfo = sys.exc_info()
                    proclog.log(Log.FATAL, "Rank "+ str(self._rank) + \
                                          " threw uncaught exception: " + \
                                          str(excInfo));
                    # Acquire the entire exception stack
                    stackPtr = e.getStack()
                    lastPtr = e.getLast()

                    stackString = stackPtr.toString("stack: ", 1)
                    proclog.log(Log.FATAL, "Rank "+ str(self._rank) + \
                                          " uncaught exception stack: " + \
                                          stackString);

                    # Get exception properties
                    properties = lastPtr.getChildren()

                    # print "lastptr  to STRING", lastPtr.toString('=', 1)
                    index = 0
                    for j in properties:
                        print " index ", index
                        if j.isNode():
                            proclog.log(Log.FATAL, "Rank "+ str(self._rank) + " Index " + \
                                str(index) +  " (branch): " + j.getName())
                        else:
                            proclog.log(Log.FATAL, "Rank "+ str(self._rank) + " Index " + \
                                str(index) +  "  :" + j.getName())
                        index=+ 1

                    # Flag that an exception occurred to guide the framework to skip processing
                    self.errorFlagged = 1
                    # Post the cliphoard that the Stage failed to transfer to the output queue
                    self.postOutputClipboard(iStage)
                except:
                    # Log / Report the Exception
                    excInfo = sys.exc_info()
                    proclog.log(Log.FATAL, "Rank "+ str(self._rank) + \
                                          " threw uncaught exception: " + \
                                          str(excInfo));
                    # Flag that an exception occurred to guide the framework to skip processing
                    self.errorFlagged = 1
                    # Post the cliphoard that the Stage failed to transfer to the output queue
                    self.postOutputClipboard(iStage)

                proclog.log(Log.INFO, "Ending process")
                looplog.log(Log.INFO, "Getting post-process signal from Pipeline")
                self.cppSlice.invokeBarrier(iStage)

            else:
                looplog.log(Log.INFO, "completed loop number " + str(count))

            self.log.log(Log.DEBUG, 'Retrieving finalClipboard for deletion')

            # If no error/exception was flagged, 
            # then clear the final Clipboard in the final Queue
            if self.errorFlagged == 0:
                finalQueue = self.queueList[self.nStages]
                finalClipboard = finalQueue.getNextDataset()
                self.log.log(Log.DEBUG, "deleting final clipboard")
                finalClipboard.__del__()

        self.log.log(Log.INFO, "Shutting down pipeline");

    def shutdown(self): 
        """
        Shutdown the Slice execution
        """
        self.cppSlice.shutdown()

    def handleEvents(self, iStage):
        """
        Handles Events: transmit or receive events as specified by Policy
        """
        log = Log(self.log, "handleEvents");
        log.log(Log.INFO, 'rank %i iStage %i' % (self._rank, iStage))
        
        thisTopic = self.eventTopicList[iStage-1]
        log.log(Log.DEBUG, "processing topic " + thisTopic)

        if (thisTopic != "None"):
            sliceTopic = self.sliceEventTopicList[iStage-1]
            x = events.EventReceiver(self.activemqBroker, sliceTopic)

            log.log(Log.INFO, 'waiting on receive...')
            inputParamPropertyPtrType = x.receive(self.eventTimeout)

            self.populateClipboard(inputParamPropertyPtrType, iStage, thisTopic)

            log.log(Log.INFO, 'Added DataPropertyPtrType to clipboard')
        else:
            log.log(Log.INFO, 'No event to handle')

    def populateClipboard(self, inputParamPropertyPtrType, iStage, eventTopic):
        """
        Place the event payload onto the Clipboard
        """
        log = Log(self.log, "populateClipboard");
        log.log(Log.DEBUG,'Python Pipeline populateClipboard');

        queue = self.queueList[iStage-1]
        clipboard = queue.getNextDataset()

        # Slice does not disassemble the payload of the event. 
        # It knows nothing of the contents. 
        # It simply places the payload on the clipboard with key of the eventTopic
        clipboard.put(eventTopic, inputParamPropertyPtrType)
        # LogRec(log, Log.DEBUG) << 'Added DataPropertyPtrType to clipboard ' \
        #             << inputParamPropertyPtrType                            \
        #            << LogRec.endr

        queue.addDataset(clipboard)

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
    Slice Main execution 
    """

    pySlice = Slice()

    pySlice.configureSlice()   

    pySlice.initializeQueues()     

    pySlice.initializeStages()   

    pySlice.startInitQueue()

    pySlice.startStagesLoop()

    pySlice.shutdown()

