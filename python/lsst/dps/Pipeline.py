#! /usr/bin/env python

from lsst.dps.Queue import Queue
from lsst.dps.Stage import Stage
from lsst.dps.Clipboard import Clipboard

import lsst.mwi.policy as policy

import lsst.mwi.data as datap
from lsst.mwi.data import DataProperty

import lsst.events as events

import os

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

    def __init__(self, runId=-1, pipelinePolicyName=None):
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
        self.executionMode = 0
        import pipeline
        self.cppPipeline = pipeline.Pipeline()
        self.cppPipeline.setRunId(runId)
        self.cppPipeline.setPolicyName(pipelinePolicyName)
        self.cppPipeline.initialize()
        self.universeSize = self.cppPipeline.getUniverseSize()
        self._runId = runId
        self.pipelinePolicyName = pipelinePolicyName
        self.LOGFILE = open("PipelinePython.log","w")
        self.LOGFILE.write("Python Pipeline __init__ : Opened log \n")
        self.LOGFILE.write("Python Pipeline __init__ : universeSize is ")
        self.LOGFILE.write(str(self.universeSize))
        self.LOGFILE.write("\n")
        self.LOGFILE.flush()

    def __del__(self):
        """
        Delete the Pipeline object: clean up
        """
        print 'Python Pipeline being deleted'

    def configurePipeline(self):
        """
        Configure the Pipeline by reading a Policy File
        """
        self.LOGFILE.write("Python Pipeline configurePipeline \n");
        self.eventHost = "lsst8.ncsa.uiuc.edu"
        # self.sliceTopic = "slicedata"

        # path1 = os.environ['LSST_POLICY_DIR']
        # print 'Python Pipeline path1', path1
      
        # 
        if(self.pipelinePolicyName == None):
            self.pipelinePolicyName = "pipeline_policy.paf"
        dictName = "pipeline_dict.paf"
        p = policy.Policy.createPolicy(self.pipelinePolicyName)

        # Process Application Stages
        fullStageList = p.getArray("appStages")

        self.LOGFILE.write("appStages")
        self.LOGFILE.write("\n")
        for item in fullStageList:
            self.LOGFILE.write(item)
            self.LOGFILE.write("\n")
        self.LOGFILE.write("end appStages")
        self.LOGFILE.write("\n")

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

        self.LOGFILE.write("Python Pipeline configurePipeline : Done \n");
        self.LOGFILE.flush()

        # Process Event Topics
        self.eventTopicList = p.getArray("eventTopics")

        self.LOGFILE.write("eventTopics")
        self.LOGFILE.write("\n")
        for item in self.eventTopicList:
            self.LOGFILE.write(item)
            self.LOGFILE.write("\n")
        self.LOGFILE.write("end eventTopics")
        self.LOGFILE.write("\n")

        self.LOGFILE.write("_runId is ")
        self.LOGFILE.write(self._runId)
        self.LOGFILE.write("\n")

        # Make a List of corresponding eventReceivers for the eventTopics
        # eventReceiverList    
        for topic in self.eventTopicList:
            eventReceiver = events.EventReceiver(self.eventHost, topic)
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
            self.stageList.append(stageObject)


    def startSlices(self):
        """
        Initialize the Queue by defining an initial dataset list
        """
        self.LOGFILE.write("Python Pipeline startSlices \n");
        self.LOGFILE.flush()
        self.cppPipeline.startSlices()
        self.LOGFILE.write("Python Pipeline startSlices : Done \n");
        self.LOGFILE.flush()

    def startInitQueue(self):
        """
        Place an empty Clipboard in the first Queue
        """
        clipboard = Clipboard()
        queue1 = self.queueList[0]
        queue1.addDataset(clipboard)

    def startStagesLoop(self): 
        """
        Method to execute loop over Stages
        """

        eventReceiver = events.EventReceiver(self.eventHost, self.shutdownTopic)

        count = 0 
        while True:

            val = eventReceiver.receive(100)
            if ((val.get() != None) or ((self.executionMode == 1) and (count == 1))):
                print "Pipeline Terminating "
                print "Pipeline Terminating the Slices "
                self.cppPipeline.invokeShutdown()
                break
            else:
                count += 1
                print 'Python Pipeline startStagesLoop : count ', count
                self.cppPipeline.invokeContinue()
                self.startInitQueue()    # place an empty clipboard in the first Queue

                for iStage in range(1, self.nStages+1):

                    stage = self.stageList[iStage-1]

                    self.handleEvents(iStage)

                    stage.preprocess()

                    self.cppPipeline.invokeProcess(iStage)

                    stage.postprocess()
           
                else:
                    print 'Python Pipeline startStagesLoop : Stage loop iteration is over'


        print 'Python Pipeline startStagesLoop : Full Pipeline Stage loop is over'
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
        print 'Python Pipeline handleEvents : iStage %d' % (iStage)

        thisTopic = self.eventTopicList[iStage-1]
        thisTopic = thisTopic.strip()
        self.LOGFILE.write("Python Pipeline handleEvents thisTopic ")
        self.LOGFILE.write(thisTopic)
        self.LOGFILE.write("\n")

        if (thisTopic != "None"):
            fileStr = StringIO()
            fileStr.write(thisTopic)
            fileStr.write("_slice")
            sliceTopic = fileStr.getvalue()
            #  Replace 
            # eventReceiver    = events.EventReceiver(self.eventHost, thisTopic)
            eventReceiver    = self.eventReceiverList[iStage-1]
            eventTransmitter = events.EventTransmitter(self.eventHost, sliceTopic)

            print 'Python Pipeline handleEvents - waiting on receive...\n'
            self.LOGFILE.write("Python Pipeline handleEvents - waiting on receive...\n")
            self.LOGFILE.flush()

            inputParamPropertyPtrType = eventReceiver.receive(800000)

            if (inputParamPropertyPtrType.get() != None): 
                self.LOGFILE.write("Python Pipeline handleEvents -  received event...\n")
                self.LOGFILE.flush()
                print 'Python Pipeline handleEvents - received event.\n'
                self.LOGFILE.write("Python Pipeline handleEvents -  Sending event to Slices\n")
                self.LOGFILE.flush()
                print 'Python Pipeline handleEvents - Sending event to Slices.\n'

                # Pipeline  does not disassemble the payload of the event.
                # It knows nothing of the contents.
                # It simply places the payload on the clipboard with key of the eventTopic
                self.populateClipboard(inputParamPropertyPtrType, iStage, thisTopic)
                eventTransmitter.publish("sliceevent1", inputParamPropertyPtrType)

                print 'Python Pipeline handleEvents : Sent event to Slices '
                self.LOGFILE.write("Python Pipeline handleEvents -  Sent event to Slices\n")
                self.LOGFILE.flush()

    def populateClipboard(self, inputParamPropertyPtrType, iStage, eventTopic):
        """
        Place the event payload onto the Clipboard 
        """
        print 'Python Pipeline populateClipboard'

        queue = self.queueList[iStage-1]
        clipboard = queue.getNextDataset()

        # Pipeline does not disassemble the payload of the event.
        # It knows nothing of the contents.
        # It simply places the payload on the clipboard with key of the eventTopic
        clipboard.put(eventTopic, inputParamPropertyPtrType)
        print 'Python Pipeline populateClipboard : Added DataPropertyPtrType to clipboard '
        # print 'Python Pipeline populateClipboard()', inputParamPropertyPtrType.toString('=',1)
        queue.addDataset(clipboard)

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


