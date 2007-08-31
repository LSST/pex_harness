#! /usr/bin/env python

from Queue import Queue
from Stage import Stage
from Clipboard import Clipboard

# from lsst.mwi.data import DataProperty
import lsst.mwi.data as datap

import lsst.events as events

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

    def __init__(self):
        """
        Initialize the Pipeline: create empty Queue and Stage lists;
        import the C++ Pipeline instance; initialize the MPI environment
        """
        self.queueList = []
        self.stageList = []
        self.stageClassList = []
        import pipeline
        self.cppPipeline = pipeline.Pipeline()
        self.cppPipeline.initialize()
        self.LOGFILE = open("PipelinePython.log","w")
        self.LOGFILE.write("Python Pipeline __init__ : Opened log \n");
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
        self.eventTopic = "pipedata"
        self.sliceTopic = "slicedata"

        filePolicy = open('pipeline.policy', 'r')
        fullStageList = filePolicy.readlines()
        self.nStages = len(fullStageList)

        for line in fullStageList:
            fullStage = line.strip()
            tokenList = line.split('.')
            classString = tokenList.pop()
            classString = classString.strip()

            package = '' 
            for item in tokenList:
                   package += item
                   package += '.'
            package = package.rstrip('.')

            # For example  package -> lsst.dps.App1Stage  classString -> App1Stage
            AppStage = __import__(package, globals(), locals(), [classString], -1)
            StageClass = getattr(AppStage, classString)
            self.stageClassList.append(StageClass) 

        self.LOGFILE.write("Python Pipeline configurePipeline : Done \n");
        self.LOGFILE.flush()

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
            StageClass = self.stageClassList[iStage-1]
            stageObject = StageClass(iStage)
            inputQueue  = self.queueList[iStage-1]
            outputQueue = self.queueList[iStage]
            stageObject.initialize(outputQueue, inputQueue)
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
        for iStage in range(1, self.nStages+1):

            stage = self.stageList[iStage-1]

            self.handleEvents(iStage)

            stage.preprocess()

            self.cppPipeline.invokeProcess(iStage)

            stage.postprocess()
           

        else:
            print 'Python Pipeline startStagesLoop : Stage loop is over'


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

        if (iStage == 1):
            eventReceiver    = events.EventReceiver(self.eventHost, self.eventTopic)
            eventTransmitter = events.EventTransmitter(self.eventHost, self.sliceTopic)

            print 'Python Pipeline handleEvents - waiting on receive...\n'
            self.LOGFILE.write("Python Pipeline handleEvents - waiting on receive...\n");
            self.LOGFILE.flush()
            inputParamPropertyPtrType = eventReceiver.receive(80000)
            self.LOGFILE.write("Python Pipeline handleEvents -  received event...\n");
            self.LOGFILE.flush()
            print 'Python Pipeline handleEvents - received event.\n'
            self.LOGFILE.write("Python Pipeline handleEvents -  Sending event to Slices\n");
            self.LOGFILE.flush()
            print 'Python Pipeline handleEvents - Sending event to Slices.\n'

            self.populateClipboard(inputParamPropertyPtrType)

            eventTransmitter.send("sliceevent1", inputParamPropertyPtrType)

            print 'Python Pipeline handleEvents : Sent event to Slices '
            self.LOGFILE.write("Python Pipeline handleEvents -  Sent event to Slices\n");
            self.LOGFILE.flush()

    def populateClipboard(self, inputParamPropertyPtrType):
        """
        Place the event payload onto the Clipboard 
        """
        print 'Python Pipeline populateClipboard'

        queue1 = self.queueList[0]
        clipboard = queue1.getNextDataset()

        key1 = "inputParam"

        clipboard.put(key1, inputParamPropertyPtrType)

        print 'Python Pipeline populateClipboard : Added DataPropertyPtrType to clipboard '
        dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.") 

        for key in dataPropertyKeyList:
            dpPtr = inputParamPropertyPtrType.findUnique(key)
            if (key == "float"):
                print 'Python Pipeline populateClipboard()', key, dpPtr.getValueFloat()
            else:
                print 'Python Pipeline populateClipboard()', key, dpPtr.getValueString()

        queue1.addDataset(clipboard)

# print __doc__

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


