#! /usr/bin/env python

from Queue import Queue
from Stage import Stage
from Clipboard import Clipboard

import lsst.mwi.data as datap

import lsst.events as events

import os


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
    def __init__(self):
        """
        Initialize the Slice: create an empty Queue List and Stage List;
        Import the C++ Slice  and initialize the MPI environment
        """
        self.queueList = []
        self.stageList = []
        self.stageClassList = []
        import slice
        self.cppSlice = slice.Slice()
        self.cppSlice.initialize()
        self._rank = self.cppSlice.getRank()

    def __del__(self):
        """
        Delete the Slice object: cleanup 
        """
        print 'Python Slice being deleted'

    def configureSlice(self):
        """
        Configure the slice via reading a Policy file 
        """
        self.eventHost = "lsst8.ncsa.uiuc.edu"
        self.eventTopic = "slicedata"

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
                package += '/'
            package = package.rstrip('/')

            # For example  package -> lsst.dps.App1Stage  classString -> App1Stage
            AppStage = __import__(package, globals(), locals(), [classString], -1)
            StageClass = getattr(AppStage, classString)
            self.stageClassList.append(StageClass)


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
            StageClass = self.stageClassList[iStage-1]
            stageObject = StageClass(iStage)
            inputQueue  = self.queueList[iStage-1]
            outputQueue = self.queueList[iStage]
            stageObject.initialize(outputQueue, inputQueue)
            stageObject.setRank(self._rank)
            self.stageList.append(stageObject)

    def startInitQueue(self):
        """
        Place an empty Clipboard in the first Queue
        """
        clipboard = Clipboard()
        queue1 = self.queueList[0]
        queue1.addDataset(clipboard)

    def startStagesLoop(self): 
        """
        Execute the Stage loop. The loop progressing in step with 
        the analogous stage loop in the central Pipeline by means of
        MPI Bcast and Barrier calls.
        """
        for iStage in range(1, self.nStages+1):

            self.handleEvents(iStage)

            stageObject = self.stageList[iStage-1]
            self.cppSlice.invokeBcast(iStage)
            stageObject.process()
            self.cppSlice.invokeBarrier(iStage)

        else:
            print 'Python Slice The for loop is over'


    def shutdown(self): 
        """
        Shutdown the Slice execution
        """
        self.cppSlice.shutdown()

    def handleEvents(self, iStage):
        """
        Handles Events: transmit or receive events as specified by Policy
        """
        print 'Python Slice handleEvents : rank %i iStage %i' % (self._rank, iStage)
        
        if (iStage == 1):
            x = events.EventReceiver(self.eventHost, self.eventTopic)

            print 'Python Slice handleEvents rank : ',self._rank, ' - waiting on receive...\n'
            inputParamPropertyPtrType = x.receive(80000)
            print 'Python Slice handleEvents rank : ',self._rank,' - received event.\n'

            self.populateClipboard(inputParamPropertyPtrType)

            print 'Python Slice handleEvents rank : ',self._rank,' Added DataPropertyPtrType to clipboard '

    def populateClipboard(self, inputParamPropertyPtrType):
        """
        Place the event payload onto the Clipboard
        """
        print 'Python Slice populateClipboard'

        queue1 = self.queueList[0]
        clipboard = queue1.getNextDataset()

        key1 = "inputParam"

        clipboard.put(key1, inputParamPropertyPtrType)

        print 'Python Slice populateClipboard rank : ',self._rank,' Added DataPropertyPtrType to clipboard '
        dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")

        for key in dataPropertyKeyList:
            dpPtr = inputParamPropertyPtrType.findUnique(key)
            if (key == "float"):
                print 'Python Slice populateClipboard() rank ', self._rank, key, dpPtr.getValueFloat()
            else:
                print 'Python Slice populateClipboard() rank ', self._rank, key, dpPtr.getValueString()

        queue1.addDataset(clipboard)


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

