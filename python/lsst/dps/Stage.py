#! /usr/bin/env python

"""
Stage provides a super class for a particular <ApplicationStage> to inherit.
In doing so, the <ApplicationStage> will overwrite the Stage API 
consisting of 
        preprocess()
        process()
        postprocess()
Serial processing will occur within preprocess() and postprocess(),
and these Stage methods will be invoked from the main Pipeline.
The process() method of the Stage will be invoked from the parallel
Slice worker.
"""

class Stage:
    '''Allows access to Python Stages that implement Stage interface'''

    #------------------------------------------------------------------------
    def __init__(self):
        """
        Initialize the Stage; set stageId to -1 as default value
        """
        self.stageId = -1
        self._rank = -1

    #------------------------------------------------------------------------
    def __init__(self, stageId):
        """
        Initialize the Stage, setting the stageId to the value sent
        """
        self.stageId = stageId
        self._rank = -1

    #------------------------------------------------------------------------
    def __del__(self):
        """
        Delete the Stage object, cleaning up
        """
	print 'Python Stage %d being deleted' % self.stageId

    #------------------------------------------------------------------------
    def initialize(self, outQueue,  inQueue): 
        """
        Initialize the references to the Queues for this Stage
        """
        self.outputQueue = outQueue
        self.inputQueue  = inQueue

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        This method will be overwritten by the Stage subclass
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            value = self.activeClipboard.get(key)
        

    #------------------------------------------------------------------------
    def process(self): 
        """
        More
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            value = self.activeClipboard.get(key)

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        This method will be overwritten by the Stage subclass
        """
        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def setStageId(self, stageId): 
        """
        Setter method for the integer field stageId
        """
        self.stageId = stageId

    #------------------------------------------------------------------------
    def getStageId(self): 
        """
        get method for the integer field stageId
        """
        return self.stageId

    #------------------------------------------------------------------------
    def getRank(self): 
        """
        get method for the integer MPI rank
        """
        return self._rank

    #------------------------------------------------------------------------
    def setRank(self, rank): 
        """
        set method for the integer field rank
        """
        self._rank = rank

    #------------------------------------------------------------------------
    def getUniverseSize(self): 
        """
        get method for the integer MPI universe size
        """
        return self._universeSize

    #------------------------------------------------------------------------
    def setUniverseSize(self, universeSize): 
        """
        set method for the integer field universe size
        """
        self._universeSize = universeSize

