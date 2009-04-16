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

    def __init__(self, stageId=-1, stagePolicy=None):
        """
        Initialize the Stage, setting the stageId to the value sent, setting policy to that sent
        """
        self.stageId = stageId
        self._rank = -1
        self._policy = stagePolicy
        self._runId = "TEST"
        self._lookup = {}
        self._evbroker = None


    def __del__(self):
        """
        Delete the Stage object, cleaning up
        """
	# print 'Python Stage %d being deleted' % self.stageId
        pass

    def initialize(self, outQueue, inQueue): 
        """
        Initialize the references to the Queues for this Stage
        """
        self.outputQueue = outQueue
        self.inputQueue  = inQueue

    def preprocess(self): 
        """
        Execute the preprocessing code for this Stage.
        This method may be overwritten by a Stage subclass.
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            value = self.activeClipboard.get(key)

    def process(self): 
        """
        Execute the processing code for this Stage.
        This method is usually be overwritten by a Stage subclass, and 
        executed by a Slice worker.
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            value = self.activeClipboard.get(key)

        self.outputQueue.addDataset(self.activeClipboard)

    def postprocess(self): 
        """
        Execute the postprocessing code for this Stage.
        This method may be overwritten by a Stage subclass.
        """
        self.outputQueue.addDataset(self.activeClipboard)

    def setStageId(self, stageId): 
        """
        Setter method for the integer field stageId
        """
        self.stageId = stageId

    def getStageId(self): 
        """
        get method for the integer field stageId
        """
        return self.stageId

    def getRank(self): 
        """
        get method for the integer MPI rank
        """
        return self._rank

    def setRank(self, rank): 
        """
        set method for the integer field rank
        """
        self._rank = rank

    def getUniverseSize(self): 
        """
        get method for the integer MPI universe size
        """
        return self._universeSize

    def setUniverseSize(self, universeSize): 
        """
        set method for the integer field universe size
        """
        self._universeSize = universeSize

    def getRun(self):
        """
        get method for the runId
        """
        return self._runId

    def setRun(self, run):
        """
        set method for the runId
        """
        self._runId = run

    def getEventBrokerHost(self):
        """get the hostname where the event broker currently in use is
        located"""
        return self._evbroker

    def setEventBrokerHost(self, host):
        """set the hostname where the event broker currently in use is
        located.  This is usually only be set by the harness framework.
        """
        self._evbroker = host

    def getLookup(self):
        """
        get method for the lookup dictionary
        """
        return self._lookup

    def setLookup(self, lookup):
        """
        set method for the lookup dictionary
        """
        self._lookup = lookup.copy()
        # self._lookup = lookup.deepcopy()


