#! /usr/bin/env python

import lsst.daf.base as dafBase
import lsst.pex.policy as pexPolicy
from Stage import Stage
from Queue import Queue
from Clipboard import Clipboard

class SimpleStageTester:
    """
    A class for running simple tests of a Stage.  Currently, this will run
    only one Slice at a time, either a master slice (running the serial
    preprocess() and postprocess() functions) or a worker slice (running
    the process() function).  The user is responsible for creating the
    data that goes into the input clipboard.  
    """

    def __init__(self, stage, runID="simpleTest", mpiUniverseSize=1):
        """create the test
        @param stage    a stage instance; the policy should have been passed
                          to the stage's constructor.
        @param runID    the run identifier to provide to the stage
        @param mpiUniverseSize  the number of MPI processes to pretend are
                          running
        """
        self.stage = stage
        self.event = None
        self.inQ = Queue()
        self.outQ = Queue()
        stage.initialize(self.outQ, self.inQ);
        stage.setRun(runID)
        stage.setUniverseSize(mpiUniverseSize)
        stage.setLookup(dict(work=".", input=".", output=".",
                             update=".", scratch="."))
        

    def run(self, clipboard, sliceID):
        """run the stage as a particular slice, returning the output clipboard
        @param clipboard     a Clipboard or plain dictionary instance
                                containing the Stage input data
        @param sliceID       the number to give as the slice identifier.  A
                                negative id number indicates the master slice.
        """
        if (sliceID < 0):
            return self.runMaster(clipboard, sliceID=sliceID)
        else:
            return self.runWorker(clipboard, sliceID)

    def runMaster(self, clipboard, dopre=True, dopost=True, sliceID=-1):
        """run the Stage as a master, running the serial functions,
        preprocess() and postprocess()
        """
        if isinstance(clipboard, dict):
            clipboard = self.toClipboard(clipboard)
        if not isinstance(clipboard, Clipboard):
            raise TypeError("runWorker input is not a clipboard")

        if self.event is not None:
            clipboard(self.event[0], self.event[1])

        self.stage.setRank(sliceID)

        self.inQ.addDataset(clipboard)
        self.stage.preprocess()
        
        self.stage.postprocess()
        return self.outQ.getNextDataset()

    def runWorker(self, clipboard, sliceID=0):
        """run the Stage as a worker, running its process() function
        @param clipboard     a Clipboard or plain dictionary instance
                                containing the Stage input data
        @param sliceID       the number to give as the slice identifier;
                                must be >= 0.
        """
        if isinstance(clipboard, dict):
            clipboard = self.toClipboard(clipboard)
        if not isinstance(clipboard, Clipboard):
            raise TypeError("runWorker input is not a clipboard")

        if self.event is not None:
            clipboard(self.event[0], self.event[1])

        self.stage.setRank(sliceID)

        self.inQ.addDataset(clipboard)
        self.stage.process()
        return self.outQ.getNextDataset()

    def setEvent(self, name, eventData):
        """set data for an event to be received by the Stage prior to
        being called.  This implementation currently does not support
        array-valued data.
        @param name        the topic name for the event
        @param eventData   the event data, either as a python dictionary or
                              a PropertySet
        """
        if isinstance(eventData, dict):
            evprop = dafBase.PropertySet()
            for key in eventData.keys():
                evprop.set(key, eventData[key])
            eventData = evprop

        self.event = (name, eventData)

    def toClipboard(self, data):
        cb = Clipboard()
        for key in data.keys():
            cb.put(key, data[key])
        return cb

def create(stage, policy, runID="simpleTest", mpiUniverseSize=1):
    """create a SimpleStageTester from a stage and a policy
    @param stage    a stage, either as a string naming the fully-qualified 
                       class to be instantiated or a fully-qualified class
                       type.
    @param policy   the policy to configure the stage with.  This is either
                       an actual policy instance, or a string giving the
                       path to a policy file.
    """
    if isinstance(stage, str):
        stage = stage.strip()
        tokens = stage.split('.')
        classString = tokens.pop().strip()
        pkg = ".".join(tokens)
        mod = __import__(pkg, globals(), locals(), [classString], -1)
        stage = getattr(mod, classString)
    if not issubclass(stage, Stage):
        raise TypeError("Not a Stage subclass: " + str(stage))

    if isinstance(policy, str) or isinstance(policy, pexPolicy.PolicyFile):
        policy = pexPolicy.Policy.createPolicy(policy)
    if not isinstance(policy, pexPolicy.Policy):
        raise TypeError("Not a Policy instance: " + policy)

    stageInst = stage(0, policy)
    return SimpleStageTester(stageInst, runID, mpiUniverseSize)

def test(stage, runID="simpleTest", mpiUniverseSize=1):
    return SimpleStageTester(stage, runID, mpiUniverseSize)
