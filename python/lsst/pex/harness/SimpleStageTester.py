#! /usr/bin/env python

import lsst.daf.base as dafBase
import lsst.pex.policy as pexPolicy
from Stage import Stage
from Queue import Queue
from Clipboard import Clipboard
from lsst.pex.logging import Log, Debug

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
        self.log = Debug("SimpleStageTester")
        self.stage = stage
        self.event = None
        self.inQ = Queue()
        self.outQ = Queue()
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
        """run the Stage as a master, calling its initialize() function,
        and then running the serial functions, preprocess() and postprocess()
        """
        if isinstance(clipboard, dict):
            clipboard = self.toClipboard(clipboard)
        if not isinstance(clipboard, Clipboard):
            raise TypeError("runWorker input is not a clipboard")

        if self.event is not None:
            clipboard(self.event[0], self.event[1])

        self.stage.setRank(sliceID)
        self.stage.initialize(self.outQ, self.inQ);

        self.inQ.addDataset(clipboard)

        self.log.debug(5, "Calling Stage preprocess()")
        self.stage.preprocess()
        self.log.debug(5, "Stage preprocess() complete")

        self.log.debug(5, "Calling Stage postprocess()")
        self.stage.postprocess()
        self.log.debug(5, "Stage postprocess() complete")

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
        self.stage.initialize(self.outQ, self.inQ);
        self.inQ.addDataset(clipboard)

        self.log.debug(5, "Calling Stage process()")
        self.stage.process()
        self.log.debug(5, "Stage process() complete")

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

    def setDebugVerbosity(self, verbLimit):
        """set the verbosity of the default log.  This and setLogThreshold()
        are different APIs that affect the same underlying limit that controls
        how many messages get logged.
        @param verbLimit    debug messages with a verbosity level larger
                               than this will not be printed.  If positive
                               INFO, WARN, and FATAL messages will also
                               be printed.  
        """
        Log.getDefaultLog().setThreshold(-1*verbLimit)

    def setLogThreshold(self, threshold):
        """set the importance threshold for the default log.  This and
        setDebugVerbosity are different APIs that affect the same underlying
        limit that controls how many messages get logged.  Normally one
        uses one of the predefined values--Log.DEBUG, Log.INFO, Log.WARN, and
        Log.FATAL--as input.
        @param threshold    the minimum importance of the message that will
                               get printed.
        """
        Log.getDefaultLog().setThreshold(threshold)

    def showAllLogProperties(self, show):
        """control whether log properties are displayed to the screen.  These
        include, for example, the DATE (and time) of the message.
        @param show   if true, show all the properties when a log message is
                         printed.  If false, don't show them.
        """
        Log.getDefaultLog().setShowAll(show)

    def willShowAllLogProperties(self):
        """return whether log properties are displayed to the screen.  These
        include, for example, the DATE (and time) of the message.
        """
        return Log.getDefaultLog().willShowAll()

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
    if policy is not None and not isinstance(policy, pexPolicy.Policy):
        raise TypeError("Not a Policy instance: " + policy)

    stageInst = stage(0, policy)
    return SimpleStageTester(stageInst, runID, mpiUniverseSize)

def test(stage, runID="simpleTest", mpiUniverseSize=1):
    return SimpleStageTester(stage, runID, mpiUniverseSize)

