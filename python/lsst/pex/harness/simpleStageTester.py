#! /usr/bin/env python

import lsst.daf.base as dafBase
import lsst.pex.policy as pexPolicy
from Queue import Queue
from Clipboard import Clipboard
from lsst.pex.logging import Log, Debug

import lsst.pex.harness.stage as hstage
from lsst.pex.harness.stage import StageProcessing
from lsst.pex.harness.stage import NoOpSerialProcessing, NoOpParallelProcessing

class SimpleStageTester:
    """
    A class for running simple tests of a Stage.  Currently, this will run
    only one Slice at a time, either a master slice (running the serial
    preprocess() and postprocess() functions) or a worker slice (running
    the process() function).  The user is responsible for creating the
    data that goes into the input clipboard.  
    """

    def __init__(self, stage=None, name="1", runID="simpleTest",
                 universeSize=1):
                 
        """create the tester
        @param stage            a Stage instance for the first stage in a
                                  simple pipeline.
        @param name             the name to associate with this stage (for
                                  display purposes.
        @param runID            the run identifier to provide to the stage
        @param universeSize     the number of parallel threads to pretend are
                                  running
        """
        self.stage = stage
        self.log = Debug("SimpleStageTester")
        self.event = None
        self.inQ = Queue()
        self.outQ = Queue()

        self.sysdata = {}
        self.sysdata["runId"] = runID
        self.sysdata["stageId"] = -1
        self.sysdata["universeSize"] = universeSize

        self.stages = []
        if stage:
            self.stages.append( (name, stage) )

    def setEventBroker(self, host, port=None):
        """
        set the event broker to use to get and receiver events.  Pass in
        None for the host to disable receiving events.
        @param host   the host where the event broker is running
        @param port   the port that the event broker is listening to
        """
        self.brokerhost = host
        if port:
            self.brokerport = port
        for stage in self.stages:
            stage.setEventBroker(self.brokerhost)

    def getEventBroker(self):
        return (self.brokerhost, self.brokerport)

    def addStage(self, stage, name=None, event=None):
        """
        add a stage to this simple pipeline.
        @param stage    the Stage instance to add in order
        @param name     a name for the stage.  If None, create one from the
                           stage object
        @param event    an event which must be received prior to execution
                           NOT YET IMPLEMENTED (ignored).
        """
        if name is None:
            name = str(len(self.stages))
        stage.setEventBroker(self.brokerhost)
        self.stages.append( (name, stage) )

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

    def _makeStages(self, sliceID, isSerial):
        sysdata = self.sysdata.copy()

        stages = []
        inQ = self.inQ
        outQ = None
        for name, stage in self.stages:
            sysdata['stageId'] = len(stages) + 1
            sysdata['name'] = name

            if (isSerial): 
                newstage = stage.createSerialProcessing(self.log, sysdata)
            else:
                newstage = stage.createParallelProcessing(sliceID, self.log,
                                                          sysdata)
            
            stages.append( newstage )
            if len(stages) == len(self.stages):
                outQ = self.outQ
            else:
                outQ = Queue()
            stages[-1].initialize(outQ, inQ)
            inQ = outQ

        return stages


    def runMaster(self, clipboard, sliceID=0):
        """run the Stage as a master, calling its initialize() function,
        and then running the serial functions, preprocess() and postprocess()
        """
        if isinstance(clipboard, dict):
            clipboard = self.toClipboard(clipboard)
        if not isinstance(clipboard, Clipboard):
            raise TypeError("runMaster input is not a clipboard")

        if self.event is not None:
            clipboard(self.event[0], self.event[1])

        stages = self._makeStages(sliceID, True)

        self.inQ.addDataset(clipboard)
        for stage in stages:
            self.log.debug(5, "Calling Stage preprocess() on " +
                               stage.getName())
            interQ = stage.applyPreprocess()
            self.log.debug(5, "Stage preprocess() complete")

            self.log.debug(5, "Calling Stage postprocess()")
            stage.applyPostprocess(interQ)
            self.log.debug(5, "Stage postprocess() complete")

        return self.outQ.getNextDataset()

    def runWorker(self, clipboard, sliceID=1):
        """run the Stage as a worker, running its process() function
        @param clipboard     a Clipboard or plain dictionary instance
                                containing the Stage input data
        @param sliceID       the number to give as the slice identifier;
                                must be >= 0.
        """
        if isinstance(clipboard, dict):
            clipboard = self.toClipboard(clipboard)
        if not isinstance(clipboard, Clipboard):
            raise TypeError("runMaster input is not a clipboard")

        if self.event is not None:
            clipboard(self.event[0], self.event[1])

        stages = self._makeStages(sliceID, False)

        self.inQ.addDataset(clipboard)
        for stage in stages:
            self.log.debug(5, "Calling Stage process()")
            stage.applyProcess()
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

def create(policy, stageSerial=None, stageParallel=None, runID="simpleTest",
           universeSize=1):
    """create a SimpleStageTester from a stage and a policy
    @param stageSerial    the serial component of the stage, either as a 
                            string naming the fully-qualified SerialProcessing 
                            class to be instantiated or a SerialProcessing 
                            class type.
    @param stageParallel  the parallel component of the stage, either as a 
                           string naming the fully-qualified ParallelProcessing
                           class to be instantiated or a ParallelProcessing 
                           class type.
    @param policy         the policy to configure the stage with.  This is 
                            either an actual policy instance, or a string 
                            giving the path to a policy file.
    @param runID          the run identifier to provide to the stage
    @param universeSize   the number of parallel threads to pretend are
                            running
    """
    if stageSerial:
        if isinstance(stageSerial, str):
            stageSerial = hstage._createClass(stageSerial.strip())
        if not issubclass(stageSerial, SerialProcessing):
            raise TypeError("Not a SerialProcessing subclass: " +
                            str(stageSerial))

    if stageParallel:
        if isinstance(stageParallel, str):
            stageSerial = hstage._createClass(stageParallel.strip())
        if not issubclass(stageParallel, ParallelProcessing):
            raise TypeError("Not a ParallelProcessing subclass: " +
                            str(stageParallel))

    if isinstance(policy, str) or isinstance(policy, pexPolicy.PolicyFile):
        policy = pexPolicy.Policy.createPolicy(policy)
    if policy is not None and not isinstance(policy, pexPolicy.Policy):
        raise TypeError("Not a Policy instance: " + policy)

    stage = hstage.Stage(policy)
    stage.serialClass = stageSerial
    stage.parallelClass = stageParallel

    return SimpleStageTester(stage, runID, universeSize)

def test(stage, runID="simpleTest", universeSize=1):
    return SimpleStageTester(stage, runID, universeSize)

