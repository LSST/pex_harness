
import thread
import threading
import os, sys, re, traceback
from threading import Thread
from threading import Event as PyEvent
from lsst.pex.harness.Slice import Slice

class SliceThread(threading.Thread):

    def __init__ (self, rank, name, pipelinePolicyName, runId, logthresh, usize, eventa, eventb):
        Thread.__init__(self)
        self.rank = rank
        self.name = name
        self.pipelinePolicyName = pipelinePolicyName
        self._runId = runId
        self.logthresh = logthresh
        self.eventa = eventa
        self.eventb = eventb
        self.universeSize = usize
        self.pid = os.getpid()

    def getPid (self):
        return self.pid 

    def stop (self):
        self.pySlice.stop()

    def exit (self):
        # thread.exit()
        print "RAISE SYSTEM EXIT" 
        # raise SystemExit()
        self.raise_exc(SystemExit)

    def run(self):

        if self.name is None or self.name == "None":
            self.name = os.path.splitext(os.path.basename(self.pipelinePolicyName))[0]

        self.pySlice = Slice(self._runId, self.pipelinePolicyName, self.name, self.rank)
        if isinstance(self.logthresh, int):
            self.pySlice.setLogThreshold(self.logthresh)

        self.pySlice.setLoopEventA(self.eventa)
        self.pySlice.setLoopEventB(self.eventb)
        self.pySlice.setUniverseSize(self.universeSize)

        self.pySlice.initializeLogger()

        self.pySlice.configureSlice()

        self.pySlice.initializeQueues()

        self.pySlice.initializeStages()

        self.pySlice.startStagesLoop()

        self.pySlice.shutdown()


SliceThread.lifeline = re.compile(r"(\d) received")
