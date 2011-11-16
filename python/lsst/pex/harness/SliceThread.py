# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#


import thread
import threading
import os, sys, re, traceback
from threading import Thread
from threading import Event as PyEvent
from lsst.pex.harness.Slice import Slice

class SliceThread(threading.Thread):

    def __init__ (self, rank, name, pipelinePolicyName, runId, logthresh, usize, eventa, eventb, logdir, workerid):
        Thread.__init__(self)
        self.rank = rank
        self.name = name
        self.pipelinePolicyName = pipelinePolicyName
        self._runId = runId
        self.logthresh = logthresh
        self.eventa = eventa
        self.eventb = eventb
        self.universeSize = usize
        self.logdir = logdir
        self.workerId = workerid
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

        self.pySlice = Slice(self._runId, self.pipelinePolicyName, self.name, self.rank, self.workerId)
        if isinstance(self.logthresh, int):
            self.pySlice.setLogThreshold(self.logthresh)

        self.pySlice.setLoopEventA(self.eventa)
        self.pySlice.setLoopEventB(self.eventb)
        self.pySlice.setUniverseSize(self.universeSize)
        self.pySlice.setLogDir(self.logdir)

        self.pySlice.initializeLogger()

        self.pySlice.configureSlice()

        self.pySlice.initializeQueues()

        self.pySlice.initializeStages()

        try:
            self.pySlice.startStagesLoop()
        except SystemExit:
            del self.pySlice
            raise

        self.pySlice.shutdown()


SliceThread.lifeline = re.compile(r"(\d) received")
