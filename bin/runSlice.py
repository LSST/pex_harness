#! /usr/bin/env python

from lsst.pex.harness.Queue import Queue
from lsst.pex.harness.Stage import Stage
from lsst.pex.harness.Clipboard import Clipboard
from lsst.pex.harness.Slice import Slice

import lsst.pex.policy as policy

import lsst.daf.base as dafBase

import lsst.ctrl.events as events

import os
import sys

if (__name__ == '__main__'):
    """
    runSlice: Slice Main execution 
    """

    if(len(sys.argv) != 3):
        print "Usage: runSlice.py <policy-file-name> <runId> "
        sys.exit(0)

    pipelinePolicyName = sys.argv[1]
    runId = sys.argv[2]

    pySlice = Slice(runId, pipelinePolicyName)

    pySlice.configureSlice()   

    pySlice.initializeQueues()     

    pySlice.initializeStages()   

    # pySlice.startInitQueue()

    pySlice.startStagesLoop()

    pySlice.shutdown()

