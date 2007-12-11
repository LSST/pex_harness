#! /usr/bin/env python

from lsst.dps.Queue import Queue
from lsst.dps.Stage import Stage
from lsst.dps.Clipboard import Clipboard
from lsst.dps.Slice import Slice

import lsst.mwi.policy as policy

import lsst.mwi.data as datap

import lsst.events as events

import os
import sys

if (__name__ == '__main__'):
    """
    runSlice: Slice Main execution 
    """

    if(len(sys.argv) != 2):
        print "Usage: runSlice.py <runId> "
        sys.exit(0)

    runId = sys.argv[1]

    pySlice = Slice(runId)

    pySlice.configureSlice()   

    pySlice.initializeQueues()     

    pySlice.initializeStages()   

    # pySlice.startInitQueue()

    pySlice.startStagesLoop()

    pySlice.shutdown()

