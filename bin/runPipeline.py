#! /usr/bin/env python

from lsst.pex.harness.Queue import Queue
from lsst.pex.harness.Stage import Stage
from lsst.pex.harness.Clipboard import Clipboard
from lsst.pex.harness.Pipeline import Pipeline

import lsst.pex.policy as policy

import lsst.daf.base as datap
from lsst.daf.base import DataProperty

import lsst.ctrl.events as events

import os
import sys

from cStringIO import StringIO

if (__name__ == '__main__'):
    """
     runPipeline : Pipeline Main method 
    """

    if(len(sys.argv) != 3):
        print "Usage: runPipeline.py <policy-file-name> <runId> "
        sys.exit(0)

    pipelinePolicyName = sys.argv[1]
    runId = sys.argv[2]

    pyPipeline = Pipeline(runId, pipelinePolicyName)

    pyPipeline.configurePipeline()   

    pyPipeline.initializeQueues()  

    pyPipeline.initializeStages()    

    pyPipeline.startSlices()  

    # pyPipeline.startInitQueue()    # place an empty clipboard in the first Queue 

    pyPipeline.startStagesLoop()

    pyPipeline.shutdown()


