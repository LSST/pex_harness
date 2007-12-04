#! /usr/bin/env python

from lsst.dps.Queue import Queue
from lsst.dps.Stage import Stage
from lsst.dps.Clipboard import Clipboard
from lsst.dps.Pipeline import Pipeline

import lsst.mwi.policy as policy

import lsst.mwi.data as datap
from lsst.mwi.data import DataProperty

import lsst.events as events

import os

from cStringIO import StringIO

if (__name__ == '__main__'):
    """
     runPipeline : Pipeline Main method 
    """

    pyPipeline = Pipeline()

    pyPipeline.configurePipeline()   

    pyPipeline.initializeQueues()  

    pyPipeline.initializeStages()    

    pyPipeline.startSlices()  

    pyPipeline.startInitQueue()    # place an empty clipboard in the first Queue 

    pyPipeline.startStagesLoop()

    pyPipeline.shutdown()


