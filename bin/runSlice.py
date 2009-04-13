#! /usr/bin/env python

from lsst.pex.harness.Queue import Queue
from lsst.pex.harness.Stage import Stage
from lsst.pex.harness.Clipboard import Clipboard
from lsst.pex.harness.Slice import Slice

import lsst.pex.policy as policy
from lsst.pex.logging import Log

import lsst.daf.base as dafBase

import lsst.ctrl.events as events

import os
import sys
import optparse, traceback

usage = """Usage: %prog [-dvqs] [-l lev] policy runID"""
desc = """Execute a slice worker process for a pipeline described by the
given policy, assigning it the given run ID.  This should not be executed
outside the context of a pipline harness process.  
"""

cl = optparse.OptionParser(usage=usage, description=desc)
cl.add_option("-l", "--log-threshold", type="int", action="store",
              dest="logthresh", default=None, metavar="lev",
              help="the logging message level threshold")

def main():
    """parse the input arguments and execute the pipeline
    """

    (cl.opts, cl.args) = cl.parse_args()

    if(len(cl.args) < 2):
        print >> sys.stderr, \
            "%s: missing required argument(s)." % cl.get_prog_name()
        print cl.get_usage()
        sys.exit(1)

    pipelinePolicyName = cl.args[0]
    runId = cl.args[1]

    runSlice(pipelinePolicyName, runId, cl.opts.logthresh)

def runSlice(policyFile, runId, logthresh=None):
    """
    runSlice: Slice Main execution 
    """
    pySlice = Slice(runId, policyFile)
    if isinstance(logthresh, int):
        pySlice.setLogThreshold(logthresh)

    pySlice.configureSlice()   

    pySlice.initializeQueues()     

    pySlice.initializeStages()   

    # pySlice.startInitQueue()

    pySlice.startStagesLoop()

    pySlice.shutdown()


if (__name__ == '__main__'):
    try:
        main()
    except Exception, e:
        log = Log(Log.getDefaultLog(),"runSlice")
        log.log(Log.FATAL, str(e))
        traceback.print_exc(file=sys.stderr)
        sys.exit(2);
