#! /usr/bin/env python
#
from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import optparse, traceback
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

usage = """usage: %%prog policy_file runid [-vqs] [-V int] [-n file]

Launch a pipeline with a given policy and Run ID.  This launching script 
ensures that .mpd.conf files are in place by running ensureMpdConf on each 
node in the node list file.  If a node list file is not provided via the 
-n option, a file called "nodelist.scr" in the current directory will be used.
"""

cl = optparse.OptionParser(usage)
cl.add_option("-V", "--verbosity", type="int", action="store",
              dest="verbosity", default=0, metavar="int",
              help="verbosity level (0=normal, 1=debug, -1=quiet, -3=silent)")
cl.add_option("-v", "--verbose", action="store_const", const=1,
              dest="verbosity",
              help="print extra messages")
cl.add_option("-q", "--quiet", action="store_const", const=-1,
              dest="verbosity",
              help="print only warning & error messages")
cl.add_option("-s", "--silent", action="store_const", const=-3,
              dest="verbosity",
              help="print only warning & error messages")
cl.add_option("-n", "--nodelist", action="store", dest="nodelist", 
              metavar="file", help="file containing the MPI machine list")

# command line results
cl.opts = {}
cl.args = []

pkgdirvar = "PEX_HARNESS_DIR"

def createLog():
    log = Log(Log.getDefaultLog(), "harness.launchPipeline")
    return log

def setVerbosity(verbosity):
    logger.setThreshold(-10 * verbosity)  

logger = createLog()

def main():
    try:
        (cl.opts, cl.args) = cl.parse_args();
        setVerbosity(cl.opts.verbosity)

        if len(cl.args) < 1:
            print usage
            raise RuntimeError("Missing arguments: dc3pipe_policy_file runId")
        if len(cl.args) < 2:
            print usage
            raise RuntimeError("Missing argument: runid")
    
        launchPipeline(cl.args[0], cl.args[1])

    except:
        tb = traceback.format_exception(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2])
        logger.log(Log.FATAL, tb[-1].strip())
        logger.log(Log.DEBUG, "".join(tb[0:-1]).strip())
        sys.exit(1)

def launchPipeline(policyFile, runid):
    if not os.environ.has_key(pkgdirvar):
        raise RuntimeError(pkgdirvar + " env. var not setup")

    nodesfile = "nodelist.scr"
    if cl.opts.nodelist is not None:
        nodesfile = cl.opts.nodelist

    # ensure we have .mpd.conf files deployed on all nodes
    nodes_set = []
    nnodes = 0
    nprocs = 0
    with file(nodesfile) as nodelist:
        for node in nodelist:
            node = node.strip()
            if (node.startswith('#')): continue
            (node, n) = node.split(':')
            nnodes += 1
            n = n.strip()
            if n != '':
                nprocs += int(n)
            else:
                nprocs += 1

            if node in nodes_set: continue

    cmd = "runPipelin.sh.py %s %s %s %d %d" % \
          (policyFile, runid, nodesfile, nnodes, nprocs)
    logger.log(Log.DEBUG, "exec " + cmd)
    os.execvp("runPipeline.sh", cmd.split())

    raise RuntimeError("Failed to exec runPipeline.sh")

def getNode(nodeentry):
    colon = nodeentry.find(':')
    if colon < 1:  
        return nodeentry
    else:
        return nodeentry[0:colon]

if __name__ == "__main__":
    main()
    
