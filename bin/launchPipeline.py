#! /usr/bin/env python
#
from __future__ import with_statement
import re, sys, os, os.path, shutil, subprocess
import optparse, traceback
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
import lsst.pex.harness.run as run

usage = """usage: %prog policy_file runid [pipelineName] [-vqsd] [-L lev] [-n file]"""

desc = """
Launch a pipeline with a given policy and Run ID.  If a node list file is not 
provided via the -n option, a file called "nodelist.scr" in the current 
directory will be used.  If the policy_file refers to other policy files, 
the path to those files will taken to be relative to the current directory.
If a log verbosity is not specified, the default will be taken from the 
policy file.
"""

cl = optparse.OptionParser(usage=usage, description=desc)
run.addAllVerbosityOptions(cl)
cl.add_option("-n", "--nodelist", action="store", dest="nodelist", 
              metavar="file", help="file containing the MPI machine list")
cl.add_option("-g", "--logdir", action="store", dest="logdir", 
              metavar="file", help="directory into which log files will be written")

# command line results
cl.opts = {}
cl.args = []

pkgdirvar = "PEX_HARNESS_DIR"

def createLog():
    log = Log(Log.getDefaultLog(), "harness.launchPipeline")
    return log

def setVerbosity(verbosity):
    logger.setThreshold(run.verbosity2threshold(verbosity, -1))  

logger = createLog()

def main():
    try:
        (cl.opts, cl.args) = cl.parse_args();
        setVerbosity(cl.opts.verbosity)

        if len(cl.args) < 1:
            print usage
            raise RuntimeError("Missing arguments: pipeline_policy_file runId")
        if len(cl.args) < 2:
            print usage
            raise RuntimeError("Missing argument: runid")

        name = None
        if len(cl.args) > 2:
            name = cl.args[2]
    
        logger.log(Log.INFO, "command line option 0 : policyFile :  " + cl.args[0])
        logger.log(Log.INFO, "command line option 1 : runid :  " + cl.args[1])

        if (cl.opts.logdir == None):
            logger.log(Log.INFO, "command line logdir option is None ")
        else:
            logger.log(Log.INFO, "command line logdir option : " + cl.opts.logdir)

        if (name == None):
            logger.log(Log.INFO, "name is None")
        else:
            logger.log(Log.INFO, name)

        if (cl.opts.verbosity == None):
            logger.log(Log.INFO, "verbosity option not specified")
        else:
            logger.log(Log.INFO, cl.opts.verbosity)
    
        run.launchPipeline(cl.args[0], cl.args[1], name, cl.opts.verbosity, cl.opts.logdir)

    except SystemExit:
        pass
    except:
        tb = traceback.format_exception(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2])
        logger.log(Log.FATAL, tb[-1].strip())
        logger.log(Log.DEBUG, "".join(tb[0:-1]).strip())
        sys.exit(1)

if __name__ == "__main__":
    main()
    
