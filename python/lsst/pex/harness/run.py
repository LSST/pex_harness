#! /usr/bin/env python

from __future__ import with_statement
import re, sys, os

import lsst.pex.exceptions as pexExcept
from lsst.pex.logging import Log
from lsst.pex.policy import Policy

msglev = { "silent": -30,
           "quiet":  -10,
           "info":     0,
           "trace":    1,
           "verb1":    1,
           "verb2":    2,
           "verb3":    3,
           "debug":   10,
           "policy":  None,
           "none":    None  }

pkgdirvar = "PEX_HARNESS_DIR"

class RunSetupException(pexExcept.LsstException):
    """
    An error occurred while processing user input or environment at the 
    initial stages of the execution of a script.  
    """
    pass

class UsageError(RunSetupException):
    """
    An error occurred while processing the scripts command-line arguments.
    Typically, when this exception is caught, the message is printed along
    with the script usage line, and then the script exits.
    """
    pass
    

def addVerbosityOption(clparser, shortopt="L", dest="verbosity"):
    """
    add a command-line option that control message verbosity 
    """
    clparser.add_option("-"+shortopt, "--log-verbosity", type="str", 
                        action="store", dest=dest, default=None, 
                        metavar="lev",
                        help="string or integer message verbosity level level for the pipeline: silent=-21, quiet=-10, info=0, trace=1, verb1=1, verb2=2, verb3=3, debug=10, policy=consult policy")

def addAllVerbosityOptions(clparser, shortopt="L", dest="verbosity"):
    """
    add command-line options that control message verbosity.  This adds the 
    --log-verbosity option (via addVerbosityOption()) along with several extra
    convenience options (-v, -q, -s, -d)
    """
    addVerbosityOption(clparser, shortopt, dest)
    clparser.add_option("-d", "--debug", action="store_const", const='debug',
                        dest=dest, help="print maximum amount of messages")
    clparser.add_option("-v", "--verbose", action="store_const", const='verb3',
                        dest=dest, 
                        help="print extra messages (same as -L verb3)")
    clparser.add_option("-q", "--quiet", action="store_const", const='quiet',
                        dest=dest, help="print only warning & error messages")
    clparser.add_option("-s", "--silent", action="store_const", const='silent',
                        dest=dest, help="print nothing (if possible)")


def verbosity2threshold(level, defthresh=None):
    """convert the requested verbosity level into a logging threshold.  
    The input level can be given as a logical name or an integer.  An integer
    verbosity level is the negative of the required threshold.
    """
    if level is None:  return defthresh

    if isinstance(level, str):
        level = level.lower()
        if msglev.has_key(level):
            level = msglev[level]
        else:
            try:
                level = int(level)
            except:
                msg = """Unrecognized verbosity level: %s
   Give integer or one of (silent,quiet,info,trace,verb1,verb2,verb3,debug)"""
                raise UsageError(msg % level)

    elif not isinstance(level, int):
        raise UsageError, "Verbosity level is not an integer or string"

    if level is None:
        return defthresh

    return -1 * level

def createLog():
    log = Log(Log.getDefaultLog(), "harness.run.launchPipeline")
    return log


def launchPipeline(policyFile, runid, name=None, verbosity=None, logdir=None):
    if not os.environ.has_key(pkgdirvar):
        raise RuntimeError(pkgdirvar + " env. var not setup")

    logger = createLog()

    logger.log(Log.INFO, "policyFile " + policyFile) 
    logger.log(Log.INFO, "runid " + runid) 
    if(name == None): 
        logger.log(Log.INFO, "name is None") 
    else:
        logger.log(Log.INFO, name) 

    if(verbosity == None): 
        logger.log(Log.INFO, "verbosity is None") 
    else:
        logger.log(Log.INFO, verbosity) 

    clineOptions = ""
    if name is not None:
        clineOptions += " -n %s" % name
    if verbosity is not None:
        clineOptions += " -V %s" % verbosity

    clineOptions += " -g %s" % logdir

    cmd = "runPipelin.sh.py %s %s %s" % \
          (clineOptions, policyFile, runid)

    logger.log(Log.INFO, "CMD to execute:") 
    logger.log(Log.INFO, cmd) 

    os.execvp("runPipeline.py", cmd.split())

    raise RuntimeError("Failed to exec runPipeline.py")


