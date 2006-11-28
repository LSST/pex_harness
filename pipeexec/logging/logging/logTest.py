#!/usr/bin/env python
"""
Sample code demonstrating use of the python interface to lsst logger

Author:  Michelle Miller
Date:    Aug. 3, 2006

Usage:
    log.log (message, priority, source)

logging.policy file sets central machine and logfile location -not yet
"""

import log

log.log ("Test1 of LSST logging", "DEBUG", "LogTest")
log.log ("Test2 of LSST logging", "INFO", "")

#for i in range(1,10):
   #message = "Test %d of LSST logging" % (i)
   #log.log (message, "DEBUG", "logTest")

