#! /usr/bin/env python

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

"""
test the lsst.pex.harness.run module
"""
import lsst.pex.harness.run as run
import optparse
import unittest

import lsst.utils.tests as tests

class RunFunctionsTestCase(unittest.TestCase):

    def testVerbOpt(self):
        cl = optparse.OptionParser()
        run.addVerbosityOption(cl, "P")

        self.assert_(cl.has_option("--log-verbosity"), 
                     "Failed to set verbosity option")
        self.assert_(cl.has_option("-P"), 
                     "Failed to set verbosity option as -P")

        (opts, args) = cl.parse_args(["testrun.py"])
        self.assert_(opts.verbosity is None, "unexpected default for verbosity")

        for name in "silent quiet info trace verb1 verb2 verb3 debug".split():
            self.assert_(run.msglev.has_key(name), 
                         "Message level name not defined: " + name)

            (opts, args) = cl.parse_args("testrun.py -P".split() + [name])
            
            self.assert_(
                run.verbosity2threshold(opts.verbosity) == -1 * run.msglev[name],
                "failed to translate verbosity level: " + opts.verbosity)

        self.assert_(run.verbosity2threshold(3) == -3, 
                     "failed to translate integer verbosity level: 3")
        self.assert_(run.verbosity2threshold('-4') == 4, 
                     "failed to translate string verbosity level: '-4'")
        self.assertRaises(run.UsageError, run.verbosity2threshold, "goob")
        self.assertRaises(run.UsageError, run.verbosity2threshold, 43.0)

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def suite():
    """Returns a suite containing all the test cases in this module."""
    tests.init()

    suites = []
    suites += unittest.makeSuite(RunFunctionsTestCase)

    return unittest.TestSuite(suites)

if __name__ == "__main__":
    tests.run(suite())

