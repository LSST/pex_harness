#! /usr/bin/env python
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

