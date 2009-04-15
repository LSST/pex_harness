import pdb                          # we may want to say pdb.set_trace()
import unittest

import lsst.utils.tests as tests
from lsst.pex.policy import Policy
from lsst.daf.base import PropertySet
from lsst.pex.exceptions import LsstCppException, LsstException
import lsst.pex.harness as pexHarness
import lsst.pex.harness.SimpleStageTester
from lsst.pex.harness.EventStage import EventStage
from lsst.pex.harness.Stage import Stage
from lsst.pex.logging import Log, Debug, LogRec, Prop

class EventStageTestCase(unittest.TestCase):
    def testKeyParse(self):
        stage = EventStage(0, Policy())
        (ev, prop) = stage._parseKeysToPublish("foo=bar")
        self.assert_(ev == "foo",
                     "Failed to parse event name from policy item: foo=bar")
        self.assert_(prop == "bar",
                     "Failed to parse property name from policy item: foo=bar")
        (ev, prop) = stage._parseKeysToPublish("foobar")
        self.assert_(ev == "foobar",
                     "Failed to parse event name from policy item: foobar")
        self.assert_(prop == "foobar",
                     "Failed to parse property name from policy item: foobar")

    def testPreprocess(self):
        print "Events in Preprocess..."
        pol = Policy()
        pol.add("RunMode", "preprocess")
        pol.add("keysToPublish", "loose=change")
        pol.add("keysToPublish", "planned=obselesence")
        self._runStageWithPolicy(pol)
        
    def testDefRunMode(self):
        print "Events in Default RunMode..."
        pol = Policy()
        pol.add("keysToPublish", "loose=change")
        pol.add("keysToPublish", "planned=obselesence")
        self._runStageWithPolicy(pol)
        
    def testPostprocess(self):
        print "Events in Postprocess..."
        pol = Policy()
        pol.add("RunMode", "postprocess")
        pol.add("keysToPublish", "loose=change")
        pol.add("keysToPublish", "planned=obselesence")
        self._runStageWithPolicy(pol)
        
    def _runStageWithPolicy(self, policy):
        stage = EventStage(0, policy)
        tester = pexHarness.SimpleStageTester.test(stage)
        tester.setDebugVerbosity(5)

        keysToPublish = policy.getArray("keysToPublish")
        clipboard = { }
        for key in keysToPublish:
            key = stage._parseKeysToPublish(key)[1]
            clipboard[key] = PropertySet()
            clipboard[key].setString("foo", "bar")

        self.assertRaises(LsstException, tester.runMaster, clipboard)
        stage.setEventBrokerHost("lsst4.ncsa.uiuc.edu")
        clipboard = tester.runMaster(clipboard)

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def suite():
    """Returns a suite containing all the test cases in this module."""
    tests.init()

    suites = []
    suites += unittest.makeSuite(EventStageTestCase)

    return unittest.TestSuite(suites)

if __name__ == "__main__":
    tests.run(suite())


