#! /usr/bin/env python
"""
This example illustrates how to use the SimpleStageTester class to test
a single stage.  See in-lined comments for details.

Multiple, chained stages could be tested in one script using
multiple instances of SimpleStageTester; the script would simply pass the
clipboard explicitly from one tester to the next.  
"""
import lsst.pex.harness as pexHarness
import lsst.pex.harness.Stage
import lsst.pex.harness.SimpleStageTester
import lsst.pex.policy as pexPolicy
from lsst.pex.exceptions import LsstCppException

def main():

    # First create a tester.  To ensure that automatic Stage creation
    # works properly, use SimpleStageTester.create(), passing in the
    # fully qualified stage class name along with the policy file name.
    # 
    file = pexPolicy.DefaultPolicyFile("pex_harness",
                                       "examples/AreaStagePolicy.paf")
    tester = pexHarness.SimpleStageTester.create(AreaStage, file)

    # Alternatively, you can instantiate the stage instance yourself, 
    # passing in the policy.
    #
    # file = pexPolicy.DefaultPolicyFile("pex_harness",
    #                                    "examples/AreaStagePolicy.paf")
    # stagePolicy = pexPolicy.Policy.createPolicy(file)
    # stage = AreaStage(0, stagePolicy)
    # tester = pexHarness.SimpleStageTester.test(stage)

    # create a simple dictionary with the data expected to be on the
    # stage's input clipboard.  If this includes images, you will need to 
    # read in and create the image objects yourself.
    clipboard = dict( width=1.0, height=2.0 )

    # you can either test the stage as part of a Master slice (which runs
    # its preprocess() and postprocess() functions)...
    outMaster = tester.runMaster(clipboard)

    # ...or you can test it as part of a Worker.  Note that in the current
    # implementation, the output clipboard is the same instance as the input
    # clipboard.  
    clipboard = dict( width=1.0, height=2.0 )
    outWorker = tester.runWorker(clipboard)

    print "Area =", outWorker.get("area")

class AreaStage(pexHarness.Stage.Stage):

    def __init__(self, stageId=-1, policy=None):
        """configure this stage with a policy"""
        pexHarness.Stage.Stage.__init__(self, stageId, policy)
        self.clipboard = None
        self.inputScale = 0
        self.outputScale = 0
        if policy is not None:
            try: 
                self.inputScale = policy.get("inputScale")
            except LsstCppException:
                pass
            try: 
                self.outputScale = policy.get("outputScale")
            except LsstCppException:
                pass

    def preprocess(self):
        self.clipboard = self.inputQueue.getNextDataset()
        if self.clipboard is not None:
            if not self.clipboard.contains("width"):
                raise RuntimeError("Missing width on clipboard")
            if not self.clipboard.contains("height"):
                raise RuntimeError("Missing width on clipboard")

    def process(self):
        self.clipboard = self.inputQueue.getNextDataset()
        if self.clipboard is not None:
            area = self.clipboard.get("width") * self.clipboard.get("height")*\
                   (10.0**self.inputScale/10.0**self.outputScale)**2
            self.clipboard.put("area", area)
            self.outputQueue.addDataset(self.clipboard)

    def postprocess(self):
        if self.clipboard is not None:
            self.outputQueue.addDataset(self.clipboard)

if __name__ == "__main__":
    main()
