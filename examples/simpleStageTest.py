#! /usr/bin/env python
"""
This example illustrates not only how to create a simple Stage, but also
how to use to test it using the SimpleStageTester class.  See in-lined
comments for details.

Multiple, chained stages could be tested in one script using
multiple instances of SimpleStageTester; the script would simply pass the
clipboard explicitly from one tester to the next.  
"""
import lsst.pex.harness as pexHarness
import lsst.pex.harness.SimpleStageTester
import lsst.pex.policy as pexPolicy
from lsst.pex.harness.Stage import Stage
from lsst.pex.logging import Log, Debug, LogRec, Prop
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

    # set the verbosity of the logger.  If the level is at least 5, you
    # will see debugging messages from the SimpleStageTester wrapper.
    tester.setDebugVerbosity(5)

    # if you want to see all log message properties (including the DATE)
    # uncomment this line:
    # tester.showAllLogProperties(False)

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

# Below is our example Stage implmentation

class AreaStage(Stage):
    """
    a simple Stage implmentation that calculates the area of a rectangle
    given the length of the sides
    """

    def __init__(self, stageId=-1, policy=None):
        """configure this stage with a policy"""

        # it's usually a good idea to call the super constructor
        pexHarness.Stage.Stage.__init__(self, stageId, policy)
        self.clipboard = None

        # You should create a default policy file that is installed
        # with your Stage implmenetation's package and merge it with
        # that policy that is handed to you by the framework.
        #
        # Here's how you do it.  Note that the default policy file can
        # be a dictionary.  Here, we indicated "examples" as the so-called
        # default policy repository for this package; however normally,
        # this is "pipeline".  
        file = pexPolicy.DefaultPolicyFile("pex_harness",   # package name
                                      "AreaStagePolicy_dict.paf", # def. policy
                                           "examples" # dir containing policies
                                           )
        defpol = pexPolicy.Policy.createPolicy(file, file.getRepositoryPath())
        if policy is None:
            policy = defpol
        else:
            policy.mergeDefaults(defpol)

        # now we can configure our pipeline from the policy (which should
        # now be complete).  An exception will be thrown if the merged 
        # policy is incomplete.  
        self.inputScale = policy.get("inputScale")
        self.outputScale = policy.get("outputScale")

        # if we want to do some logging, this is a good time to create
        # the log.  Here we assume that all of this Stage will use the
        # same logger.  This uses the threshold for message printing
        # inherited from the root logger.
        #
        # Use this for standard logger:
        # self.log = Log(Log.getDefaultLog(), "AreaStage")
        #
        # Use this for additional debugging API sugar:
        self.log = Debug("AreaStage")
        if self.outputScale != 0:
            self.log.log(Log.INFO, "Area scaling factor: %i"% self.outputScale)

    # Usually, the initialize function does not need to be overridden;
    # however, you may do so if you need to do some one-time setup that 
    # depends on the the slice ID (as returned by getRank()).  Be sure to
    # call the super version
    def initialize(self, outQueue, inQueue):
        Stage.initialize(self, outQueue, inQueue)
        self.log.debug(3, "Running with sliceID = %s" % self.getRank())

    # Most often, one need only to provide a process() implementation; this
    # this is the code that will get run in parallel.  preprocess() gets
    # execute only on the master node prior to process, and postprocess(),
    # afterward.  We provide a pre- and postprocess() here mainly as an
    # example; our excuse is to check that the clipboard has the inputs
    # we need.  
    
    def preprocess(self):
        # on the master, pull the next clipboard
        self.clipboard = self.inputQueue.getNextDataset()

        # do our work
        if self.clipboard is not None:
            if not self.clipboard.contains("width"):
                raise RuntimeError("Missing width on clipboard")
            if not self.clipboard.contains("height"):
                raise RuntimeError("Missing width on clipboard")

        # if you are worried about overhead of formatting a debug message,
        # you can wrap it in an if block
        if self.log.sends(Log.DEBUG):
            # this attaches properties to our message
            LogRec(self.log, Log.DEBUG) << "all input data found."           \
                             << Prop("width", self.clipboard.get("width"))   \
                             << Prop("height", self.clipboard.get("height")) \
                             << LogRec.endr


    def process(self):
        # in a worker, pull the next clipboard
        self.clipboard = self.inputQueue.getNextDataset()
        if self.clipboard is not None:

            # do our work
            area = self.clipboard.get("width") * self.clipboard.get("height")*\
                   (10.0**self.inputScale/10.0**self.outputScale)**2

            # maybe you want to write a debug message
            self.log.debug(3, "found area of %f" % area)

            # save the results to the clipboard
            self.clipboard.put("area", area)

            # pass the clipboard to the next stage (in this slice)
            self.outputQueue.addDataset(self.clipboard)

    def postprocess(self):
        # on the master, send clipboard to the next stage
        if self.clipboard is not None:
            self.outputQueue.addDataset(self.clipboard)

if __name__ == "__main__":
    main()
