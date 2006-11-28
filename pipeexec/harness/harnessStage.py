#! /usr/bin/env python
import os
import sys
import optparse
from lsst.mw.dps.pipeexec.nlog import nlog

DefPipelinePolicy = "Pipeline.policy"
DefDiskPolicy = "DiskRoot.policy"

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("", "--pipelinePolicy", default=DefPipelinePolicy, 
            metavar="file", 
            help="pipeline policy; default %r" % (DefPipelinePolicy,),)
    parser.add_option("", "--diskPolicy", default=DefDiskPolicy, 
            metavar="file", 
            help="disk locator policy; default %r" % (DefDiskPolicy,),)
    parser.add_option("", "--stageNum", metavar="number",type='int',
            default=None,
            help="stage number within pipeline; required",)
    parser.add_option("", "--sliceNum", metavar="number",type='int',
            default=None,
            help="CCD slice to be processed; required",)
 
    opts, args = parser.parse_args()

    requiredOpts = ("stageNum","sliceNum")
    if opts.stageNum == None or opts.sliceNum == None :
        print "Missing one or more of the required options:", requiredOpts
        parser.print_help()
        sys.exit(1)


    nlog.log("HELLO_WORLD", PROG="harnessStage.py", STAGENUM=opts.stageNum, SLICENUM=opts.sliceNum)

    print("harnessStage.py STAGENUM=%s SLICENUM=%s\n"%(opts.stageNum,opts.sliceNum))

