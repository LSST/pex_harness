#!/usr/projects/LSST/DC1/python-2.4.2/python-2.4.2/bin/python
#
#  KLUDGE ALERT:  --jmyers--
# Eventually, we should have a C++ policy class like our Python
# Policy class, as well as a C++ nlog class like our Python nlog class.
# (I would argue that we should make this change when we move to C++ 
# and start using XML for policy files.)
#
# However, for now, our C code writes logs to
#    /tmp/lsst.harness.<module>.<pid>.<username>.log
# and this behaviour is *hard-coded!*  Ergo, our PBS script
# *must* forward '/tmp/lsst.harness.*.<username>.log' to the central
# logging node.
#
# And of course, I look forward to the day this hack goes away.
#

import os
import sys
import optparse
from lsst.dps.pipeexec.harness import Pipeline
from lsst.dps.pipeexec.harness import Batch
from lsst.dps.pipeexec.nlog import nlog

DefInputQ = "./queue"
DefBatchPolicy = "Batch.policy"
DefDiskPolicy = "DiskRoot.policy"
DefPipelinePolicy = "Pipeline.policy"
DefExecutable = "./harnessStage.py"

MPICH=True
#=====================================================================
#                        In-line functions
#=====================================================================

#=====================================================================
#                Main Routine 
#=====================================================================
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("", "--rootDir", default=".", metavar="dir",
        help="directory containing policy files; default '.'",
    )
    parser.add_option("", "--pipeline", default=DefPipelinePolicy, metavar="file",
        help="pipeline policy; default %r" % (DefPipelinePolicy,),
    )
    parser.add_option("", "--batch", default = DefBatchPolicy, metavar="file",
        help="batch policy; default %r" % (DefBatchPolicy,),
    )
    parser.add_option("", "--disk", default = DefDiskPolicy, metavar="file",
        help="file locator policy; default %r" % (DefDiskPolicy,),
    )
    parser.add_option("", "--inputQ", default = DefInputQ, metavar="dir",
        help="fullpathname of stage0 input queue; default %r" % (DefInputQ,),
    )
    
    opts, args = parser.parse_args()
    os.environ['LSST_POLICY_DIR'] = opts.rootDir

    loggingPolicy = nlog.getPolicy()

    pipelinePolicyFilename = opts.pipeline
    batchPolicyFilename = opts.batch

    # Setup for PBS queuing of MPI jobs to process the pipeline stages
    pipe = Pipeline.pipeline(pipelinePolicyFilename)
    stageCount = pipe.stageCount

    
    batch = Batch.batch(batchPolicyFilename)
    batch.printParams()
    numCCD = int(batch.batchPolicy["numCCD"])
    numProcs = int(batch.batchPolicy["nodes"])
    queue = batch.batchPolicy["queue"]
    jobname = batch.batchPolicy["jobname"]
    walltime = batch.batchPolicy["walltime"]
    stdout = batch.batchPolicy["stdout"]
    stderr = batch.batchPolicy["stderr"]
    accnt = batch.batchPolicy["accnt"]

    # determine the number of CCDs to be processed on each node.
    numCCDPerProcs = int(  (numCCD + numProcs -1 ) / numProcs    )

    # Create the PBS script which will start all stages across all nodes
    pbsScriptName = "StartStages.scr"

    pbsScript = open(pbsScriptName,'w')
    pbsScript.write("#!/bin/csh\n")
    pbsScript.write("#PBS -q %s\n"%(queue))
    pbsScript.write("#PBS -N %s\n"%(jobname))
    pbsScript.write("#PBS -l nodes=%d:ppn=1\n"%(numProcs))
    pbsScript.write("#PBS -l walltime=%s\n"%(walltime))
    pbsScript.write("#PBS -o %s\n"%(stdout))
    pbsScript.write("#PBS -e %s\n"%(stderr))
    pbsScript.write("#PBS -A %s\n"%(accnt))
    pbsScript.write("#PBS -V\n")
    pbsScript.write("echo $PBS_O_WORKDIR\n")
    pbsScript.write("cd $PBS_O_WORKDIR\n")

    nodeFile = "nodelist.scr" 
    pbsScript.write("cat $PBS_NODEFILE | sort -u > %s\n" %(nodeFile))
    # we'll double-cat the nodefile into our nodelist.  This will
    # double the -usize (universe size) parameter used in mpiexec, thus
    # giving us two mpi nodes per machine (and thus one mpi node per core).
    # the nodefile is also used in stageleader to allocate stageslices,
    # so we need it to have two entries per node.
    pbsScript.write("cat $PBS_NODEFILE | sort -u >> %s\n" %(nodeFile))
    pbsScript.write("cat %s\n" %(nodeFile))
    pbsScript.write("/usr/projects/LSST/mpich2-1.0.3/bin/mpdboot --totalnum=%d --file=%s\n" %(numProcs,nodeFile))

    #run nlforward on all these machines
    #and also run netlogd.py on the head node
    pbsScript.write("set HEAD=`cat $PBS_NODEFILE | head -n 1`\n")
    if loggingPolicy != None:     
        #set up a listener for the logs
        centralLog = loggingPolicy.Get('centralLogFile')
        pbsScript.write("ssh $HEAD netlogd.py --output=" + centralLog + "&\n")
        #now run forwarders for all the logs
        pbsScript.write('foreach NODE ( `cat $PBS_NODEFILE | sort | uniq` )\n')
        pbsScript.write('    ssh $NODE mkdir /dev/shm/' + os.getenv("USER") + '\n')
        pbsScript.write('    ssh $NODE \'rm -f /tmp/lsst.harness.*.' + os.getenv("USER") + '.log\'\n')
        pbsScript.write("    ssh $NODE 'rm -f %s'\n" %(nlog.getLogFileGlob(loggingPolicy)))
        pbsScript.write("    ssh $NODE \"\"\"nlforward.py '%s' x-netlog://$HEAD\"\"\"&\n" %(nlog.getLogFileGlob(loggingPolicy)))
        #see KLUDGE ALERT above
        pbsScript.write("    ssh $NODE \"\"\"nlforward.py '/tmp/lsst.harness.*." + os.getenv("USER") + ".log' x-netlog://$HEAD\"\"\"&\n")
        pbsScript.write("    ssh $NODE nlogtopd.py&\n")
        pbsScript.write("end\n")

    pbsScript.write("@ numNodes = `cat nodelist.scr | wc -l`\n")
    pbsScript.write("@ numNodes = $numNodes * 2\n") # jmyers - so we can use both cores!
    pbsScript.write("/usr/projects/LSST/mpich2-1.0.3/bin/mpirun -usize `cat nodelist.scr | wc -l` -machinefile %s -np 1 manager --policyDir=%s --diskPolicy=%s --pipelinePolicy=%s --numCCD=%d --numStage=%d --inputQ=%s  --nodeList=%s\n" %(nodeFile,opts.rootDir,opts.disk,opts.pipeline,numCCD,stageCount,opts.inputQ,nodeFile))
    # Since backgrounding mpi jobs, supposedly need final 'wait' -
    #we're not backgrounding anything that we need to wait for anymore
    #pbsScript.write('wait\n')
    #give the forwarding daemons plenty of time to send data over the
    #network before they die
    pbsScript.write('sleep 15s\n')
    #clean up after ourselves
    pbsScript.write('foreach NODE ( `cat $PBS_NODEFILE | sort | uniq ` )\n')
    if loggingPolicy != None:
        pbsScript.write("    ssh $NODE 'rm -f %s'\n" %(nlog.getLogFileGlob(loggingPolicy)))
    pbsScript.write('    ssh $NODE \'rm -f /tmp/lsst.harness.*.' + os.getenv('USER') + 'log\'\n')
    pbsScript.write('end\n')
    pbsScript.write('mpdallexit')
    pbsScript.close()

    # Finally start batch job to start mpi jobs to start stages!
    result = os.system('qsub %s' %(pbsScriptName))
    if result != 0:
        print "Error: Problem submitting batch job starting pipelines\n"
    else:
        print "OK: batch job is submitted to start pipelines\n%s" %(result)


