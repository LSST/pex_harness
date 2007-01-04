from lsst.fw.Policy import Policy


#=====================================================================
#                           batch
#=====================================================================
class batch(object):
    """
    internal variables
        batchPolicyFile   policy file defining batch processing 
        batchPolicy       policy class derived from batchPolicyFile
    """
    #------------------------------------------------------------------------
    def __init__(self,batchPolicyFile):
        """
        input
            batchPolicyFile  policy file defining batch processing 

        output
            none
        """
        # acquire the sequence of batch stages 
        self.batchPolicyFile = batchPolicyFile
        self.batchPolicy = Policy(batchPolicyFile)

    #------------------------------------------------------------------------
    def printParams(self):
        """
        """
        print "=========================================================\n"
        print "batchPolicy: ",self.batchPolicyFile
        print "numCCD: %s" %(self.batchPolicy.Get('numCCD'))
        print "numProcs: %s" %(self.batchPolicy.Get('nodes'))
        print "queue: %s" %(self.batchPolicy.Get('queue'))
        print "jobname: %s" %(self.batchPolicy.Get('jobname'))
        print "walltime: %s" %(self.batchPolicy.Get('walltime'))
        print "stdout: %s" %(self.batchPolicy.Get('stdout'))
        print "stderr: %s" %(self.batchPolicy.Get('stderr'))
        print "accnt: %s" %(self.batchPolicy.Get('accnt'))

