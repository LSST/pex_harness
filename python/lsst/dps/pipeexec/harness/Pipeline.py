from lsst.fw.Policy import Policy

#=====================================================================
#                           pipeline
#=====================================================================
class pipeline(object):
    """
    internal variables
        pipelinePolicyFile policy file defining pipeline processing stages
        pipelinePolicy    policy class 
        stageCount        count of stages withint this pipeline
        setups[i]         elaboration of stage's setup  requirements
        stages[i]         elaboration of stage's algorithm and file use
    """
    #------------------------------------------------------------------------
    def __init__(self,pipelinePolicyFile):
        """
        input
            pipelinePolicyFile  policy file defining pipeline processing stages

        output
            none
        """
        # acquire the sequence of pipeline stages 
        self.pipelinePolicyFile = pipelinePolicyFile
        self.pipelinePolicy = Policy(pipelinePolicyFile)

        setuplist = self.pipelinePolicy.Get ('setup')
        if (not isinstance(setuplist,list)):
            save = setuplist
            setuplist = []
            setuplist.append(save)

        self.setupCount = len(setuplist)
        self.setups = []
        for i in range(self.setupCount):
            #print "\nPipeline:__init__: ",setuplist[i]
            # parse the setup definitions 
            stagePolicy, diskPolicy, inQ, outQ, stageCode = setuplist[i].split(',',5)
            stagePolicy = stagePolicy.strip()
            diskPolicy = diskPolicy.strip()
            inQ = inQ.strip()
            outQ = outQ.strip()
            stageCode = stageCode.strip()
            self.setups.append((stagePolicy,diskPolicy,inQ,outQ,stageCode))

        stagelist = self.pipelinePolicy.Get ('stage')
        if (not isinstance(stagelist,list)):
            save = stagelist
            stagelist = []
            stagelist.append(save)

        self.stageCount = len(stagelist)
        self.stages = []
        for i in range(self.stageCount):
            #print "\nPipeline:__init__: ",stagelist[i]
            # parse the stage definitions 
            stagePolicy,diskPolicy,inQ,outQ,stageCode =stagelist[i].split(',',5)
            stagePolicy = stagePolicy.strip()
            diskPolicy = diskPolicy.strip()
            inQ = inQ.strip()
            outQ = outQ.strip()
            stageCode = stageCode.strip()
            self.stages.append((stagePolicy,diskPolicy,inQ,outQ,stageCode))

    #------------------------------------------------------------------------
    def printParams(self):
        """
        """
        print "=========================================================\n"
        print "pipelinePolicy: ",self.pipelinePolicy
        print "setupCount: ",self.setupCount
        for i in range(self.setupCount):
            print "Setup: %d   stagePolicy: %s   diskPolicy: %s    inQ: %s    outQ: %s    stageCode: %s "% (i, self.stages[i][0], self.stages[i][1], self.stages[i][2],self.stages[i][3], self.stages[i][4])
        print "stageCount: ",self.stageCount,"\n"
        for i in range(self.stageCount):
            print "Stage: %d   stagePolicy: %s   diskPolicy: %s    inQ: %s    outQ: %s    stageCode: %s "% (i, self.stages[i][0], self.stages[i][1], self.stages[i][2],self.stages[i][3], self.stages[i][4]),"\n"

