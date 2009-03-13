import lsst.pex.harness as pexHarness

class StageRunner:
    def __init__(self, stageNameList):
        stageNum = 0
        self.stageList = []
        self.queueList = []
        self.queueList.append(pexHarness.Queue())
        for stageName in stageNameList:
            tokenList = stageName.split('.')
            classString = tokenList.pop()
            classString = classString.strip()
            package = ".".join(tokenList)
            print "Importing", package, classString
            AppStage = __import__(package, globals(), locals(),
                    [classString], -1) 
            print "Got AppStage", AppStage
            StageClass = getattr(AppStage, classString)
            print "Got StageClass", StageClass
            stageObject = StageClass(stageNum)
            print "Got stageObject", stageObject
            self.stageList.append(stageObject)
            self.queueList.append(pexHarness.Queue())
            stageObject.initialize(self.queueList[stageNum + 1],
                    self.queueList[stageNum])
            stageObject.setUniverseSize(1)
            stageObject.setRun("tEsTrUn")

    def setPolicy(self, stageNum, policy):
        self.stageList[stageNum]._policy = policy

    def run(self, clipboard):
        self.queueList[0].addDataset(clipboard)
        for stageNum in xrange(len(self.stageList)):
            self.stageList[stageNum].process()
        return self.queueList[len(self.stageList)].getNextDataset()
