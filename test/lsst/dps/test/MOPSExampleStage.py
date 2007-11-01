#! /usr/bin/env python

"""
Test Application Stage 1
"""

from lsst.dps.Stage import Stage

import lsst.mwi.data as datap

class MOPSExampleStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python MOPSExampleStage preprocess : stageId %d' % self.stageId
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
	    print 'Python MOPSExampleStage preprocess(): stageId %i key %s value %s' % (self.stageId, key, inputParamPropertyPtrType)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.") 

            if (key == "mops1Event"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "FOVRA"):
                        self.fovra = dpPtr.getValueDouble()
                        print 'Python MOPSExampleStage preprocess() '
                        print 'Python MOPSExampleStage preprocess() ', key, dataPropKey, self.fovra
                    elif (dataPropKey == "FOVDec"):
                        self.fovdec = dpPtr.getValueDouble()
                        print 'Python MOPSExampleStage preprocess() '
                        print 'Python MOPSExampleStage preprocess() ', key, dataPropKey, self.fovdec
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python MOPSExampleStage preprocess() '
                        print 'Python MOPSExampleStage preprocess() ', key, dataPropKey, self.fovid
                    elif (dataPropKey == "FOVTime"):
                        self.fovtime = dpPtr.getValueString()
                        print 'Python MOPSExampleStage preprocess() '
                        print 'Python MOPSExampleStage preprocess() ', key, dataPropKey, self.fovtime


    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python MOPSExampleStage process : _rank %i stageId %d' % (self._rank, self.stageId)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            # print 'Python MOPSExampleStage process(): _rank %i stageId %i key %s value %s' % (self._rank, self.stageId, key, inputParamPropertyPtrType)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")

            if (key == "mops1Event"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "FOVRA"):
                        self.fovra = dpPtr.getValueDouble()
                        print 'Python MOPSExampleStage process() ', self._rank
                        print 'Python MOPSExampleStage process() ', self._rank, key, dataPropKey, self.fovra
                    elif (dataPropKey == "FOVDec"):
                        self.fovdec = dpPtr.getValueDouble()
                        print 'Python MOPSExampleStage process() ', self._rank
                        print 'Python MOPSExampleStage process() ', self._rank, key, dataPropKey, self.fovdec
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python MOPSExampleStage process() ', self._rank
                        print 'Python MOPSExampleStage process() ', self._rank, key, dataPropKey, self.fovid
                    elif (dataPropKey == "FOVTime"):
                        self.fovtime = dpPtr.getValueString()
                        print 'Python MOPSExampleStage process() ', self._rank
                        print 'Python MOPSExampleStage process() ', self._rank, key, dataPropKey, self.fovtime

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python MOPSExampleStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)


