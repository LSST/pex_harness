#! /usr/bin/env python

"""
Test Application Stage 1
"""

from lsst.dps.Stage import Stage

import lsst.mwi.data as datap

class App1Stage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python App1Stage preprocess : stageId %i' % self.stageId
	print 'Python App1Stage preprocess : universeSize %i' % self._universeSize
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
	    print 'Python App1Stage preprocess(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.") 
	    print dataPropertyKeyList

            if (key == "mops1Event"):
                for dataPropKey in dataPropertyKeyList:
	            print 'Python App1Stage preprocess(): stageId %i dataPropKey %s' % (self.stageId, dataPropKey)
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "FOVRA"):
                        self.fovra = dpPtr.getValueDouble()
                        print 'Python App1Stage preprocess() '
                        print 'Python App1Stage preprocess() ', key, dataPropKey, self.fovra
                    elif (dataPropKey == "FOVDec"):
                        self.fovdec = dpPtr.getValueDouble()
                        print 'Python App1Stage preprocess() '
                        print 'Python App1Stage preprocess() ', key, dataPropKey, self.fovdec
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python App1Stage preprocess() '
                        print 'Python App1Stage preprocess() ', key, dataPropKey, self.fovid
                    elif (dataPropKey == "FOVTime"):
                        self.fovtime = dpPtr.getValueString()
                        print 'Python App1Stage preprocess() '
                        print 'Python App1Stage preprocess() ', key, dataPropKey, self.fovtime


    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python App1Stage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python App1Stage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python App1Stage process(): _rank %i stageId %i key %s' % (self._rank, self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")

            if (key == "mops1Event"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "FOVRA"):
                        self.fovra = dpPtr.getValueDouble()
                        print 'Python App1Stage process() ', self._rank
                        print 'Python App1Stage process() ', self._rank, key, dataPropKey, self.fovra
                    elif (dataPropKey == "FOVDec"):
                        self.fovdec = dpPtr.getValueDouble()
                        print 'Python App1Stage process() ', self._rank
                        print 'Python App1Stage process() ', self._rank, key, dataPropKey, self.fovdec
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python App1Stage process() ', self._rank
                        print 'Python App1Stage process() ', self._rank, key, dataPropKey, self.fovid
                    elif (dataPropKey == "FOVTime"):
                        self.fovtime = dpPtr.getValueString()
                        print 'Python App1Stage process() ', self._rank
                        print 'Python App1Stage process() ', self._rank, key, dataPropKey, self.fovtime

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python App1Stage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)


