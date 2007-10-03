#! /usr/bin/env python

"""
Test Application Stage 2
"""

from Stage import Stage

class App2Stage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        This method will be overwritten by the Stage subclass
        """
	print 'Python App2Stage preprocess : stageId %d' % self.stageId
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            # print 'Python App2Stage preprocess(): stageId %i key %s value %s' % (self.stageId, key, inputParamPropertyPtrType)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")

            if (key == "assocTriggerEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "DMATCH"):
                        self.dmatch = dpPtr.getValueFloat()
                        print 'Python App2Stage preprocess() '
                        print 'Python App2Stage preprocess() ', key, dataPropKey, self.dmatch
                    elif (dataPropKey == "DIALOC"):
                        self.dialoc = dpPtr.getValueString()
                        print 'Python App2Stage preprocess() '
                        print 'Python App2Stage preprocess() ', key, dataPropKey, self.dialoc
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python App2Stage preprocess() '
                        print 'Python App2Stage preprocess() ', key, dataPropKey, self.fovid

            if (key == "mops1Event"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "FOVRA"):
                        self.fovra = dpPtr.getValueFloat()
                        print 'Python App2Stage preprocess() '
                        print 'Python App2Stage preprocess() ', key, dataPropKey, self.fovra
                    elif (dataPropKey == "FOVDec"):
                        self.fovdec = dpPtr.getValueFloat()
                        print 'Python App2Stage preprocess() '
                        print 'Python App2Stage preprocess() ', key, dataPropKey, self.fovdec
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python App2Stage preprocess() '
                        print 'Python App2Stage preprocess() ', key, dataPropKey, self.fovid
                    elif (dataPropKey == "FOVTime"):
                        self.fovtime = dpPtr.getValueString()
                        print 'Python App2Stage preprocess() '
                        print 'Python App2Stage preprocess() ', key, dataPropKey, self.fovtime


    #------------------------------------------------------------------------
    def process(self):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python App2Stage process : _rank %i stageId %d' % (self._rank, self.stageId)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            # print 'Python App2Stage process(): _rank %i stageId %i key %s value %s' % 
            #                 (self._rank, self.stageId, key, inputParamPropertyPtrType)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")

            if (key == "assocTriggerEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "DMATCH"):
                        self.dmatch = dpPtr.getValueFloat()
                        print 'Python App2Stage process() ', self._rank
                        print 'Python App2Stage process() ', self._rank, key, dataPropKey, self.dmatch
                    elif (dataPropKey == "DIALOC"):
                        self.dialoc = dpPtr.getValueString()
                        print 'Python App2Stage process() ', self._rank
                        print 'Python App2Stage process() ', self._rank, key, dataPropKey, self.dialoc
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python App2Stage process() ', self._rank
                        print 'Python App2Stage process() ', self._rank, key, dataPropKey, self.fovid

            if (key == "mops1Event"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "FOVRA"):
                        self.fovra = dpPtr.getValueFloat()
                        print 'Python App2Stage process() ', self._rank
                        print 'Python App2Stage process() ', self._rank, key, dataPropKey, self.fovra
                    elif (dataPropKey == "FOVDec"):
                        self.fovdec = dpPtr.getValueFloat()
                        print 'Python App2Stage process() ', self._rank
                        print 'Python App2Stage process() ', self._rank, key, dataPropKey, self.fovdec
                    elif (dataPropKey == "FOVID"):
                        self.fovid = dpPtr.getValueString()
                        print 'Python App2Stage process() ', self._rank
                        print 'Python App2Stage process() ', self._rank, key, dataPropKey, self.fovid
                    elif (dataPropKey == "FOVTime"):
                        self.fovtime = dpPtr.getValueString()
                        print 'Python App2Stage process() ', self._rank
                        print 'Python App2Stage process() ', self._rank, key, dataPropKey, self.fovtime


        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        This method will be overwritten by the Stage subclass
        """
	print 'Python App2Stage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)


