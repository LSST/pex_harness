#! /usr/bin/env python

"""
Test Application Stages for proto association pipeline
"""

from lsst.dps.Stage import Stage

import lsst.mwi.data as datap

class LoadStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.LoadStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.LoadStage preprocess : universeSize %i' % self._universeSize

        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.LoadStage preprocess(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.LoadStage preprocess() ', key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.LoadStage preprocess() ', key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.LoadStage preprocess() ', key, dataPropKey, self.FOVDec


    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.LoadStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.LoadStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.LoadStage process(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.LoadStage process() ', self._rank, key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.LoadStage process() ', self._rank, key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.LoadStage process() ', self._rank, key, dataPropKey, self.FOVDec

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.LoadStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)



class MatchDiaSourceStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.MatchDiaSourceStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.MatchDiaSourceStage preprocess : universeSize %i' % self._universeSize

        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.MatchDiaSourceStage preprocess(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.MatchDiaSourceStage preprocess() ', key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchDiaSourceStage preprocess() ', key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchDiaSourceStage preprocess() ', key, dataPropKey, self.FOVDec

    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.MatchDiaSourceStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.MatchDiaSourceStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.MatchDiaSourceStage process(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.MatchDiaSourceStage process() ', self._rank, key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchDiaSourceStage process() ', self._rank, key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchDiaSourceStage process() ', self._rank, key, dataPropKey, self.FOVDec

        self.outputQueue.addDataset(self.activeClipboard)


    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.MatchDiaSourceStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)



class MatchMopStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.MatchMopStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.MatchMopStage preprocess : universeSize %i' % self._universeSize
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.MatchMopStage preprocess(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.MatchMopStage preprocess() ', key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchMopStage preprocess() ', key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchMopStage preprocess() ', key, dataPropKey, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.MatchMopStage preprocess() ', key, dataPropKey, self.visitId

    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.MatchMopStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.MatchMopStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.MatchMopStage process(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, dataPropKey, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, dataPropKey, self.visitId

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.MatchMopStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)



class StoreStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.StoreStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.StoreStage preprocess : universeSize %i' % self._universeSize
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.StoreStage preprocess(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.StoreStage preprocess() ', key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.StoreStage preprocess() ', key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.StoreStage preprocess() ', key, dataPropKey, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.StoreStage preprocess() ', key, dataPropKey, self.visitId

    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.StoreStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.StoreStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            inputParamPropertyPtrType = self.activeClipboard.get(key)
            print 'Python pipeline.StoreStage process(): stageId %i key %s' % (self.stageId, key)
            dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
            print dataPropertyKeyList

            if (key == "triggerAssociationEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.StoreStage process() ', self._rank, key, dataPropKey, self.visitId
                    elif (dataPropKey == "FOVRa"):
                        self.FOVRa = dpPtr.getValueDouble()
                        print 'Python pipeline.StoreStage process() ', self._rank, key, dataPropKey, self.FOVRa
                    elif (dataPropKey == "FOVDec"):
                        self.FOVDec = dpPtr.getValueDouble()
                        print 'Python pipeline.StoreStage process() ', self._rank, key, dataPropKey, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for dataPropKey in dataPropertyKeyList:
                    dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
                    if (dataPropKey == "visitId"):
                        self.visitId = dpPtr.getValueInt()
                        print 'Python pipeline.StoreStage process() ', self._rank, key, dataPropKey, self.visitId

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.StoreStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)

