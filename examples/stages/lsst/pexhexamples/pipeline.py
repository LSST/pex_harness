#! /usr/bin/env python

"""
Test Application Stages for proto association pipeline
"""

from lsst.pex.harness.Stage import Stage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log, LogRec

import lsst.daf.base as dafBase
from lsst.daf.base import *

class SampleStage(Stage):

    def process(self): 
        """
        Processing code for this Stage to be executed within a Slice 
        """

        self.activeClipboard = self.inputQueue.getNextDataset()

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.pipeline.SampleStage.process")

        value ="None"
        if self._policy.exists('RunMode'):
            value = self._policy.getString('RunMode')

        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self._rank)
        lr << " stageId " + str(self.stageId) 
        lr << " universeSize " + str(self._universeSize) 
        lr << " RunMode from Policy " + value 
        lr << LogRec.endr

        self.outputQueue.addDataset(self.activeClipboard)

class MyLookupStage(Stage):

    def preprocess(self): 
        """
        Execute the preprocessing code for this Stage
        """

        self.activeClipboard = self.inputQueue.getNextDataset()

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.pipeline.MyLookupStage.preprocess")

        lookup = self.getLookup()

        print lookup 

        dirWork = lookup.get('work')
        dirInput = lookup.get('input')
        dirOutput = lookup.get('output')
        dirUpdate = lookup.get('update')
        dirScratch = lookup.get('scratch')

        lr = LogRec(log, Log.INFO)
        lr << " dirWork " + dirWork
        lr << " dirInput " + dirInput
        lr << " dirOutput " + dirOutput
        lr << " dirUpdate " + dirUpdate
        lr << " dirScratch " + dirScratch
        lr << LogRec.endr

    def process(self): 
        """
        Processing code for this Stage to be executed within a Slice 
        """

        self.activeClipboard = self.inputQueue.getNextDataset()

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.pipeline.MyLookupStage.process")

        lookup = self.getLookup()

        print lookup 

        dirWork = lookup.get('work')
        dirInput = lookup.get('input')
        dirOutput = lookup.get('output')
        dirUpdate = lookup.get('update')
        dirScratch = lookup.get('scratch')

        lr = LogRec(log, Log.INFO)
        lr << " dirWork " + dirWork
        lr << " dirInput " + dirInput
        lr << " dirOutput " + dirOutput
        lr << " dirUpdate " + dirUpdate
        lr << " dirScratch " + dirScratch
        lr << LogRec.endr

        self.outputQueue.addDataset(self.activeClipboard)

    def postprocess(self): 
        """
        Execute the  postprocessing code for this Stage
        """
        self.outputQueue.addDataset(self.activeClipboard)

class LoadStage(Stage):

    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.LoadStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.LoadStage preprocess : universeSize %i' % self._universeSize

        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
	print 'Python pipeline.LoadStage preprocess : activeClipboard ',  self.activeClipboard

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.LoadStage preprocess(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.LoadStage preprocess() ', key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.LoadStage preprocess() ', key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.LoadStage preprocess() ', key, name, self.FOVDec


    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.LoadStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.LoadStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.LoadStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.LoadStage process() ', self._rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.LoadStage process() ', self._rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.LoadStage process() ', self._rank, key, name, self.FOVDec

        self.outputQueue.addDataset(self.activeClipboard)

    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.LoadStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)



class MatchDiaSourceStage(Stage):

    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.MatchDiaSourceStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.MatchDiaSourceStage preprocess : universeSize %i' % self._universeSize

        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.MatchDiaSourceStage preprocess(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchDiaSourceStage preprocess() ', key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.MatchDiaSourceStage preprocess() ', key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.MatchDiaSourceStage preprocess() ', key, name, self.FOVDec

    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.MatchDiaSourceStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.MatchDiaSourceStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.MatchDiaSourceStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchDiaSourceStage process() ', self._rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.MatchDiaSourceStage process() ', self._rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.MatchDiaSourceStage process() ', self._rank, key, name, self.FOVDec

        self.outputQueue.addDataset(self.activeClipboard)


    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.MatchDiaSourceStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)



class MatchMopStage(Stage):

    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.MatchMopStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.MatchMopStage preprocess : universeSize %i' % self._universeSize
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.MatchMopStage preprocess(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchMopStage preprocess() ', key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.MatchMopStage preprocess() ', key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.MatchMopStage preprocess() ', key, name, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchMopStage preprocess() ', key, name, self.visitId

    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.MatchMopStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.MatchMopStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.MatchMopStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, name, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchMopStage process() ', self._rank, key, name, self.visitId

        self.outputQueue.addDataset(self.activeClipboard)

    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.MatchMopStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)



class StoreStage(Stage):

    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python pipeline.StoreStage preprocess : stageId %i' % self.stageId
	print 'Python pipeline.StoreStage preprocess : universeSize %i' % self._universeSize
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.StoreStage preprocess(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.StoreStage preprocess() ', key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.StoreStage preprocess() ', key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.StoreStage preprocess() ', key, name, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.StoreStage preprocess() ', key, name, self.visitId

    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python pipeline.StoreStage process : _rank %i stageId %i' % (self._rank, self.stageId)
	print 'Python pipeline.StoreStage process : _rank %i universeSize %i' % (self._rank, self._universeSize)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python pipeline.StoreStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.StoreStage process() ', self._rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.StoreStage process() ', self._rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.StoreStage process() ', self._rank, key, name, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.StoreStage process() ', self._rank, key, name, self.visitId

        self.outputQueue.addDataset(self.activeClipboard)

    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python pipeline.StoreStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)

