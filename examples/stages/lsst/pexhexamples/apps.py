#! /usr/bin/env python

"""
Test Application Stages
"""

from lsst.pex.harness.Stage import Stage

import lsst.pex.harness.Utils

import lsst.daf.base as dafBase
from lsst.daf.base import *

class App1Stage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.App1Stage preprocess : stageId %i' % self.stageId
	print 'Python apps.App1Stage preprocess : universeSize %i' % self._universeSize

        self.activeClipboard = self.inputQueue.getNextDataset()

        propertySet = dafBase.PropertySet()

        propertySet.setInt("visitId", 1)
        propertySet.setDouble("FOVRa", 273.48066298343)
        propertySet.setDouble("FOVDec", -27.125)

        self.activeClipboard.put("outgoingKey", propertySet)


    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python apps.App1Stage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)


class SyncSetupStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.SyncSetupStage preprocess : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage preprocess : _rank %i' % self._rank

        self.activeClipboard = self.inputQueue.getNextDataset()

        propertySet = dafBase.PropertySet()

        propertySet.setInt("redHerring", self._rank)

        self.activeClipboard.put("redKey", propertySet)

    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python apps.SyncSetupStage process : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage process : _rank %i' % self._rank

        self.activeClipboard = self.inputQueue.getNextDataset()

        propertySet = dafBase.PropertySet()

        propertySet.setInt("sliceRank", self._rank)
        propertySet.setString("Level", "Debug")

        self.activeClipboard.put("rankKey", propertySet)

        self.activeClipboard.setShared("rankKey", True)

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for SyncSetupStage
        """
	print 'Python apps.SyncSetupStage postprocess : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage postprocess : _rank %i' % self._rank
        self.outputQueue.addDataset(self.activeClipboard)

class SyncTestStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.SyncTestStage preprocess : stageId %i' % self.stageId
	print 'Python apps.SyncTestStage preprocess : _rank %i' % self._rank

        self.activeClipboard = self.inputQueue.getNextDataset()


    #------------------------------------------------------------------------
    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python apps.SyncTestStage process AAAA : stageId %i' % self.stageId
	print 'Python apps.SyncTestStage process BBBB : _rank %i' % self._rank

        self.activeClipboard = self.inputQueue.getNextDataset()

        keys = self.activeClipboard.getKeys()

        for key in keys:
            propertySet = self.activeClipboard.get(key)
            print 'Python apps.SyncTestStage process: XXXX stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()
            print 'Python apps.SyncTestStage process: YYYY stageId %i key %s' % (self.stageId, key)

            for name in nameList:
                 # iValue = propertySet.getInt(name)
                 ## iValue = propertySet.get()
                 # print 'Python apps.SyncTestStage process: Y ', self._rank, key, name, iValue
                 print 'Python apps.SyncTestStage process: Y ', self._rank, key, name


        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for SyncTestStage
        """
	print 'Python apps.SyncTestStage postprocess : stageId %i' % self.stageId
	print 'Python apps.SyncTestStage postprocess : _rank %i' % self._rank
        self.outputQueue.addDataset(self.activeClipboard)

