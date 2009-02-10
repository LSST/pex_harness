#! /usr/bin/env python

"""
Test Application Stages
"""

from lsst.pex.harness.Stage import Stage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log, LogRec

import lsst.daf.base as dafBase
from lsst.daf.base import *

class App1Stage(Stage):

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


    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python apps.App1Stage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)


class SyncSetupStage(Stage):

    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.SyncSetupStage preprocess : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage preprocess : _rank %i' % self._rank

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.SyncSetupStage.preprocess")

        lr = LogRec(log, Log.INFO)
        lr << "Retrieving Clipboard"
        lr << LogRec.endr

        self.activeClipboard = self.inputQueue.getNextDataset()

        propertySet = dafBase.PropertySet()

        propertySet.setInt("redHerring", self._rank)

        self.activeClipboard.put("redKey", propertySet)

        lr = LogRec(log, Log.INFO)
        lr << "Posted data to Clipboard"
        lr << LogRec.endr

    def process(self): 
        """
        Execute the needed processing code for this Stage
        """
	print 'Python apps.SyncSetupStage process : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage process : _rank %i' % self._rank

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.SyncSetupStage.process")

        lr = LogRec(log, Log.INFO)
        lr << "Retrieving Clipboard"
        lr << LogRec.endr

        self.activeClipboard = self.inputQueue.getNextDataset()

        propertySet = dafBase.PropertySet()

        propertySet.setInt("sliceRank", self._rank)
        propertySet.setString("Level", "Debug")

        self.activeClipboard.put("rankKey", propertySet)

        self.activeClipboard.setShared("rankKey", True)

        lr = LogRec(log, Log.INFO)
        lr << "Posted data to be Shared on Clipboard"
        lr << LogRec.endr

        self.outputQueue.addDataset(self.activeClipboard)

    def postprocess(self): 
        """
        Execute the needed postprocessing code for SyncSetupStage
        """
	print 'Python apps.SyncSetupStage postprocess : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage postprocess : _rank %i' % self._rank

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.SyncSetupStage.postprocess")

        self.outputQueue.addDataset(self.activeClipboard)

        lr = LogRec(log, Log.INFO)
        lr << "Posted Clipboard to outputQueue"
        lr << LogRec.endr

class SyncTestStage(Stage):

    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.SyncTestStage preprocess : stageId %i' % self.stageId
	print 'Python apps.SyncTestStage preprocess : _rank %i' % self._rank

        self.activeClipboard = self.inputQueue.getNextDataset()


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

    def postprocess(self): 
        """
        Execute the needed postprocessing code for SyncTestStage
        """
	print 'Python apps.SyncTestStage postprocess : stageId %i' % self.stageId
	print 'Python apps.SyncTestStage postprocess : _rank %i' % self._rank
        self.outputQueue.addDataset(self.activeClipboard)


class ImageprocStage(Stage):

    def process(self):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python ImageprocStage process : _rank %i stageId %d' % (self._rank, self.stageId)
        self.activeClipboard = self.inputQueue.getNextDataset()

        key = "triggerImageprocEvent"
        propertySet = self.activeClipboard.get(key)

        nameList = propertySet.names()
        for name in nameList:
            print 'Python apps.ImageprocStage process() ', 'name', name

        self.exposureId = propertySet.get("exposureid")
        print 'Python apps.ImageprocStage process() ', key, "exposureid", self.exposureId
        self.ccdId = propertySet.get("ccdid")
        print 'Python apps.ImageprocStage process() ', key, "ccdid", self.ccdId

        inputImage = self.activeClipboard.get('InputImage')

        print inputImage
        # print "inputImage cols=%r, rows=%r" % (inputImage.getCols(), inputImage.getRows())
        # print 'Python ImageprocStage process(): _rank %i stageId %i value %s' % (self._rank, self.stageId, inputImage)

        self.activeClipboard.put('OutputImage', inputImage)

        self.outputQueue.addDataset(self.activeClipboard)


class IPDPStage(Stage):

    def process(self):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python IPDPStage process : _rank %i stageId %d' % (self._rank, self.stageId)

        self.activeClipboard = self.inputQueue.getNextDataset()

        inputImage = self.activeClipboard.get("inputImage") 

        nameList = inputImage.names()

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.IPDPStage.process")

        for name in nameList:
            value = inputImage.getString(name) 
            print name, value
            lr = LogRec(log, Log.INFO)
            lr << name  
            lr << value 
            lr << LogRec.endr

        self.outputQueue.addDataset(self.activeClipboard)

