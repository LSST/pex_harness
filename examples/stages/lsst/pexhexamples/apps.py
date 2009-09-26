#! /usr/bin/env python

"""
Test Application Stages
"""

import lsst.pex.harness.stage as harnessStage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log, LogRec

import lsst.daf.base as dafBase
from lsst.daf.base import *

class App1StageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard):
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.App1Stage preprocess : stageId %i' % self.stageId
	print 'Python apps.App1Stage preprocess : universeSize %i' % self.universeSize

        propertySet = dafBase.PropertySet()

        propertySet.setInt("visitId", 1)
        propertySet.setDouble("FOVRa", 273.48066298343)
        propertySet.setDouble("FOVDec", -27.125)

        clipboard.put("outgoingKey", propertySet)


    def postprocess(self, clipboard):
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python apps.App1Stage postprocess : stageId %d' % self.stageId


class SyncSetupStageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard):
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.SyncSetupStage preprocess : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage preprocess : _rank %i' % self.rank

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.SyncSetupStage.preprocess")

        lr = LogRec(log, Log.INFO)
        lr << "Retrieving Clipboard"
        lr << LogRec.endr

        propertySet = dafBase.PropertySet()
        propertySet.setInt("redHerring", self.rank)
        clipboard.put("redKey", propertySet)

        lr = LogRec(log, Log.INFO)
        lr << "Posted data to Clipboard"
        lr << LogRec.endr

    def postprocess(self, clipboard):
        """
        Execute the needed postprocessing code for SyncSetupStage
        """
	print 'Python apps.SyncSetupStage postprocess : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStage postprocess : _rank %i' % self.rank

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.SyncSetupStage.postprocess")

        lr = LogRec(log, Log.INFO)
        lr << "In SyncSetupStage.postprocess"
        lr << LogRec.endr

class SyncSetupStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard):
        """
        Execute the needed processing code for this Stage
        """
	print 'Python apps.SyncSetupStageParallel process : stageId %i' % self.stageId
	print 'Python apps.SyncSetupStageParallel process : _rank %i' % self.rank

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.SyncSetupStageParallel.process")

        lr = LogRec(log, Log.INFO)
        lr << "Retrieving Clipboard"
        lr << LogRec.endr


        propertySet = dafBase.PropertySet()
        propertySet.setInt("sliceRank", self.rank)
        propertySet.setString("Level", "Debug")

        clipboard.put("rankKey", propertySet)

        clipboard.setShared("rankKey", True)

        lr = LogRec(log, Log.INFO)
        lr << "Posted data to be Shared on Clipboard"
        lr << LogRec.endr


class SyncTestStageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard):
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.SyncTestStageSerial preprocess : stageId %i' % self.stageId
	print 'Python apps.SyncTestStageSerial preprocess : _rank %i' % self.rank


    def postprocess(self, clipboard):
        """
        Execute the needed postprocessing code for SyncTestStage
        """
	print 'Python apps.SyncTestStageSerial postprocess : stageId %i' % self.stageId
	print 'Python apps.SyncTestStageSerial postprocess : _rank %i' % self.rank

class SyncTestStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard):
        """
        Execute the needed processing code for this Stage
        """
	print 'Python apps.SyncTestStageParallel process : stageId %i' % self.stageId
	print 'Python apps.SyncTestStageParallel process : _rank %i' % self.rank

        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
            print 'Python apps.SyncTestStageParallel process: stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()
            print 'Python apps.SyncTestStageParallel process: stageId %i key %s' % (self.stageId, key)

            for name in nameList:
                 print 'Python apps.SyncTestStageParallel process: Y ', self.rank, key, name


class ImageprocStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python ImageprocStageParallel process : _rank %i stageId %d' % (self.rank, self.stageId)
        key = "triggerImageprocEvent"
        propertySet = clipboard.get(key)

        nameList = propertySet.names()
        for name in nameList:
            print 'Python apps.ImageprocStageParallel process() ', 'name', name

        self.exposureId = propertySet.get("exposureid")
        print 'Python apps.ImageprocStageParallel process() ', key, "exposureid", self.exposureId
        self.ccdId = propertySet.get("ccdid")
        print 'Python apps.ImageprocStageParallel process() ', key, "ccdid", self.ccdId

        inputImage = clipboard.get('InputImage')

        print inputImage

        self.clipboard.put('OutputImage', inputImage)


class IPDPStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python IPDPStage process : _rank %i stageId %d' % (self.rank, self.stageId)

        inputImage = clipboard.get("inputImage") 

        nameList = inputImage.names()

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.pexhexamples.apps.IPDPStageParallel.process")

        for name in nameList:
            value = inputImage.getString(name) 
            print name, value
            lr = LogRec(log, Log.INFO)
            lr << name  
            lr << value 
            lr << LogRec.endr

