#! /usr/bin/env python

"""
Test Application Stages for proto association pipeline
"""
import time

import lsst.pex.harness.stage as harnessStage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log, LogRec

import lsst.daf.base as dafBase
from lsst.daf.base import *

class SampleStageSerial(harnessStage.SerialProcessing):

    def setup(self): 
        self.runmode ="None"
        if self.policy.exists('RunMode'):
            self.runmode = self.policy.getString('RunMode')

    def preprocess(self, clipboard): 
        """
        Processing code for this Stage to be executed by the main Pipeline 
        prior to invoking Slice process 
        """
        # root =  Log.getDefaultLog()
        log = Log(self.log, "lsst.pexhexamples.pipeline.SampleStageSerial.preprocess")

        log.log(Log.INFO, 'Executing SampleStageSerial preprocess')

    def postprocess(self, clipboard): 
        """
        Processing code for this Stage to be executed by the main Pipeline 
        after the completion of Slice process 
        """
        # root =  Log.getDefaultLog()

        log = Log(self.log, "lsst.pexhexamples.pipeline.SampleStageSerial.postprocess")
        log.log(Log.INFO, 'Executing SampleStageSerial postprocess')

        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self.rank)
        lr << " stageId " + str(self.stageId) 
        lr << " universeSize " + str(self.universeSize) 
        lr << " RunMode from Policy " + self.runmode 
        lr << LogRec.endr

class SampleStageParallel(harnessStage.ParallelProcessing):

    def setup(self): 
        self.runmode ="None"
        if self.policy.exists('RunMode'):
            self.runmode = self.policy.getString('RunMode')

    def process(self, clipboard): 
        """
        Processing code for this Stage to be executed within a Slice 
        """

        # root =  Log.getDefaultLog()
        log = Log(self.log, "lsst.pexhexamples.pipeline.SampleStageParallel.process")

        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self.rank)
        lr << " stageId " + str(self.stageId) 
        lr << " runId " + str(self.runId) 
        lr << " universeSize " + str(self.universeSize) 
        lr << " RunMode from Policy " + self.runmode 
        lr << LogRec.endr

class ShutdownTestStageSerial(harnessStage.SerialProcessing):

    def setup(self): 
        self.runmode ="None"
        if self.policy.exists('RunMode'):
            self.runmode = self.policy.getString('RunMode')

    def preprocess(self, clipboard): 
        """
        Processing code for this Stage to be executed by the main Pipeline 
        prior to invoking Slice process 
        """
        log = Log(self.log, "lsst.pexhexamples.pipeline.ShutdownTestStageSerial.preprocess")

        log.log(Log.INFO, 'Executing ShutdownTestStageSerial preprocess')

    def postprocess(self, clipboard): 
        """
        Processing code for this Stage to be executed by the main Pipeline 
        after the completion of Slice process 
        """
        # root =  Log.getDefaultLog()

        log = Log(self.log, "lsst.pexhexamples.pipeline.ShutdownStageSerial.postprocess")
        log.log(Log.INFO, 'Executing ShutdownTestStageSerial postprocess')

        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self.rank)
        lr << " stageId " + str(self.stageId) 
        lr << " universeSize " + str(self.universeSize) 
        lr << " RunMode from Policy " + self.runmode 
        lr << LogRec.endr

class ShutdownTestStageParallel(harnessStage.ParallelProcessing):

    def setup(self): 
        self.runmode ="None"
        if self.policy.exists('RunMode'):
            self.runmode = self.policy.getString('RunMode')

    def process(self, clipboard): 
        """
        Processing code for this Stage to be executed within a Slice 
        """

        log = Log(self.log, "lsst.pexhexamples.pipeline.ShutdownTestStageParallel.process")

        i = 0 
        loopTime = 0.25
        while(i < 100):
            print "APP STAGE PROCESSING LOOP " + str(i) + " \n"; 
            time.sleep(loopTime)
            i=i+1


        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self.rank)
        lr << " stageId " + str(self.stageId) 
        lr << " runId " + str(self.runId) 
        lr << " universeSize " + str(self.universeSize) 
        lr << " RunMode from Policy " + self.runmode 
        lr << LogRec.endr

class LoadStageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard): 
        """
        Execute the needed preprocessing code for this Stage
        """
        print 'Python pipeline.LoadStage preprocess : stageId %i' % self.stageId
        print 'Python pipeline.LoadStage preprocess : universeSize %i' % self.universeSize

        keys = clipboard.getKeys()
        print 'Python pipeline.LoadStage preprocess : activeClipboard ',  clipboard

        for key in keys:
            propertySet = clipboard.get(key)
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

    def postprocess(self, clipboard): 
        """
        Execute the needed postprocessing code for this Stage
        """
        print 'Python pipeline.LoadStageSerial postprocess : stageId %d' % self.stageId

class LoadStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard): 
        """
        Execute the needed processing code for this Stage
        """
        print 'Python pipeline.LoadStage process : _rank %i stageId %i' % (self.rank, self.stageId)
        print 'Python pipeline.LoadStage process : _rank %i universeSize %i' % (self.rank, self.universeSize)
        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
            print 'Python pipeline.LoadStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.LoadStage process() ', self.rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.LoadStage process() ', self.rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.LoadStage process() ', self.rank, key, name, self.FOVDec


class MatchDiaSourceStageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard): 
        """
        Execute the needed preprocessing code for this Stage
        """
        print 'Python pipeline.MatchDiaSourceStage preprocess : stageId %i' % self.stageId
        print 'Python pipeline.MatchDiaSourceStage preprocess : universeSize %i' % self.universeSize

        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
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

    def postprocess(self, clipboard): 
        """
        Execute the needed postprocessing code for this Stage
        """
        print 'Python pipeline.MatchDiaSourceStageSerial postprocess : stageId %d' % self.stageId


class MatchDiaSourceStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard): 
        """
        Execute the needed processing code for this Stage
        """
        print 'Python pipeline.MatchDiaSourceStage process : _rank %i stageId %i' % (self.rank, self.stageId)
        print 'Python pipeline.MatchDiaSourceStage process : _rank %i universeSize %i' % (self.rank, self.universeSize)
        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
            print 'Python pipeline.MatchDiaSourceStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchDiaSourceStage process() ', self.rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.MatchDiaSourceStage process() ', self.rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.MatchDiaSourceStage process() ', self.rank, key, name, self.FOVDec



class MatchMopStageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard): 
        """
        Execute the needed preprocessing code for this Stage
        """
        print 'Python pipeline.MatchMopStage preprocess : stageId %i' % self.stageId
        print 'Python pipeline.MatchMopStage preprocess : universeSize %i' % self.universeSize
        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
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

    def postprocess(self, clipboard): 
        """
        Execute the needed postprocessing code for this Stage
        """
        print 'Python pipeline.MatchMopStageSerial postprocess : stageId %d' % self.stageId

class MatchMopStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard): 
        """
        Execute the needed processing code for this Stage
        """
        print 'Python pipeline.MatchMopStage process : _rank %i stageId %i' % (self.rank, self.stageId)
        print 'Python pipeline.MatchMopStage process : _rank %i universeSize %i' % (self.rank, self.universeSize)
        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
            print 'Python pipeline.MatchMopStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchMopStage process() ', self.rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.MatchMopStage process() ', self.rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.MatchMopStage process() ', self.rank, key, name, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.MatchMopStage process() ', self.rank, key, name, self.visitId

  

class StoreStageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard): 
        """
        Execute the needed preprocessing code for this Stage
        """
        print 'Python pipeline.StoreStage preprocess : stageId %i' % self.stageId
        print 'Python pipeline.StoreStage preprocess : universeSize %i' % self.universeSize
        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
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

    def postprocess(self, clipboard): 
        """
        Execute the needed postprocessing code for this Stage
        """
        print 'Python pipeline.StoreStageSerial postprocess : stageId %d' % self.stageId

class StoreStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard): 
        """
        Execute the needed processing code for this Stage
        """
        print 'Python pipeline.StoreStage process : _rank %i stageId %i' % (self.rank, self.stageId)
        print 'Python pipeline.StoreStage process : _rank %i universeSize %i' % (self.rank, self.universeSize)
        keys = clipboard.getKeys()

        for key in keys:
            propertySet = clipboard.get(key)
            print 'Python pipeline.StoreStage process(): stageId %i key %s' % (self.stageId, key)
            nameList = propertySet.names()

            if (key == "triggerAssociationEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.StoreStage process() ', self.rank, key, name, self.visitId
                    elif (name == "FOVRa"):
                        self.FOVRa = propertySet.getDouble(name)
                        print 'Python pipeline.StoreStage process() ', self.rank, key, name, self.FOVRa
                    elif (name == "FOVDec"):
                        self.FOVDec = propertySet.getDouble(name)
                        print 'Python pipeline.StoreStage process() ', self.rank, key, name, self.FOVDec

            if (key == "triggerMatchMopEvent"):
                for name in nameList:
                    if (name == "visitId"):
                        self.visitId = propertySet.getInt(name)
                        print 'Python pipeline.StoreStage process() ', self.rank, key, name, self.visitId


class AppFailureStageSerial(harnessStage.SerialProcessing):

    def preprocess(self, clipboard): 
        """
        Execute the needed preprocessing code for this Stage
        """
        print 'Python pipeline.AppFailureStageSerial preprocess : stageId %i' % self.stageId
        print 'Python pipeline.AppFailureStageSerial preprocess : universeSize %i' % self.universeSize

    def postprocess(self, clipboard): 
        """
        Execute the needed postprocessing code for this Stage
        """
        print 'Python pipeline.AppFailureStageSerial postprocess : stageId %d' % self.stageId

class AppFailureStageParallel(harnessStage.ParallelProcessing):

    def process(self, clipboard): 
        """
        Execute the needed processing code for this Stage
        """
        print 'Python pipeline.AppFailureStageParallel process : _rank %i stageId %i' % (self.rank, self.stageId)
        print 'Python pipeline.AppFailureStageParallel process : _rank %i universeSize %i' % (self.rank, self.universeSize)


