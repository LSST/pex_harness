#! /usr/bin/env python

"""
Stage for publishing events, pulling contents off the Clipboard according to Policy. 
"""

from lsst.dps.Stage import Stage

import lsst.mwi.data as datap
import lsst.events as events
import time

class EventStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python lsst.dps.EventStage preprocess : stageId %i' % self.stageId
	print 'Python lsst.dps.EventStage preprocess : universeSize %i' % self._universeSize

        self.activeClipboard = self.inputQueue.getNextDataset()

        if self._policy.exists('RunMode') and \
                (self._policy.getString('RunMode') == 'process' or \
                self._policy.getString('RunMode') == 'postprocess'):
            return

        if self._policy.exists('keysToPublish'):
            publKeyList = self._policy.getArray("keysToPublish") 
            for key in publKeyList:
                dpPtrType = self.activeClipboard.get(key)
                # if dpPtrType is suitable  
                oneEventTransmitter = events.EventTransmitter(\
                                             "lsst8.ncsa.uiuc.edu", key)
                oneEventTransmitter.publish("eventtype", dpPtrType)
                print 'Python pipeline.EventStage preprocess() published event ', key


    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python lsst.dps.EventStage postprocess : stageId %i' % self.stageId
	print 'Python lsst.dps.EventStage postprocess : universeSize %i' % self._universeSize

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'postprocess': 
            if self._policy.exists('keysToPublish'):
                publKeyList = self._policy.getArray("keysToPublish") 
                for key in publKeyList:
                    dpPtrType = self.activeClipboard.get(key)
                    # if dpPtrType is suitable  
                    oneEventTransmitter = events.EventTransmitter(\
                                             "lsst8.ncsa.uiuc.edu", key)
                    oneEventTransmitter.publish("eventtype", dpPtrType)
                    print 'Python pipeline.EventStage postprocess() published event ', key

	print 'Python lsst.dps.EventStage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)




