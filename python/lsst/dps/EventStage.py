#! /usr/bin/env python

"""
Stage for publishing events, pulling contents off the Clipboard according to Policy. 
"""

from lsst.dps.Stage import Stage
from lsst.mwi.logging import Log

import lsst.mwi.data as datap
import lsst.mwi.utils as utils
import lsst.events as events
import time

class EventStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	utils.Trace("EventStage", 3, 'Python lsst.dps.EventStage preprocess : stageId %i' % self.stageId)
	utils.Trace("EventStage", 3, 'Python lsst.dps.EventStage preprocess : universeSize %i' % self._universeSize)

        self.activeClipboard = self.inputQueue.getNextDataset()

        if self._policy.exists('RunMode') and \
                (self._policy.getString('RunMode') == 'process' or \
                self._policy.getString('RunMode') == 'postprocess'):
            return

        self._publish();

	utils.Trace("EventStage", 3, 'Python lsst.dps.EventStage preprocess : (after publish) stageId %d' % self.stageId)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	utils.Trace("EventStage", 3, 'Python lsst.dps.EventStage postprocess : stageId %i' % self.stageId)
	utils.Trace("EventStage", 3, 'Python lsst.dps.EventStage postprocess : universeSize %i' % self._universeSize)

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'postprocess': 
            self._publish()

	utils.Trace("EventStage", 3, 'Python lsst.dps.EventStage postprocess : stageId %d' % self.stageId)
        self.outputQueue.addDataset(self.activeClipboard)


    #------------------------------------------------------------------------
    def _publish(self):
        """
        Publish events if required
        """
        mylog = Log(Log.getDefaultLog(), "EventStage")
	utils.Trace("EventStage", 4, "Looking for keysToPublish")
        if not self._policy.exists('keysToPublish'):
            mylog.log(Log.WARN, "Did not find keysToPublish in EventStage Policy")
            return

	utils.Trace("EventStage", 4, "Found keysToPublish")
        publKeyList = self._policy.getArray("keysToPublish") 
        if len(publKeyList) <= 0:
            mylog.log(Log.WARN, "Empty keysToPublish in EventStage Policy")
            return
	utils.Trace("EventStage", 4, "Got array: " + str(publKeyList))

        for key in publKeyList:
	    utils.Trace("EventStage", 4, "Got key %s" % key)
            pos = key.find("=")
            if pos > 0:
                eventName = key[:pos]
                dataPropertyName = key[pos+1:]
            else:
                eventName = key
                dataPropertyName = key
	    utils.Trace("EventStage", 4, "eventName=%s, dataPropertyName=%s" % (eventName, dataPropertyName))

            if not self.activeClipboard.has_key(dataPropertyName):
                mylog.log(Log.WARN, "DataProperty %s missing" % dataPropertyName)
                continue
            dpPtrType = self.activeClipboard.get(dataPropertyName)
            utils.Trace("EventStage", 4, "Got dataProperty %s" % dpPtrType.toString())
            oneEventTransmitter = events.EventTransmitter(\
                                         "lsst8.ncsa.uiuc.edu", eventName)
            oneEventTransmitter.publish("eventtype", dpPtrType)
            utils.Trace("EventStage", 4, 'Python pipeline.EventStage published event %s' % key)
