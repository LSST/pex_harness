#! /usr/bin/env python

"""
Stage for publishing events, pulling contents off the Clipboard according to Policy. 
"""

from lsst.pex.harness.Stage import Stage
from lsst.pex.logging import Log

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.pex.logging as logging
import lsst.ctrl.events as events
import time

class EventStage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	logging.Trace("EventStage", 3, 'Python lsst.pex.harness.EventStage preprocess : stageId %i' % self.stageId)
	logging.Trace("EventStage", 3, 'Python lsst.pex.harness.EventStage preprocess : universeSize %i' % self._universeSize)

        self.activeClipboard = self.inputQueue.getNextDataset()

        if self._policy.exists('RunMode') and \
                (self._policy.getString('RunMode') == 'process' or \
                self._policy.getString('RunMode') == 'postprocess'):
            return

        self._publish();

	logging.Trace("EventStage", 3, 'Python lsst.pex.harness.EventStage preprocess : (after publish) stageId %d' % self.stageId)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	logging.Trace("EventStage", 3, 'Python lsst.pex.harness.EventStage postprocess : stageId %i' % self.stageId)
	logging.Trace("EventStage", 3, 'Python lsst.pex.harness.EventStage postprocess : universeSize %i' % self._universeSize)

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'postprocess': 
            self._publish()

	logging.Trace("EventStage", 3, 'Python lsst.pex.harness.EventStage postprocess : stageId %d' % self.stageId)
        self.outputQueue.addDataset(self.activeClipboard)


    #------------------------------------------------------------------------
    def _publish(self):
        """
        Publish events if required
        """
        mylog = Log(Log.getDefaultLog(), "EventStage")
	logging.Trace("EventStage", 4, "Looking for keysToPublish")
        if not self._policy.exists('keysToPublish'):
            mylog.log(Log.WARN, "Did not find keysToPublish in EventStage Policy")
            return

	logging.Trace("EventStage", 4, "Found keysToPublish")
        publKeyList = self._policy.getArray("keysToPublish") 
        if len(publKeyList) <= 0:
            mylog.log(Log.WARN, "Empty keysToPublish in EventStage Policy")
            return
	logging.Trace("EventStage", 4, "Got array: " + str(publKeyList))

        for key in publKeyList:
	    logging.Trace("EventStage", 4, "Got key %s" % key)
            pos = key.find("=")
            if pos > 0:
                eventName = key[:pos]
                propertySetName = key[pos+1:]
            else:
                eventName = key
                propertySetName = key
	    logging.Trace("EventStage", 4, "eventName=%s, propertySetName=%s" % (eventName, propertySetName))

            psPtr = self.activeClipboard.get(propertySetName)
            logging.Trace("EventStage", 4, "Got propertySet %s" % psPtr.toString(False))
            oneEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", eventName)
            oneEventTransmitter.publish(psPtr)
            logging.Trace("EventStage", 4, 'Python pipeline.EventStage published event %s' % key)
