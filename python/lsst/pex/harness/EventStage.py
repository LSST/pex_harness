#! /usr/bin/env python

"""
Stage for publishing events, pulling contents off the Clipboard according to Policy. 
"""

from lsst.pex.harness.Stage import Stage
from lsst.pex.logging import Log, Debug
from lsst.pex.exceptions import LsstException

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.pex.logging as logging
import lsst.ctrl.events as events
import time

class EventStage(Stage):

    def __init__(self, stageId, stagePolicy):
        Stage.__init__(self, stageId, stagePolicy)
        self.log = Log(Log.getDefaultLog(), "harness.EventStage")

    def initialize(self, outQueue, inQueue): 
        """
        Initialize this stage.  This will make sure that an event broker
        is available.  The event broker should have been set by the harness
        framework as part of the stage creation and configuration.
        """
        Stage.initialize(self, outQueue, inQueue)
        if self.getEventBrokerHost() is None:
            raise LsstException("No event broker host configured " +
                                "for this EventStage")

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
        log = Debug(self.log, "preprocess")
        log.debug(3, "stageId=%i, universeSize=%i" %
                     (self.stageId, self._universeSize))

        self.activeClipboard = self.inputQueue.getNextDataset()

        if self._policy.exists('RunMode') and \
                (self._policy.getString('RunMode') == 'process' or \
                self._policy.getString('RunMode') == 'postprocess'):
            return

        self._publish();

        log.debug(3, "events published (stageId=%i)" % self.stageId)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
        log = Debug(self.log, "postprocess")
        log.debug(3, "stageId=%i, universeSize=%i" %
                     (self.stageId, self._universeSize))

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'postprocess': 
            self._publish()
            log.debug(3, "events published (stageId=%i)" % self.stageId)

        log.debug(3, "event processing done (stageId=%i)" % self.stageId)
        self.outputQueue.addDataset(self.activeClipboard)


    #------------------------------------------------------------------------
    def _publish(self, log=None):
        """
        Publish events if required
        """
        if log is None:
            log = self.log
        log = Debug(log, "publish")
        
        log.debug(4, "Looking for keysToPublish")
        if not self._policy.exists('keysToPublish'):
            log.log(Log.WARN,"Did not find keysToPublish in EventStage Policy")
            return

	log.debug(4, "Found keysToPublish")
        publKeyList = self._policy.getArray("keysToPublish") 
        if len(publKeyList) <= 0:
            log.log(Log.WARN, "Empty keysToPublish in EventStage Policy")
            return
	log.debug(4, "Got array: " + str(publKeyList))

        for key in publKeyList:
	    log.debug(4, "Got key %s" % key)
            (eventName, propertySetName) = self._parseKeysToPublish(key)
	    log.debug(4, "eventName=%s, propertySetName=%s" %
                         (eventName, propertySetName))

            psPtr = self.activeClipboard.get(propertySetName)
            log.debug(4, "Got propertySet %s" % psPtr.toString(False))
            oneEventTransmitter = \
                  events.EventTransmitter(self.getEventBrokerHost(), eventName)
            oneEventTransmitter.publish(psPtr)
            log.debug(4, 'Published event %s' % key)

    def _parseKeysToPublish(self, key):
        pos = key.find("=")
        if pos > 0:
            eventName = key[:pos]
            propertySetName = key[pos+1:]
        else:
            eventName = key
            propertySetName = key
        return (eventName, propertySetName)


        
