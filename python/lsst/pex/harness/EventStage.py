#! /usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#


"""
Stage for publishing events, pulling contents off the Clipboard according to Policy. 
"""

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log, Debug
from lsst.pex.exceptions import LsstException

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.pex.logging as logging
import lsst.ctrl.events as events
import time

class EventStageSerial(harnessStage.SerialProcessing):

    # def __init__(self, stageId, stagePolicy):
    #     Stage.__init__(self, stageId, stagePolicy)
    #     self.log = Log(Log.getDefaultLog(), "harness.EventStage")

    def setup(self):
        self.runmode ="None"
        if self.policy.exists('RunMode'):
            self.runmode = self.policy.getString('RunMode')
        if self.getEventBrokerHost() is None:
            raise LsstException("No event broker host configured " +
                                "for this EventStage")

    # def initialize(self, outQueue, inQueue): 
    #     """
    #     Initialize this stage.  This will make sure that an event broker
    #     is available.  The event broker should have been set by the harness
    #     framework as part of the stage creation and configuration.
    #     """
    #     Stage.initialize(self, outQueue, inQueue)

    def preprocess(self, clipboard):
        """
        Execute the needed preprocessing code for this Stage
        """
        log = Debug(self.log, "preprocess")
        log.debug(3, "stageId=%i, universeSize=%i" %
                     (self.stageId, self.universeSize))

        if (self.runmode == 'process' or self.runmode == 'postprocess'):
            return

        self._publish(clipboard);
        log.debug(3, "events published (stageId=%i)" % self.stageId)

    #------------------------------------------------------------------------
    def postprocess(self, clipboard):
        """
        Execute the needed postprocessing code for this Stage
        """
        log = Debug(self.log, "postprocess")
        log.debug(3, "stageId=%i, universeSize=%i" %
                     (self.stageId, self.universeSize))

        if (self.runmode == 'postprocess'):
            self._publish(clipboard)
            log.debug(3, "events published (stageId=%i)" % self.stageId)

        log.debug(3, "event processing done (stageId=%i)" % self.stageId)


    #------------------------------------------------------------------------
    def _publish(self, clipboard, log=None):
        """
        Publish events if required
        """
        if log is None:
            log = self.log
        log = Debug(log, "publish")
        
        log.debug(4, "Looking for keysToPublish")
        if not self.policy.exists('keysToPublish'):
            log.log(Log.WARN,"Did not find keysToPublish in EventStage Policy")
            return

	log.debug(4, "Found keysToPublish")
        publKeyList = self.policy.getArray("keysToPublish") 
        if len(publKeyList) <= 0:
            log.log(Log.WARN, "Empty keysToPublish in EventStage Policy")
            return
	log.debug(4, "Got array: " + str(publKeyList))

        for key in publKeyList:
	    log.debug(4, "Got key %s" % key)
            (eventName, propertySetName) = self._parseKeysToPublish(key)
	    log.debug(4, "eventName=%s, propertySetName=%s" %
                         (eventName, propertySetName))

            psPtr = clipboard.get(propertySetName)
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


        
