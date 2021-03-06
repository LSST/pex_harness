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


import thread
import threading
import os, sys, re, traceback, time
from threading import Thread
from threading import Event as PyEvent

from lsst.pex.logging import Log, BlockTimingLog

import lsst.ctrl.events as events

"""
ShutdownThread class manages a separate Python thread that runs
alongside a Pipeline for the purpose of 1) listening for Shutdown 
events and 2) taking appropriate action for processing shutdown 
in response. The levels that designate the manner in which the shutdown
proceeds are :
 * Shutdown at level 1 : stop immediately by killing process (ugly)
 * Shutdown at level 2 : exit in a clean manner (Pipeline and Slices) at a synchronization point 
 * Shutdown at level 3 : exit in a clean manner (Pipeline and Slices) at the end of a Stage
 * Shutdown at level 4 : exit in a clean manner (Pipeline and Slices) at the end of a Visit
"""


class ShutdownThread(threading.Thread):

    def __init__ (self, pipeline):
        Thread.__init__(self)
        self.pipeline = pipeline
        self._stop = PyEvent()
        self.pid = os.getpid()

        # log message levels
        self.TRACE = BlockTimingLog.INSTRUM+2
        self.VERB1 = self.TRACE
        self.VERB2 = self.VERB1 - 1
        self.VERB3 = self.VERB2 - 1
        self.logthresh = self.pipeline.getLogThreshold()
        
    def getPid (self):
        return self.pid

    def setStop (self):
        self._stop.set()

    def exit (self):
        thread.exit()

    def run(self):

        runId = self.pipeline.getRun()
        shutdownTopic = self.pipeline.getShutdownTopic()
        eventBrokerHost = self.pipeline.getEventBrokerHost()

        # eventsSystem = events.EventSystem.getDefaultEventSystem()
        # eventsSystem.createReceiver(eventBrokerHost, shutdownTopic)
        clause = "RUNID = '" + runId + "'"
        recv = events.EventReceiver(eventBrokerHost, shutdownTopic, clause)
        # works 
        # recv = events.EventReceiver(eventBrokerHost, shutdownTopic)

        sleepTimeout = 2.0
        transTimeout = 900

        shutdownEvent = None
        shutdownPropertySetPtr = None

        # while(shutdownPropertySetPtr == None):
        while(shutdownEvent == None):
            if(self.logthresh == Log.DEBUG):
                print "ShutdownThread Looping : checking for Shutdown event ... \n" 
                print "ShutdownThread Looping : " + clause 

            time.sleep(sleepTimeout)
            # shutdownPropertySetPtr = eventsSystem.receive(shutdownTopic, transTimeout)
            shutdownEvent  = recv.receiveEvent(transTimeout)
            if(shutdownEvent == None):
                pass
            else:
                # if (shutdownPropertySetPtr != None):
                # if (shutdownEvent != None):
                shutdownPropertySetPtr = shutdownEvent.getCustomPropertySet()
                self.level = shutdownPropertySetPtr.getInt("level")

            if(self._stop.isSet()):
                if(self.logthresh == self.VERB3):
                    print "ShutdownThread exiting from loop : Stop from Pipeline  \n" 
                sys.exit()

        self.pipeline.setExitLevel(self.level)

        # Shutdown at level 1 : stop immediately by killing process (ugly)
        if (self.level == 1):
            self.pipeline.exit()

        # Shutdown at level 2 : exit in a clean manner (Pipeline and Slices) 
        # at a synchronization point 
        if (self.level == 2):
            self.pipeline.setStop()

        # Shutdown at level 3 : exit in a clean manner (Pipeline and Slices)
        # at the end of a Stage
        if (self.level == 3):
            self.pipeline.setStop()

        # Shutdown at level 4 : exit in a clean manner (Pipeline and Slices)
        # at the end of a Visit
        if (self.level == 4):
            self.pipeline.setStop()

        # Technique for handling no-more-date scenario still in discussion
        # if (self.level == 5):
        #    print "SHUTDOWNTHREAD STOP PIPELINE LEVEL 5 \n" 
        #     self.pipeline.setEventTimeout(2000)
        #    self.pipeline.stop()


