
import thread
import threading
import os, sys, re, traceback, time
from threading import Thread
from threading import Event as PyEvent

from lsst.pex.logging import Log 
from lsst.pex.harness.harnessLib import TracingLog

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
        self.TRACE = TracingLog.TRACE
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

        eventsSystem = events.EventSystem.getDefaultEventSystem()
        eventsSystem.createReceiver(eventBrokerHost, shutdownTopic)

        sleepTimeout = 2.0
        transTimeout = 900

        shutdownPropertySetPtr = None
        while(shutdownPropertySetPtr == None):
            if(self.logthresh == self.VERB3):
                print "ShutdownThread Looping : checking for Shutdown event ... \n" 

            time.sleep(sleepTimeout)
            shutdownPropertySetPtr = eventsSystem.receive(shutdownTopic, transTimeout)
    
            if (shutdownPropertySetPtr != None):
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


