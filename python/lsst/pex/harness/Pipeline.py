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


from lsst.pex.harness.Queue import Queue
from lsst.pex.harness.stage import StageProcessing
from lsst.pex.harness.stage import NoOpSerialProcessing
from lsst.pex.harness.Clipboard import Clipboard
from lsst.pex.harness.Directories import Directories
from lsst.pex.logging import Log, LogRec, cout, Prop
from lsst.pex.logging import BlockTimingLog
from lsst.pex.harness import harnessLib as logutils

from lsst.pex.harness.SliceThread import SliceThread
from lsst.pex.harness.ShutdownThread import ShutdownThread

import threading 

from threading import Thread
from threading import Event as PyEvent

import lsst.pex.policy as policy

import lsst.pex.exceptions
from lsst.pex.exceptions import *

import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.daf.persistence as dafPersist
from lsst.daf.persistence import *


import lsst.ctrl.events as events

import os, sys, re, traceback, time

"""
Pipeline class manages the operation of a multi-stage parallel pipeline.
The Pipeline is configured by reading a Policy file.   
Pipeline has a __main__ portion as it serves as the main executable program 
('glue layer') for running a Pipeline. The Pipeline spawns Slice workers 
for parallel computations. 
"""


class Pipeline(object):
    '''Python Pipeline class implementation. Contains main pipeline workflow'''

    def __init__(self, runId='-1', pipelinePolicyName=None, name="unnamed", workerId=None):
        """
        Initialize the Pipeline: create empty Queue and Stage lists;
        import the C++ Pipeline instance; initialize the MPI environment
        """
        # log message levels
        self.TRACE = BlockTimingLog.INSTRUM+2
        self.VERB1 = self.TRACE
        self.VERB2 = self.VERB1 - 1
        self.VERB3 = self.VERB2 - 1
        self.log = None
        self.logthresh = None
        self._logdir = ""

        self._pipelineName = name
        
        self.queueList = []
        self.stageList = []
        self.stageClassList = []
        self.stagePolicyList = []
        self.eventTopicList = []
        self.shareDataList = []
        self.clipboardList = []
        self.executionMode = 0
        self._runId = runId
        self.pipelinePolicyName = pipelinePolicyName
        if workerId is not None:
            self.workerId = workerId
        else:
            self.workerId = "-1"

        self.cppLogUtils = logutils.LogUtils()
        self._stop = PyEvent()

    def setStop (self):
        self._stop.set()

    def exit (self):

        if self.log is not None:
            self.log.log(self.VERB1, 'Killing Pipeline process immediately: shutdown level 1')

        thisPid = os.getpid()
        os.popen("kill -9 "+str(thisPid))
 
        sys.exit()

    def __del__(self):
        """
        Delete the Pipeline object: clean up
        """
        if self.log is not None:
            self.log.log(self.VERB1, 'Python Pipeline being deleted')

    def initializeLogger(self):
        """
        Initialize the Logger after opening policy file 
        """
        if(self.pipelinePolicyName == None):
            self.pipelinePolicyName = "pipeline_policy.paf"
        dictName = "pipeline_dict.paf"
        topPolicy = policy.Policy.createPolicy(self.pipelinePolicyName)

        if (topPolicy.exists('execute')):
            self.executePolicy = topPolicy.get('execute')
        else:
            self.executePolicy = policy.Policy.createPolicy(self.pipelinePolicyName)

        # Check for eventBrokerHost 
        if (self.executePolicy.exists('eventBrokerHost')):
            self.eventBrokerHost = self.executePolicy.getString('eventBrokerHost')
        else:
            self.eventBrokerHost = "lsst8.ncsa.uiuc.edu"   # default value
        self.cppLogUtils.setEventBrokerHost(self.eventBrokerHost);

        doLogFile = self.executePolicy.getBool('localLogMode')
        self.cppLogUtils.initializeLogger(doLogFile, self._pipelineName,
                                          self._runId, self._logdir,
                                          self.workerId,
                                          BlockTimingLog.LINUXUDATA)

        # The log for use in the Python Pipeline
        self.log = self.cppLogUtils.getLogger()

        if (self.executePolicy.exists('logThreshold')):
            self.logthresh = self.executePolicy.get('logThreshold')
        else:
            if(self.logthresh == None):
                self.logthresh = self.TRACE
        self.setLogThreshold(self.logthresh)

        # self.log.addDestination(cout, Log.DEBUG);

    def configurePipeline(self):
        """
        Configure the Pipeline by reading a Policy File
        """

        if (self.executePolicy.exists('nSlices')):
            self.nSlices = self.executePolicy.getInt('nSlices')
        else:
            self.nSlices = 0   # default value
        self.universeSize = self.nSlices + 1; 

        if (self.executePolicy.exists('barrierDelay')):
            self.barrierDelay = self.executePolicy.getDouble('barrierDelay')
        else:
            self.barrierDelay = 0.000001   # default value

        # do some juggling to capture the actual stage policy names.  We'll
        # use these to assign some logical names to the stages for logging
        # purposes.  Note, however, that it is only convention that the
        # stage policies will be specified as separate files; thus, we need
        # a fallback.  
        stgcfg = self.executePolicy.getArray("appStage")
        self.stageNames = []
        for subpol in stgcfg:
            stageName = subpol.get("name") 
            self.stageNames.append(stageName)

        self.executePolicy.loadPolicyFiles()


        # Obtain the working directory space locators
        psLookup = lsst.daf.base.PropertySet()
        if (self.executePolicy.exists('dir')):
            dirPolicy = self.executePolicy.get('dir')
            shortName = None
            if (dirPolicy.exists('shortName')):
                shortName = dirPolicy.get('shortName')
            if shortName == None:
                shortName = self.pipelinePolicyName.split('.')[0]
            dirs = Directories(dirPolicy, shortName, self._runId)
            psLookup = dirs.getDirs()
        if (self.executePolicy.exists('database.url')):
            psLookup.set('dbUrl', self.executePolicy.get('database.url'))


        log = Log(self.log, "configurePipeline")
        log.log(Log.INFO,
                "Logging messages using threshold=%i" % log.getThreshold())
        LogRec(log, self.VERB1) << "Configuring pipeline"        \
                                << Prop("universeSize", self.universeSize) \
                                << Prop("runID", self._runId) \
                                << Prop("rank", -1)   \
                                << Prop("workerId", self.workerId) \
                                << LogRec.endr;
        

        # Configure persistence logical location map with values for directory 
        # work space locators
        dafPersist.LogicalLocation.setLocationMap(psLookup)

        log.log(self.VERB2, "eventBrokerHost %s " % self.eventBrokerHost)
        log.log(self.VERB2, "barrierDelay %s " % self.barrierDelay)

        # Check for eventTimeout
        if (self.executePolicy.exists('eventTimeout')):
            self.eventTimeout = self.executePolicy.getInt('eventTimeout')
        else:
            self.eventTimeout = 10000000   # default value is 10 000 000

        # Process Application Stages
        fullStageList = self.executePolicy.getArray("appStage")
        self.nStages = len(fullStageList)
        log.log(self.VERB2, "Found %d stages" % len(fullStageList))

        fullStageNameList = [ ]
        self.stagePolicyList = [ ]
        for stagei in xrange(self.nStages):
            fullStagePolicy = fullStageList[stagei]
            if (fullStagePolicy.exists('serialClass')):
                serialName = fullStagePolicy.getString('serialClass')
                stagePolicy = fullStagePolicy.get('stagePolicy') 
            else:
                serialName = "lsst.pex.harness.stage.NoOpSerialProcessing"
                stagePolicy = None

            fullStageNameList.append(serialName)
            self.stagePolicyList.append(stagePolicy)

            if self.stageNames[stagei] is None:
                self.stageNames[stagei] = fullStageNameList[-1].split('.')[-1]
            log.log(self.VERB3,
                    "Stage %d: %s: %s" % (stagei+1, self.stageNames[stagei],
                                          fullStageNameList[-1]))
        for astage in fullStageNameList:
            fullStage = astage.strip()
            tokenList = astage.split('.')
            classString = tokenList.pop()
            classString = classString.strip()

            package = ".".join(tokenList)

            # For example  package -> lsst.pex.harness.App1Stage  classString -> App1Stage
            AppStage = __import__(package, globals(), locals(), [classString], -1)
            StageClass = getattr(AppStage, classString)
            self.stageClassList.append(StageClass) 

        #
        # Configure the Failure Stage
        #   - Read the policy information
        #   - Import failure stage Class and make failure stage instance Object
        #
        self.failureStageName = None 
        self.failSerialName   = None
        if (self.executePolicy.exists('failureStage')):
            failstg = self.executePolicy.get("failureStage")
            self.failureStageName = failstg.get("name") 

            if (failstg.exists('serialClass')):
                self.failSerialName = failstg.getString('serialClass')
                failStagePolicy = failstg.get('stagePolicy') 
            else:
                self.failSerialName = "lsst.pex.harness.stage.NoOpSerialProcessing"
                failStagePolicy = None

            astage = self.failSerialName
            tokenList = astage.split('.')
            failClassString = tokenList.pop()
            failClassString = failClassString.strip()

            package = ".".join(tokenList)

            # For example  package -> lsst.pex.harness.App1Stage  classString -> App1Stage
            FailAppStage = __import__(package, globals(), locals(), [failClassString], -1)
            FailStageClass = getattr(FailAppStage, failClassString)

            sysdata = {}
            sysdata["name"] = self.failureStageName
            sysdata["rank"] = -1
            sysdata["stageId"] = -1
            sysdata["universeSize"] = self.universeSize
            sysdata["runId"] =  self._runId
            if (failStagePolicy != None):
                self.failStageObject = FailStageClass(failStagePolicy, self.log, self.eventBrokerHost, sysdata)
            else:
                self.failStageObject = FailStageClass(None, self.log, self.eventBrokerHost, sysdata)

            log.log(self.VERB2, "failureStage %s " % self.failureStageName)
            log.log(self.VERB2, "failSerialName %s " % self.failSerialName)

        # Process Event Topics
        self.eventTopicList = []
        for item in fullStageList:
            self.eventTopicList.append(item.getString("eventTopic"))

        # Process Share Data Schedule
        self.shareDataList = []
        for item in fullStageList:
            shareDataStage = False
            if (item.exists('shareData')):
                shareDataStage = item.getBool('shareData')
            self.shareDataList.append(shareDataStage)

        log.log(self.VERB3, "Loading in %d trigger topics" % \
                len(filter(lambda x: x != "None", self.eventTopicList)))

        log.log(self.VERB3, "Loading in %d shareData flags" % \
                len(filter(lambda x: x != "None", self.shareDataList)))

        for iStage in xrange(len(self.eventTopicList)):
            item = self.eventTopicList[iStage]
            if self.eventTopicList[iStage] != "None":
                log.log(self.VERB3, "eventTopic%d: %s" % (iStage+1, item))
            else:
                log.log(Log.DEBUG, "eventTopic%d: %s" % (iStage+1, item))


        eventsSystem = events.EventSystem.getDefaultEventSystem()

        for topic in self.eventTopicList:
            if (topic == "None"):
                pass
            else:
                log.log(self.VERB2, "Creating receiver for topic %s" % (topic))
                eventsSystem.createReceiver(self.eventBrokerHost, topic)

        # Check for executionMode of oneloop 
        if (self.executePolicy.exists('executionMode') and (self.executePolicy.getString('executionMode') == "oneloop")):
            self.executionMode = 1 

        # Check for shutdownTopic 
        if (self.executePolicy.exists('shutdownTopic')):
            self.shutdownTopic = self.executePolicy.getString('shutdownTopic')
        else:
            self.shutdownTopic = "triggerShutdownEvent"

        # Check for exitTopic 
        if (self.executePolicy.exists('exitTopic')):
            self.exitTopic = self.executePolicy.getString('exitTopic')
        else:
            self.exitTopic = None

        log.log(self.VERB1, "Pipeline configuration complete");

    def initializeQueues(self):
        """
        Initialize the Queue List for the Pipeline
        """
        for iQueue in range(1, self.nStages+1+1):
            queue = Queue()
            self.queueList.append(queue)

    def initializeStages(self):
        """
        Initialize the Stage List for the Pipeline
        """
        log = self.log.timeBlock("initializeStages", self.TRACE-2)

        for iStage in range(1, self.nStages+1):
            # Make a Policy object for the Stage Policy file
            stagePolicy = self.stagePolicyList[iStage-1] 
            # Make an instance of the specifies Application Stage
            # Use a constructor with the Policy as an argument 
            StageClass = self.stageClassList[iStage-1]
            sysdata = {} 
            sysdata["name"] = self.stageNames[iStage-1]
            sysdata["rank"] = -1 
            sysdata["stageId"] = iStage 
            sysdata["universeSize"] = self.universeSize 
            sysdata["runId"] =  self._runId
            if (stagePolicy != "None"):
                stageObject = StageClass(stagePolicy, self.log, self.eventBrokerHost, sysdata)  
                # (self, policy=None, log=None, eventBroker=None, sysdata=None, callSetup=True):
            else:
                stageObject = StageClass(None, self.log, self.eventBrokerHost, sysdata)
            inputQueue  = self.queueList[iStage-1]
            outputQueue = self.queueList[iStage]

            stageObject.initialize(outputQueue, inputQueue)
            self.stageList.append(stageObject)

        log.done()

    def startShutdownThread(self):

        self.oneShutdownThread = ShutdownThread(self)
        self.oneShutdownThread.setDaemon(True)
        self.oneShutdownThread.start()

    def startSlices(self):
        """
        Initialize the Queue by defining an initial dataset list
        """
        log = self.log.timeBlock("startSlices", self.TRACE-2)

        log.log(self.VERB3, "Number of slices " + str(self.nSlices));

        self.loopEventList = []
        for i in range(self.nSlices):
           loopEventA = PyEvent()
           self.loopEventList.append(loopEventA)
           loopEventB = PyEvent()
           self.loopEventList.append(loopEventB)

        self.sliceThreadList = []

        for i in range(self.nSlices):
            k = 2*i
            loopEventA = self.loopEventList[k]
            loopEventB = self.loopEventList[k+1]
            oneSliceThread = SliceThread(i, self._pipelineName, self.pipelinePolicyName, \
               self._runId, self.logthresh, self.universeSize, loopEventA, loopEventB, self._logdir, self.workerId)
            self.sliceThreadList.append(oneSliceThread)

        for slicei in self.sliceThreadList:
            log.log(self.VERB3, "Starting slice");
            slicei.setDaemon(True) 
            slicei.start()
            if slicei.isAlive():
                log.log(self.VERB3, "slicei is Alive");
            else:
                log.log(self.VERB3, "slicei is not Alive");

        log.done()


    def startInitQueue(self):
        """
        Place an empty Clipboard in the first Queue
        """
        clipboard = Clipboard()

        #print "Python Pipeline Clipboard check \n"
        #acount=0
        #for clip in self.clipboardList:
        #    acount+=1
        #    print acount
        #    print str(clip)

        queue1 = self.queueList[0]
        queue1.addDataset(clipboard)

    def startStagesLoop(self): 
        """
        Method to execute loop over Stages
        """
        startStagesLoopLog = self.log.timeBlock("startStagesLoop", self.TRACE)
        looplog = BlockTimingLog(self.log, "visit", self.TRACE)
        stagelog = BlockTimingLog(looplog, "stage", self.TRACE-1)
        proclog = BlockTimingLog(stagelog, "process", self.TRACE)

        visitcount = 0 

        self.threadBarrier(0)

        while True:

            if (((self.executionMode == 1) and (visitcount == 1))):
                LogRec(looplog, Log.INFO)  << "terminating pipeline after one loop/visit "
                # 
                # Need to shutdown Threads here 
                # 
                break
            else:
                visitcount += 1
                looplog.setPreamblePropertyInt("LOOPNUM", visitcount)
                looplog.start()
                stagelog.setPreamblePropertyInt("LOOPNUM", visitcount)
                proclog.setPreamblePropertyInt("LOOPNUM", visitcount)

                # self.cppPipeline.invokeContinue()

                self.startInitQueue()    # place an empty clipboard in the first Queue

                self.errorFlagged = 0
                for iStage in range(1, self.nStages+1):
                    stagelog.setPreamblePropertyInt("STAGEID", iStage)
                    stagelog.start(self.stageNames[iStage-1] + " loop")
                    proclog.setPreamblePropertyInt("STAGEID", iStage)

                    stage = self.stageList[iStage-1]

                    self.handleEvents(iStage, stagelog)

                    # synchronize before preprocess
                    self.threadBarrier(iStage)

                    self.tryPreProcess(iStage, stage, stagelog)

                    # synchronize after preprocess, before process
                    self.threadBarrier(iStage)

                    # synchronize after process, before postprocess
                    self.threadBarrier(iStage)

                    self.tryPostProcess(iStage, stage, stagelog)

                    # synchronize after postprocess
                    self.threadBarrier(iStage)

                    stagelog.done()

                    self.checkExitByStage()

                else:
                    looplog.log(self.VERB2, "Completed Stage Loop")

                self.checkExitByVisit()

            # Uncomment to print a list of Citizens after each visit 
            # print datap.Citizen_census(0,0), "Objects:"
            # print datap.Citizen_census(datap.cout,0)

            looplog.log(Log.DEBUG, 'Retrieving finalClipboard for deletion')
            finalQueue = self.queueList[self.nStages]
            finalClipboard = finalQueue.getNextDataset()
            looplog.log(Log.DEBUG, "deleting final clipboard")
            looplog.done()
            # delete entries on the clipboard
            finalClipboard.close()
            del finalClipboard

        startStagesLoopLog.log(Log.INFO, "Shutting down pipeline");
        self.shutdown()
        startStagesLoopLog.done()


    def getSliceThreadList(self):
        return self.sliceThreadList

    def setExitLevel(self, level):
        self.exitLevel = level

    def checkExitBySyncPoint(self): 
        log = Log(self.log, "checkExitBySyncPoint")

        if((self._stop.isSet()) and (self.exitLevel == 2)):
            log.log(Log.INFO, "Pipeline stop is set at exitLevel of 2")
            log.log(Log.INFO, "Exit here at a Synchronization point")
            sys.exit()

    def checkExitByStage(self): 
        log = Log(self.log, "checkExitByStage")

        if((self._stop.isSet()) and (self.exitLevel == 3)):
            log.log(Log.INFO, "Pipeline stop is set at exitLevel of 3")
            log.log(Log.INFO, "Exit here at the end of the Stage")
            sys.exit()

    def checkExitByVisit(self): 
        log = Log(self.log, "checkExitByVisit")

        if((self._stop.isSet()) and (self.exitLevel == 4)):
            log.log(Log.INFO, "Pipeline stop is set at exitLevel of 4")
            log.log(Log.INFO, "Exit here at the end of the Visit")
            sys.exit()

    def threadBarrier(self, iStage): 
        """
        Create an approximate barrier where all Slices intercommunicate with the Pipeline 
        """

        log = Log(self.log, "threadBarrier")

        self.checkExitBySyncPoint()

        # if((self._stop.isSet()) and (self.exitLevel == 2)):

        #     log.log(Log.INFO, "Pipeline stop is set at exitLevel of 2; exit here at a synchronization point")
        #     print "Pipeline stop is set at exitLevel of 2; exit here at a synchronization point" 
            # os._exit() 
        #    sys.exit()
        #    log.log(Log.INFO, "Pipeline Ever reach here ?? ")

        entryTime = time.time()
        log.log(Log.DEBUG, "Entry time %f" % (entryTime)) 
        

        for i in range(self.nSlices):
            k = 2*i
            loopEventA = self.loopEventList[k]
            loopEventB = self.loopEventList[k+1]

            signalTime1 = time.time()
            log.log(Log.DEBUG, "Signal to Slice  %d %f" % (i, signalTime1)) 

            loopEventA.set()

            log.log(Log.DEBUG, "Wait for signal from Slice %d" % (i)) 

            # Wait for the B event to be set by the Slice
            # Excute time sleep in between checks to free the GIL periodically 
            useDelay = self.barrierDelay

            if(iStage == 1): 
                useDelay = 0.1
            if(iStage == 290): 
                useDelay = 0.1

            while( not (loopEventB.isSet())):
                 time.sleep(useDelay)

            signalTime2 = time.time()
            log.log(Log.DEBUG, "Done waiting for signal from Slice %d %f" % (i, signalTime2)) 

            if(loopEventB.isSet()):
                loopEventB.clear()

        self.checkExitBySyncPoint()

    def forceShutdown(self): 
        """
        Shutdown the Pipeline execution: delete the MPI environment
        Send the Exit Event if required
        """

        self.log.log(self.VERB2, 'Pipeline forceShutdown : Stopping Slices ')

        for i in range(self.nSlices):
            slice = self.sliceThreadList[i]
            slice.stop()
            self.log.log(self.VERB2, 'Slice ' + str(i) + ' stopped.')

        for i in range(self.nSlices):
            slice = self.sliceThreadList[i]
            slice.join()
            self.log.log(self.VERB2, 'Slice ' + str(i) + ' exited.')

        # Also have to tell the shutdown Thread to stop  
        # self.log.log(self.VERB2, 'Telling Shutdown thread to exit')
        # self.oneShutdownThread.exit()
        # self.log.log(self.VERB2, 'Shutdown thread has exited')
        # self.log.log(self.VERB2, 'Main thread exiting ')
        # sys.exit()

    def shutdown(self): 
        """
        Shutdown the Pipeline execution: delete the MPI environment
        Send the Exit Event if required
        """
        if self.exitTopic == None:
            pass
        else:
            oneEventTransmitter = events.EventTransmitter(self.eventBrokerHost, self.exitTopic)
            psPtr = dafBase.PropertySet()
            psPtr.setString("message", str("exiting_") + self._runId )

            oneEventTransmitter.publish(psPtr)

        for i in range(self.nSlices):
            slice = self.sliceThreadList[i]
            slice.join()
            self.log.log(self.VERB2, 'Slice ' + str(i) + ' ended.')

        # Also have to tell the shutdown Thread to stop  
        self.oneShutdownThread.setStop()
        self.oneShutdownThread.join()
        self.log.log(self.VERB2, 'Shutdown thread ended ')

        sys.exit()


    def invokeSyncSlices(self, iStage, stagelog):
        """
        THIS GOES AWAY in non MPI harness 
        If needed, calls the C++ Pipeline invokeSyncSlices
        """
        invlog = stagelog.timeBlock("invokeSyncSlices", self.TRACE-1)
        if(self.shareDataList[iStage-1]):
            invlog.log(self.VERB3, "Calling invokeSyncSlices")
            # self.cppPipeline.invokeSyncSlices()  
            # invokeSyncSlices 
        invlog.done()

    def tryPreProcess(self, iStage, stage, stagelog):
        """
        Executes the try/except construct for Stage preprocess() call 
        """
        prelog = stagelog.timeBlock("tryPreProcess", self.TRACE-2);

        # Important try - except construct around stage preprocess() 
        try:
            # If no error/exception has been flagged, run preprocess()
            # otherwise, simply pass along the Clipboard 
            if (self.errorFlagged == 0):
                processlog = stagelog.timeBlock("preprocess", self.TRACE)
                self.interQueue = stage.applyPreprocess()
                processlog.done()
            else:
                prelog.log(self.TRACE, "Skipping process due to error")
                self.transferClipboard(iStage)

        except:
            trace = "".join(traceback.format_exception(
                    sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            prelog.log(Log.FATAL, trace)

            # Flag that an exception occurred to guide the framework to skip processing
            prelog.log(self.VERB2, "Flagging error in tryPreProcess, tryPostProcess to be skipped")
            self.errorFlagged = 1

            if(self.failureStageName != None): 
                if(self.failSerialName != "lsst.pex.harness.stage.NoOpSerialProcessing"):

                    LogRec(prelog, self.VERB2) << "failureStageName exists "    \
                                           << self.failureStageName     \
                                           << "and failSerialName exists "    \
                                           << self.failSerialName     \
                                           << LogRec.endr;

                    inputQueue  = self.queueList[iStage-1]
                    outputQueue = self.queueList[iStage]

                    clipboard = inputQueue.element()
                    clipboard.put("failedInStage",  stage.getName()) 
                    clipboard.put("failedInStageN", iStage) 
                    clipboard.put("failureType", str(sys.exc_info()[0])) 
                    clipboard.put("failureMessage", str(sys.exc_info()[1])) 
                    clipboard.put("failureTraceback", trace) 

                    self.failStageObject.initialize(outputQueue, inputQueue)

                    self.interQueue = self.failStageObject.applyPreprocess()

                else:
                    prelog.log(self.VERB2, "No SerialProcessing to do for failure stage")

            # Post the cliphoard that the Stage failed to transfer to the output queue
            self.postOutputClipboard(iStage)

        prelog.done()
        # Done try - except around stage preprocess 

    def tryPostProcess(self, iStage, stage, stagelog):
        """
        Executes the try/except construct for Stage postprocess() call 
        """
        postlog = stagelog.timeBlock("tryPostProcess",self.TRACE-2);

        # Important try - except construct around stage postprocess() 
        try:
            # If no error/exception has been flagged, run postprocess()
            # otherwise, simply pass along the Clipboard 
            if (self.errorFlagged == 0):
                processlog = stagelog.timeBlock("postprocess", self.TRACE)
                stage.applyPostprocess(self.interQueue)
                processlog.done()
            else:
                postlog.log(self.TRACE, "Skipping applyPostprocess due to flagged error")
                self.transferClipboard(iStage)

        except:
            trace = "".join(traceback.format_exception(
                    sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            postlog.log(Log.FATAL, trace)

            # Flag that an exception occurred to guide the framework to skip processing
            self.errorFlagged = 1

            if(self.failureStageName != None):
                if(self.failSerialName != "lsst.pex.harness.stage.NoOpSerialProcessing"):

                    LogRec(postlog, self.VERB2) << "failureStageName exists "    \
                                           << self.failureStageName     \
                                           << "and failSerialName exists "    \
                                           << self.failSerialName     \
                                           << LogRec.endr;

                    inputQueue  = self.queueList[iStage-1]
                    outputQueue = self.queueList[iStage]

                    self.failStageObject.initialize(outputQueue, inputQueue)

                    self.failStageObject.applyPostprocess(self.interQueue)

                else:
                    postlog.log(self.VERB2, "No SerialProcessing to do for failure stage")

            # Post the cliphoard that the Stage failed to transfer to the output queue
            self.postOutputClipboard(iStage)

        # Done try - except around stage preprocess
        postlog.done()

    def waitForEvent(self, thisTopic):
        """
        wait for a single event of a designated topic
        """

        eventsSystem = events.EventSystem.getDefaultEventSystem()

        sleepTimeout = 0.1
        transTimeout = 900

        inputParamPropertySetPtr  = None
        waitLoopCount = 0
        while((inputParamPropertySetPtr == None) and (waitLoopCount < self.eventTimeout)):
            if(self.logthresh == self.VERB3):
                print "Pipeline waitForEvent Looping : checking for event ... \n"

            inputParamPropertySetPtr = eventsSystem.receive(thisTopic, transTimeout)

            time.sleep(sleepTimeout)

            if((self._stop.isSet())):
                break

            waitLoopCount = waitLoopCount+1

        return inputParamPropertySetPtr


    def handleEvents(self, iStage, stagelog):
        """
        Handles Events: transmit or receive events as specified by Policy
        """
        log = stagelog.timeBlock("handleEvents", self.TRACE-2)
        eventsSystem = events.EventSystem.getDefaultEventSystem()

        thisTopic = self.eventTopicList[iStage-1]
        thisTopic = thisTopic.strip()


        if (thisTopic != "None"):
            log.log(self.VERB3, "Processing topic: " + thisTopic)
            sliceTopic = "%s_%s" % (thisTopic, self._pipelineName)

            waitlog = log.timeBlock("eventwait " + thisTopic, self.TRACE,
                                     "wait for event...")

            inputParamPropertySetPtr = self.waitForEvent(thisTopic)

            # inputParamPropertySetPtr = eventsSystem.receive(thisTopic, self.eventTimeout)

            waitlog.done()

            if (inputParamPropertySetPtr != None):
                LogRec(log, self.TRACE) << "received event; contents: "        \
                                << inputParamPropertySetPtr \
                                << LogRec.endr;

                log.log(self.VERB2, "received event; sending it to Slices " + sliceTopic)

                # Pipeline  does not disassemble the payload of the event.
                # It places the payload on the clipboard with key of the eventTopic
                self.populateClipboard(inputParamPropertySetPtr, iStage, thisTopic)

                eventsSystem.createTransmitter(self.eventBrokerHost, sliceTopic)
                eventsSystem.publish(sliceTopic, inputParamPropertySetPtr)

                log.log(self.VERB2, "event sent to Slices")
            else: 
                if((self._stop.isSet())):
                    # Shutdown event received while waiting for data event 
                    log.log(self.VERB2, "Pipeline Shutting down : Shutdown event received.")
                else:
                    # event was not received after a long timeout
                    # log.log(self.VERB2, "Pipeline Shutting Down: Event timeout self.: No event arrived")

                    LogRec(log, self.VERB2) << "Pipeline Shutting Down: Event timeout  "   \
                                << str(self.eventTimeout) \
                                << "reached: No or next event did not arrive " \
                                << LogRec.endr;


                sys.exit()

        else: # This stage has no event dependence 
            log.log(Log.DEBUG, 'No event to handle')

        log.done()

    def populateClipboard(self, inputParamPropertySetPtr, iStage, eventTopic):
        """
        Place the event payload onto the Clipboard 
        """
        log = self.log.timeBlock("populateClipboard", self.TRACE-2);

        queue = self.queueList[iStage-1]
        clipboard = queue.element()

        # Pipeline does not disassemble the payload of the event.
        # It knows nothing of the contents.
        # It simply places the payload on the clipboard with key of the eventTopic
        clipboard.put(eventTopic, inputParamPropertySetPtr)

        log.done()

    def postOutputClipboard(self, iStage):
        """
        Place an empty Clipboard in the output queue for designated stage
        """
        clipboard = Clipboard()
        queue2 = self.queueList[iStage]
        queue2.addDataset(clipboard)

    def transferClipboard(self, iStage):
        """
        Move the Clipboard from the input queue to output queue for the designated stage
        """
        # clipboard = Clipboard()
        queue1 = self.queueList[iStage-1]
        queue2 = self.queueList[iStage]
        clipboard = queue1.getNextDataset()
        if (clipboard != None):
            queue2.addDataset(clipboard)

    def getRun(self):
        """
        get method for the runId
        """
        return self._runId

    def setRun(self, run):
        """
        set method for the runId
        """
        self._runId = run

    def getShutdownTopic(self):
        """
        get method for the shutdown event topic 
        """
        return self.shutdownTopic

    def getEventBrokerHost(self):
        """
        get method for the event broker host 
        """
        return self.eventBrokerHost


    def getLogThreshold(self):
        """
        return the default message importance threshold being used for 
        recording messages.  The returned value reflects the threshold 
        associated with the default root (system-wide) logger (or what it will
        be after logging is initialized).  Some underlying components may 
        override this threshold.
        """
        if self.log is None:
            return self.logthresh
        else:
            return Log.getDefaultLog().getThreshold()

    def setLogThreshold(self, level):
        """
        set the default message importance threshold to be used for 
        recording messages.  This will value be applied to the default
        root (system-wide) logger (or what it will be after logging is 
        initialized) so that all software components are affected.
        """
        if self.log is not None:
            Log.getDefaultLog().setThreshold(level)
            self.log.log(Log.INFO, 
                         "Upating Root Log Message Threshold to %i" % level)
        self.logthresh = level

    def setLogDir(self, logdir):
        """
        set the default directory into which the pipeline should write log files 
        @param logdir   the directory in which log files should be written
        """
        if (logdir == "None" or logdir == None):
            self._logdir = ""
        else:
            self._logdir = logdir

    def setEventTimeout(self, timeout):
        """
        set the default message importance threshold to be used for 
        recording messages.  This will value be applied to the default
        root (system-wide) logger (or what it will be after logging is 
        initialized) so that all software components are affected.
        """
        if self.log is not None:
            self.log.log(Log.INFO, 
                         "Updating event timeout to %i" % timeout)
        self.eventTimeout = timeout

    def makeStageName(self, appStagePolicy):
        if appStagePolicy.getValueType("stagePolicy") == appStagePolicy.FILE:
            pfile = os.path.splitext(os.path.basename(
                        appStagePolicy.getFile("stagePolicy").getPath()))[0]
            return trailingpolicy.sub('', pfile)
        else:
            return None
        
trailingpolicy = re.compile(r'_*(policy|dict)$', re.IGNORECASE)


