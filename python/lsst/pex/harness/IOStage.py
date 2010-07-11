#!/usr/bin/env python

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

# -*- python -*-

"""
IOStage provides a generic mechanism for persisting and retrieving data for
LSST pipelines.  InputStages and OutputStages are configurable by Policy to
retrieve and persist Persistable classes, given, for each, a list of
sub-Policies with storage types and logical locations.

Additional data may be provided to customize the LogicalLocation string and to
pass to the persistence framework.  This data comes from the clipboard passed
to the stage; PropertySet names of the additional data items to be retrieved
are given in the AdditionalData sub-Policy.
"""

import os
import sys
import time
import lsst.pex.harness.stage as harnessStage

import lsst.pex.harness.Utils
from lsst.pex.harness import Dataset
import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.policy as pexPolicy
from lsst.pex.logging import Log
import lsst.pex.exceptions as pexExcept

class OutputStageSerial(harnessStage.SerialProcessing):
    """A Stage that persists data."""

    def setup(self):
        _outputSetup(self)

    def preprocess(self, clipboard):
        """Persist the requested data in the master process before any
        (subclass) processing, if desired."""

        if self.policy.exists('parameters.runMode') and \
                self.policy.getString('parameters.runMode') == 'preprocess':
            _output(self, self.policy, clipboard, self.log)


    def postprocess(self, clipboard):
        """Persist the requested data in the master process after any
        (subclass) processing, if desired."""

        if self.policy.exists('parameters.runMode') and \
                self.policy.getString('parameters.runMode') == 'postprocess':
            _output(self, self.policy, clipboard, self.log)

class OutputStageParallel(harnessStage.ParallelProcessing):

    def setup(self):
        _outputSetup(self)

    def process(self, clipboard):
        """Persist the requested data in the slice processes."""

        _output(self, self.policy, clipboard, self.log)

class InputStageParallel(harnessStage.ParallelProcessing):

    def setup(self):
        _inputSetup(self)

    def process(self, clipboard):
        """Retrieve the requested data in the slice processes."""

        _input(self, self.policy, clipboard, self.log)

class InputStageSerial(harnessStage.SerialProcessing):
    """A Stage that retrieves data."""

    def setup(self):
        _inputSetup(self)

    def preprocess(self, clipboard):
        """Retrieve the requested data in the master process before any
        (subclass) processing, if desired."""

        if self.policy.exists('parameters.runMode') and \
                self.policy.getString('parameters.runMode') == 'preprocess':
            _input(self, self.policy, clipboard, self.log)

    def postprocess(self, clipboard):
        """Retrieve the requested data in the master process after any
        (subclass) processing, if desired."""

        if self.policy.exists('parameters.runMode') and \
                self.policy.getString('parameters.runMode') == 'postprocess':
            _input(self, self.policy, clipboard, self.log)


class InputStage(harnessStage.Stage):
    serialClass = InputStageSerial
    parallelClass = InputStageParallel

class OutputStage(harnessStage.Stage):
    serialClass = OutputStageSerial
    parallelClass = OutputStageParallel


###############################################################################

def _outputSetup(stage):
    """Set up an OutputStage."""

    stage.log = Log(Log.getDefaultLog(), "pex.harness.iostage.output")
    defaultFile = pexPolicy.DefaultPolicyFile("pex_harness",
            "OutputStageDictionary.paf", "policy")
    defaults = pexPolicy.Policy.createPolicy(defaultFile,
            defaultFile.getRepositoryPath())
    stage.policy.mergeDefaults(defaults)

    if stage.policy.exists('parameters.butler'):
        bf = dafPersist.ButlerFactory(
                stage.policy.getPolicy('parameters.butler'))
        stage.butler = bf.create()
    else:
        stage.butler = None

def _output(stage, policy, clipboard, log):
    """Perform the actual persistence.
    
    @param stage     The stage requesting output.
    @param policy    The policy for the stage.
    @param clipboard The clipboard for the stage.  The persisted objects are taken from this.
    @param log       A logger for messages.
    """

    if not policy.exists('parameters.outputItems'):
        # Propagate the clipboard to the output queue, but otherwise
        # do nothing.
        log.log(Log.WARN, "No outputItems found")
        return

    mainAdditionalData = lsst.pex.harness.Utils.createAdditionalData(stage,
            policy, clipboard)

    # Create a persistence object using policy, if present.
    if policy.exists('parameters.persistence'):
        persistencePolicy = pexPolicy.Policy(
                policy.getPolicy('parameters.persistence'))
    else:
        persistencePolicy = pexPolicy.Policy()
    persistence = dafPersist.Persistence.getPersistence(
            persistencePolicy)

    # Iterate over items in OutputItems policy.
    outputPolicy = policy.getPolicy('parameters.outputItems')
    itemNames = outputPolicy.policyNames(True)
    somethingWasOutput = False

    for item in itemNames:

        additionalData = mainAdditionalData.deepCopy()

        itemPolicy = outputPolicy.getPolicy(item)

        # Skip the item if it is not required and is not present.
        itemRequired = itemPolicy.exists('required') and \
                itemPolicy.getBool('required')
        if not clipboard.contains(item):
            if itemRequired:
                raise RuntimeError, 'Missing output item: ' + item
            else:
                continue

        itemData = clipboard.get(item)

        # Add the item name to the additionalData.
        additionalData.set('itemName', item)

        if itemPolicy.exists('datasetId'):
            dsPolicy = itemPolicy.getPolicy('datasetId')
            ds = Dataset(dsPolicy.get('datasetType'))
            ds.ids = {}
            if dsPolicy.exists('set'):
                setPolicy = dsPolicy.getPolicy('set')
                for param in setPolicy.paramNames():
                    ds.ids[param] = setPolicy.get(param)
                    additionalData.set(param, setPolicy.get(param))
            if dsPolicy.exists('fromJobIdentity'):
                jobIdentity = clipboard.get(policy.get('inputKeys.jobIdentity'))
                for id in dsPolicy.getStringArray('fromJobIdentity'):
                    ds.ids[id] = jobIdentity[id]
                    additionalData.set(id, jobIdentity[id])
            outputKey = policy.get('outputKeys.outputDatasets')
            dsList = clipboard.get(outputKey)
            if dsList is None:
                dsList = []
                clipboard.put(outputKey, dsList)
            dsList.append(ds)
            if stage.butler is not None:
                # Use the butler to figure out storage and locations.
                log.log(Log.INFO, "persisting %s as %s with keys %s" % (item,
                    ds.type, ds.ids))
                stage.butler.put(itemData, ds.type, dataId=ds.ids)
                log.log(Log.INFO, "persisting %s complete" % (item,))
                somethingWasOutput = True
                continue

        # Get the item's StoragePolicy.
        if itemPolicy.isArray('storagePolicy'):
            policyList = itemPolicy.getPolicyArray('storagePolicy')
        else:
            policyList = []
            policyList.append(itemPolicy.getPolicy('storagePolicy'))
       
        # Create a list of Storages for the item based on policy.
        storageList = dafPersist.StorageList()
        for storagePolicy in policyList:
            storageName = storagePolicy.getString('storage')
            location = storagePolicy.getString('location')
            logLoc = dafPersist.LogicalLocation(location, additionalData)
            log.log(Log.INFO, "persisting %s to %s" % (item, logLoc.locString()))
            additionalData.add('StorageLocation.' + storageName, logLoc.locString())
            mainAdditionalData.add('StorageLocation.' + storageName, logLoc.locString())
            storage = persistence.getPersistStorage(storageName,  logLoc)
            storageList.append(storage)

        # Persist the item.

        if hasattr(itemData, '__deref__'):
            persistence.persist(itemData.__deref__(), storageList, additionalData)
        else:
            persistence.persist(itemData, storageList, additionalData)
        log.log(Log.INFO, "persisting %s complete" % (item,))
        somethingWasOutput = True

    if not somethingWasOutput:
        log.log(Log.WARN, "No items were output")


###############################################################################

def _inputSetup(stage):
    """Set up an InputStage."""

    stage.log = Log(Log.getDefaultLog(), "pex.harness.iostage.input")
    defaultFile = pexPolicy.DefaultPolicyFile("pex_harness",
            "InputStageDictionary.paf", "policy")
    defaults = pexPolicy.Policy.createPolicy(defaultFile,
            defaultFile.getRepositoryPath())
    stage.policy.mergeDefaults(defaults)

    if stage.policy.exists('parameters.butler'):
        bf = dafPersist.ButlerFactory(
                stage.policy.getPolicy('parameters.butler'))
        stage.butler = bf.create()
    else:
        stage.butler = None

def _input(stage, policy, clipboard, log):
    """Perform the retrieval of items from the clipboard as controlled by policy.
    
    @param stage     The stage requesting input.
    @param policy    The policy for the stage.
    @param clipboard The clipboard for the stage.  The retrieved objects are added to this.
    @param log       A logger for messages.
    """

    if not policy.exists('parameters.inputItems'):
        # Propagate the clipboard to the output queue, but otherwise
        # do nothing.
        log.log(Log.WARN, "No InputItems found")
        return

    if stage.butler is not None:
        _inputUsingButler(stage, policy, clipboard, log)
        return

    additionalData = lsst.pex.harness.Utils.createAdditionalData(stage,
            policy, clipboard)

    # Create a persistence object using policy, if present.
    if policy.exists('parameters.persistence'):
        persistencePolicy = pexPolicy.Policy(
                policy.getPolicy('parameters.persistence'))
    else:
        persistencePolicy = pexPolicy.Policy()
    persistence = dafPersist.Persistence.getPersistence(
            persistencePolicy)

    # Iterate over items in InputItems policy.
    inputPolicy = policy.getPolicy('parameters.inputItems')
    itemNames = inputPolicy.policyNames(True)
    for item in itemNames:

        itemPolicy = inputPolicy.getPolicy(item)
        cppType = itemPolicy.getString('type')
        pythonTypeName = itemPolicy.getString('pythonType')
        # import this pythonType dynamically 
        pythonTypeTokenList = pythonTypeName.split('.')
        importClassString = pythonTypeTokenList.pop()
        importClassString = importClassString.strip()
        importPackage = ".".join(pythonTypeTokenList)

        # For example  importPackage -> lsst.afw.Core.afwLib  
        #              importClassString -> MaskedImageF
        importType = __import__(importPackage, globals(), locals(), \
                                   [importClassString], -1)
        pythonType = getattr(importType, importClassString)

        # Add the item name to the additionalData.
        additionalData.set('itemName', item)

        if itemPolicy.exists("datasetType"):
            result = []
            datasetType = itemPolicy.get("datasetType")
            idList = []
            for ds in clipboard.get(policy.get("inputKeys.inputDatasets")):
                if ds.type == datasetType:
                    idList.append(ds.ids)
            storage = itemPolicy.getString('storage')
            location = itemPolicy.getString('location')

            for id in idList:
                tempAdditionalData = additionalData.deepCopy()
                for k, v in id.iteritems():
                    tempAdditionalData.set(k, v)
                finalItem = _read(item, cppType, pythonType,
                        [(storage, location)], tempAdditionalData,
                        persistence, log)
                result.append(finalItem)
            if len(result) == 1:
                clipboard.put(item, result[0])
            else:
                clipboard.put(item, result)

        else:
            # Get the item's StoragePolicy.
            if itemPolicy.isArray('storagePolicy'):
                policyList = itemPolicy.getPolicyArray('storagePolicy')
            else:
                policyList = []
                policyList.append(itemPolicy.getPolicy('storagePolicy'))
        
            storageInfo = []
            for policy in policyList:
                storage = policy.getString('storage')
                location = policy.getString('location')
                storageInfo.append((storage, location))
    
            finalItem = _read(item, cppType, pythonType,
                    storageInfo, additionalData,
                    persistence, log)
            clipboard.put(item, finalItem)

def _read(item, cppType, pythonType, storageInfo,
        additionalData, persistence, log):
    # Create a list of Storages for the item based on policy.
    storageList = dafPersist.StorageList()
    for storageName, location in storageInfo:
        logLoc = dafPersist.LogicalLocation(location, additionalData)
        if not logLoc.locString().startswith("mysql:"):
            _waitForPath(logLoc.locString(), log)
        log.log(Log.INFO, "loading %s from %s" % (item, logLoc.locString()));
        storage = persistence.getRetrieveStorage(storageName, logLoc)
        storageList.append(storage)

    # Retrieve the item.
    itemData = persistence.unsafeRetrieve(cppType, storageList, additionalData)

    # Cast the SWIGged Persistable to a more useful type.

    cvt = getattr(pythonType, "swigConvert")
    finalItem = cvt(itemData)
    log.log(Log.INFO, "loading %s complete" % (item,));

    # If Persistable and subclasses are NOT wrapped using SWIG_SHARED_PTR,
    # then one must make sure that the wrapper for the useful type owns
    # the pointer (rather than the wrapper for the original Persistable).
    # The following lines accomplish this:
    #itemData.this.disown()
    #finalItem.this.acquire()

    # Put the item on the clipboard
    return finalItem

def _inputUsingButler(stage, policy, clipboard, log):
    inputPolicy = policy.getPolicy('parameters.inputItems')
    itemNames = inputPolicy.policyNames(True)
    for item in itemNames:
        itemPolicy = inputPolicy.getPolicy(item)
        datasetType = itemPolicy.getString('datasetType')
        datasetIdPolicy = itemPolicy.getPolicy('datasetId')
        required = True
        if itemPolicy.exists('required'):
            required = itemPolicy.getBool('required')
        timeout = 60.0
        if itemPolicy.exists('timeout'):
            timeout = itemPolicy.get('timeout')
        if datasetIdPolicy.exists('fromInputDatasets') and \
                datasetIdPolicy.getBool('fromInputDatasets'):
            inputDatasets = clipboard.get(
                    policy.getString('inputKeys.inputDatasets'))
            itemList = []
            for ds in inputDatasets:
                if ds.type == datasetType:
                    _waitForDataset(stage.butler, datasetType, ds.ids, log,
                            required, timeout)
                    log.log(Log.INFO, "will load %s from %s with keys %s" %
                            (item, datasetType, str(ds.ids)));
                    obj = stage.butler.get(datasetType, dataId=ds.ids)
                    itemList.append(obj)
            if len(itemList) == 0:
                raise IOError, "No input datasets of type %s for item %s" % \
                        (datasetType, item)
            elif len(itemList) == 1:
                clipboard.put(item, itemList[0])
            else:
                clipboard.put(item, itemList)
        elif datasetIdPolicy.exists('fromJobIdentity'):
            jobIdentity = clipboard.get(
                    policy.getString('inputKeys.jobIdentity'))
            dataId = {}
            for key in datasetIdPolicy.getStringArray('fromJobIdentity'):
                dataId[key] = jobIdentity[key]
            if datasetIdPolicy.exists('set'):
                setPolicy = datasetIdPolicy.getPolicy('set')
                for param in setPolicy.paramNames():
                    dataId[param] = setPolicy.get(param)
            _waitForDataset(stage.butler, datasetType, dataId, log, required,
                    timeout)
            log.log(Log.INFO, "will load %s from %s with keys %s" % (item,
                datasetType, str(dataId)));
            obj = stage.butler.get(datasetType, dataId=dataId)
            clipboard.put(item, obj)
        else:
            raise pexExcept.LsstException, \
                "datasetId missing both fromInputDatasets and fromJobIdentity"

def _waitFor(func, log, timeoutMsg="Unavailable dataset", timeout=60.0,
        initial=0.5, backoff=1.2):
    sleep = initial
    totalSleep = 0
    while func() is False: # Allow for None return
        log.log(Log.INFO, "waiting for dataset")
        if timeout > 0:
            time.sleep(sleep)
        totalSleep += sleep
        sleep *= backoff
        if totalSleep >= timeout:
            if timeoutMsg is not None:
                raise pexExcept.LsstException, timeoutMsg
            else:
                return

def _waitForPath(path, log):
    _waitFor(lambda: os.path.exists(path), log,
            "Timed out waiting for file %s" % (path,))

def _waitForDataset(butler, datasetType, dataId, log, required, timeout):
    if required:
        msg = "Timed out waiting for dataset %s with keys %s" % \
                (datasetType, str(dataId))
    else:
        msg = None
    _waitFor(lambda: butler.datasetExists(datasetType, dataId=dataId), log,
            msg, timeout)
