#!/usr/bin/env python
# -*- python -*-

"""
IOStage provides a generic mechanism for persisting and retrieving data for
LSST pipelines.  InputStages and OutputStages are configurable by Policy to
retrieve and persist Persistable classes, given, for each, a list of
sub-Policies with storage types and logical locations.

Additional data may be provided to customize the LogicalLocation string and to
pass to the persistence framework.  This data comes from the clipboard passed
to the stage; DataProperty names of the additional data items to be retrieved
are given in the AdditionalData sub-Policy.
"""

import lsst.dps.Stage
import lsst.mwi.data
import lsst.mwi.policy
import lsst.mwi.persistence
import lsst.mwi.utils
from lsst.mwi.logging import Log
import re

__all__ = ['OutputStage', 'InputStage']

def massage(location, additionalData): 
    """
    Substitute values from additionalData into the location, if requested.
    """

    lsst.mwi.utils.Trace("IOStage", 3, "Massaging location: " + location)
    if location.find('%{') == -1:
        lsst.mwi.utils.Trace("IOStage", 3, "\tNo substitutions")
        return location
    vars = re.compile(r'\%\{(\w+)(\+1)?(=([^}]+))?\}')

    def replace(match):
        """
        Get the value of a DataProperty key in match.group(1), using
        match.group(4) as a default if not found.
        Add one to integer values if match.group(2) is "+1".
        """
        dp = additionalData.findUnique(match.group(1))
	if dp:
            try:
                value = dp.getValueString()
                return value
            except:
                pass
            try:
                value = dp.getValueInt64()
                if match.group(2):
                    value += 1
                return repr(value)
            except:
                pass
            try:
                value = dp.getValueInt()
                if match.group(2):
                    value += 1
                return repr(value)
            except:
                pass
            try:
                value = dp.getValueFloat()
                return repr(value)
            except:
                pass
        if match.group(4):
            return match.group(4)
        else:
            raise Runtime, 'Unknown substitution in IOStage'

    newLoc = vars.sub(replace, location)
    lsst.mwi.utils.Trace("IOStage", 3, "\tnew location: " + newLoc)
    return newLoc

def createAdditionalData(stage, stagePolicy, clipboard):
    """
    Extract additionalData values, as specified by policy, from the clipboard.
    """

    dataProperty = \
        lsst.mwi.data.SupportFactory.createPropertyNode("additionalData")

    # Parse array of "key=clipboard-key" or
    # "key=clipboard-key.dataproperty-key" mappings
    if stagePolicy.exists('AdditionalData'):
        dataPairs = stagePolicy.getStringArray('AdditionalData')
        for pair in dataPairs:
            (rename, name) = pair.split("=")
            if name.find(".") != -1:
                (clipKey, dpKey) = name.split(".", 1)
                data = clipboard.get(clipKey).findUnique(dpKey).getValue()
            else:
                data = clipboard.get(name)
            leaf = lsst.mwi.data.DataProperty(rename, data)
            lsst.mwi.utils.Trace("IOStage", 3, "AdditionalData item: " + pair)
            dataProperty.addProperty(leaf)

    # Add the predefined runId, sliceId, and universeSize keys

    runId = stage.getRun()
    leaf = lsst.mwi.data.DataProperty('runId', runId)
    dataProperty.addProperty(leaf)

    sliceId = stage.getRank()
    leaf = lsst.mwi.data.DataProperty('sliceId', sliceId)
    dataProperty.addProperty(leaf)

    ccdId = "%02d" % (stage.getRank() + 1)
    leaf = lsst.mwi.data.DataProperty('ccdId', ccdId)
    dataProperty.addProperty(leaf)

    universeSize = stage.getUniverseSize()
    leaf = lsst.mwi.data.DataProperty('universeSize', universeSize)
    dataProperty.addProperty(leaf)

    lsst.mwi.utils.Trace("IOStage", 3, "additionalData:\n" + \
        dataProperty.toString('\t', True))
    return dataProperty

class OutputStage (lsst.dps.Stage.Stage):
    """
    A Stage that persists data.
    """

    def __init__(self, stageId, stagePolicy):
        """
        Initialize the stage with a policy.
        """

        lsst.dps.Stage.Stage.__init__(self, stageId, stagePolicy)

    def preprocess(self):
        """
        Persist the requested data in the master process before any
        (subclass) processing, if desired.
        """

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'preprocess':
            self._output()

    def process(self):
        """
        Persist the requested data in the slice processes.
        """

        if self._policy.exists('RunMode') and \
                (self._policy.getString('RunMode') == 'preprocess' or \
                self._policy.getString('RunMode') == 'postprocess'):
            # Nobody has copied the clipboards yet in the worker process
            for i in xrange(self.inputQueue.size()):
                clipboard = self.inputQueue.getNextDataset()
                self.outputQueue.addDataset(clipboard)
            return
        self._output()

    def postprocess(self):
        """
        Persist the requested data in the master process after any
        (subclass) processing, if desired.
        """

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'postprocess':
            self._output()

        if not self._policy.exists('RunMode') or \
                (self._policy.getString('RunMode') != 'preprocess' and \
                self._policy.getString('RunMode') != 'postprocess'):
            # Nobody has copied the clipboards yet in the master process
            for i in xrange(self.inputQueue.size()):
                clipboard = self.inputQueue.getNextDataset()
                self.outputQueue.addDataset(clipboard)

    def _output(self):
        """
        Perform the actual persistence.
        """

        for i in xrange(self.inputQueue.size()):

            clipboard = self.inputQueue.getNextDataset()
            if not self._policy.exists('OutputItems'):
                # Propagate the clipboard to the output queue, but otherwise
                # do nothing.
                mylog = Log(Log.defaultLog(), "IOStage")
                mylog.log(Log.WARN, "No OutputItems found")
                self.outputQueue.addDataset(clipboard)
                continue

            additionalData = createAdditionalData(self, self._policy, clipboard)

            # Create a persistence object using policy, if present.
            if self._policy.exists('Persistence'):
                persistencePolicy = lsst.mwi.policy.PolicyPtr( \
                        self._policy.getPolicy('Persistence'))
            else:
                persistencePolicy = lsst.mwi.policy.PolicyPtr()
            persistence = lsst.mwi.persistence.Persistence.getPersistence( \
                    persistencePolicy)

            # Iterate over items in OutputItems policy.
            outputPolicy = self._policy.getPolicy('OutputItems')
            itemNames = outputPolicy.policyNames(True)
            for item in itemNames:

                itemPolicy = outputPolicy.getPolicy(item)

                # Skip the item if it is not required and is not present.
                itemRequired = itemPolicy.exists('Required') and \
                        itemPolicy.getBool('Required')
                try:
                    itemData = clipboard.get(item)
                except KeyError:
                    if itemRequired:
                        raise Runtime, 'Missing output item: ' + item
                    else:
                        continue

                # Add the item name to the additionalData.
                additionalData.deleteAll('itemName', False)
                additionalData.addProperty( \
                        lsst.mwi.data.DataProperty('itemName', item))

                # Add a subproperty for storage locations.
                locProp = lsst.mwi.data.SupportFactory.createPropertyNode( \
                        "StorageLocation")
                additionalData.addProperty(locProp)

                # Get the item's StoragePolicy.
                if itemPolicy.isArray('StoragePolicy'):
                    policyList = itemPolicy.getPolicyArray('StoragePolicy')
                else:
                    policyList = []
                    policyList.append(itemPolicy.getPolicy('StoragePolicy'))

                # Create a list of Storages for the item based on policy.
                storageList = lsst.mwi.persistence.StorageList()
                for policy in policyList:
                    storageName = policy.getString('Storage')
                    location = policy.getString('Location')
                    location = massage(location, additionalData)

                    locProp.deleteAll(storageName)
                    locProp.addProperty( \
                            lsst.mwi.data.DataProperty(storageName, location))

                    logLoc = lsst.mwi.persistence.LogicalLocation(location)
                    storage = persistence.getPersistStorage(storageName, \
                            logLoc)
                    storageList.append(storage)

                # Persist the item.
                if '__deref__' in dir(itemData):
                    # We have a smart pointer, so dereference it.
                    persistence.persist(itemData.__deref__(), storageList, \
                            additionalData)
                else:
                    persistence.persist(itemData, storageList, additionalData)

            # Propagate the clipboard to the output queue.
            self.outputQueue.addDataset(clipboard)


class InputStage (lsst.dps.Stage.Stage):
    """
    A Stage that retrieves data.
    """

    def __init__(self, stageId, stagePolicy):
        """
        Initialize the stage with a policy.
        """


        lsst.dps.Stage.Stage.__init__(self, stageId, stagePolicy)

    def preprocess(self):
        """
        Retrieve the requested data in the master process before any
        (subclass) processing, if desired.
        """

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'preprocess':
            self._input()

    def process(self):
        """
        Retrieve the requested data in the slice processes.
        """

        if self._policy.exists('RunMode') and \
                (self._policy.getString('RunMode') == 'preprocess' or \
                self._policy.getString('RunMode') == 'postprocess'):
            # Nobody has copied the clipboards yet in the worker process
            for i in xrange(self.inputQueue.size()):
                clipboard = self.inputQueue.getNextDataset()
                self.outputQueue.addDataset(clipboard)
            return
        self._input()

    def postprocess(self):
        """
        Retrieve the requested data in the master process after any
        (subclass) processing, if desired.
        """

        if self._policy.exists('RunMode') and \
                self._policy.getString('RunMode') == 'postprocess':
            self._input()

        if not self._policy.exists('RunMode') or \
                (self._policy.getString('RunMode') != 'preprocess' and \
                self._policy.getString('RunMode') != 'postprocess'):
            # Nobody has copied the clipboards yet in the master process
            for i in xrange(self.inputQueue.size()):
                clipboard = self.inputQueue.getNextDataset()
                self.outputQueue.addDataset(clipboard)


    def _input(self):
        """
        Perform the actual retrieval.
        """

        for i in xrange(self.inputQueue.size()):

            clipboard = self.inputQueue.getNextDataset()
            if not self._policy.exists('InputItems'):
                # Propagate the clipboard to the output queue, but otherwise
                # do nothing.
                mylog = Log(Log.defaultLog(), "IOStage")
                mylog.log(Log.WARN, "No InputItems found")
                self.outputQueue.addDataset(clipboard)
                continue

            additionalData = createAdditionalData(self, self._policy, clipboard)

            # Create a persistence object using policy, if present.
            if self._policy.exists('Persistence'):
                persistencePolicy = lsst.mwi.policy.PolicyPtr( \
                        self._policy.getPolicy('Persistence'))
            else:
                persistencePolicy = lsst.mwi.policy.PolicyPtr()
            persistence = lsst.mwi.persistence.Persistence.getPersistence( \
                    persistencePolicy)

            # Iterate over items in InputItems policy.
            inputPolicy = self._policy.getPolicy('InputItems')
            itemNames = inputPolicy.policyNames(True)
            for item in itemNames:

                itemPolicy = inputPolicy.getPolicy(item)
                itemType = itemPolicy.getString('Type')
                pythonType = itemPolicy.getString('PythonType')

                # import this pythonType dynamically 
                pythonTypeTokenList = pythonType.split('.')
                importClassString = pythonTypeTokenList.pop()
                importClassString = importClassString.strip()
                importPackage = ".".join(pythonTypeTokenList)

                print "i importing: importPackage importClassString ", \
                       i, importPackage, importClassString, "\n"

                # For example  importPackage -> lsst.fw.Core.fwLib  
                #              importClassString -> MaskedImageF
                importType = __import__(importPackage, globals(), locals(), \
                                       [importClassString], -1)

                # Add the item name to the additionalData.
                additionalData.deleteAll('itemName', False)
                additionalData.addProperty( \
                        lsst.mwi.data.DataProperty('itemName', item))

                # Get the item's StoragePolicy.
                if itemPolicy.isArray('StoragePolicy'):
                    policyList = itemPolicy.getPolicyArray('StoragePolicy')
                else:
                    policyList = []
                    policyList.append(itemPolicy.getPolicy('StoragePolicy'))

                # Create a list of Storages for the item based on policy.
                storageList = lsst.mwi.persistence.StorageList()
                for policy in policyList:
                    storageName = policy.getString('Storage')
                    location = policy.getString('Location')
                    location = massage(location, additionalData)
                    logLoc = lsst.mwi.persistence.LogicalLocation(location)
                    storage = persistence.getRetrieveStorage(storageName, \
                            logLoc)
                    storageList.append(storage)


                # Retrieve the item.
                itemData = persistence.unsafeRetrieve(itemType, storageList, \
                        additionalData)

                # Cast the SWIGged Persistable to a more useful type.
                pos = pythonType.rfind('.')
                if pos != -1:
                    pythonModule = pythonType[0:pos]
                    exec 'import ' + pythonModule
                exec 'finalItem = ' + pythonType + '.swigConvert(itemData)'

                # Make sure that the useful type owns the pointer, not the
                # original Persistable.
                itemData.this.disown()
                finalItem.this.acquire()

                # Put the item on the clipboard
                clipboard.put(item, finalItem)
            
            # Propagate the clipboard to the output queue.
            self.outputQueue.addDataset(clipboard)
