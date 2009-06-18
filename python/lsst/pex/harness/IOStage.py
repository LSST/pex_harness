#!/usr/bin/env python
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
import sys
import lsst.pex.harness.Stage
import lsst.pex.harness.Utils
import lsst.daf.base
import lsst.daf.persistence
import lsst.pex.policy
import lsst.pex.logging
from lsst.pex.logging import Log
import re

__all__ = ['OutputStage', 'InputStage']

class OutputStage (lsst.pex.harness.Stage.Stage):
    """
    A Stage that persists data.
    """

    def __init__(self, stageId = -1, policy = None):

        lsst.pex.harness.Stage.Stage.__init__(self, stageId, policy)
        self.log = Log(Log.getDefaultLog(), "pex.harness.iostage.output")

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
                self.log.log(Log.WARN, "No OutputItems found")
                self.outputQueue.addDataset(clipboard)
                continue

            additionalData = lsst.pex.harness.Utils.createAdditionalData(self, \
                    self._policy, clipboard)

            # Create a persistence object using policy, if present.
            if self._policy.exists('Persistence'):
                persistencePolicy = lsst.pex.policy.Policy( \
                        self._policy.getPolicy('Persistence'))
            else:
                persistencePolicy = lsst.pex.policy.Policy()
            persistence = lsst.daf.persistence.Persistence.getPersistence( \
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
                        raise RuntimeError, 'Missing output item: ' + item
                    else:
                        continue

                # Add the item name to the additionalData.
                additionalData.set('itemName', item)

                # Get the item's StoragePolicy.
                if itemPolicy.isArray('StoragePolicy'):
                    policyList = itemPolicy.getPolicyArray('StoragePolicy')
                else:
                    policyList = []
                    policyList.append(itemPolicy.getPolicy('StoragePolicy'))

                # Create a list of Storages for the item based on policy.
                storageList = lsst.daf.persistence.StorageList()
                for policy in policyList:
                    storageName = policy.getString('Storage')
                    location = policy.getString('Location')
                    logLoc = lsst.daf.persistence.LogicalLocation(location,
                            additionalData)
                    self.log.log(Log.INFO, "persisting %s as %s" % (item,
                                     logLoc.locString()));

                    additionalData.add('StorageLocation.' + storageName,
                            logLoc.locString())
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


class InputStage (lsst.pex.harness.Stage.Stage):
    """
    A Stage that retrieves data.
    """

    def __init__(self, stageId = -1, policy = None):

        lsst.pex.harness.Stage.Stage.__init__(self, stageId, policy)
        self.log = Log(Log.getDefaultLog(), "pex.harness.iostage.input")

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
                self.log.log(Log.WARN, "No InputItems found")
                self.outputQueue.addDataset(clipboard)
                continue

            additionalData = lsst.pex.harness.Utils.createAdditionalData(self, \
                    self._policy, clipboard)

            # Create a persistence object using policy, if present.
            if self._policy.exists('Persistence'):
                persistencePolicy = lsst.pex.policy.Policy( \
                        self._policy.getPolicy('Persistence'))
            else:
                persistencePolicy = lsst.pex.policy.Policy()
            persistence = lsst.daf.persistence.Persistence.getPersistence( \
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

                print "importing:", item, importPackage, importClassString

                # For example  importPackage -> lsst.afw.Core.afwLib  
                #              importClassString -> MaskedImageF
                importType = __import__(importPackage, globals(), locals(), \
                                       [importClassString], -1)

                # Add the item name to the additionalData.
                additionalData.set('itemName', item)

                # Get the item's StoragePolicy.
                if itemPolicy.isArray('StoragePolicy'):
                    policyList = itemPolicy.getPolicyArray('StoragePolicy')
                else:
                    policyList = []
                    policyList.append(itemPolicy.getPolicy('StoragePolicy'))

                # Create a list of Storages for the item based on policy.
                storageList = lsst.daf.persistence.StorageList()
                for policy in policyList:
                    storageName = policy.getString('Storage')
                    location = policy.getString('Location')
                    logLoc = lsst.daf.persistence.LogicalLocation(location,
                            additionalData)
                    self.log.log(Log.INFO, "loading %s as %s" %
                            (logLoc.locString(), item));
                    storage = persistence.getRetrieveStorage(storageName, \
                            logLoc)
                    storageList.append(storage)

                # Retrieve the item.
                itemData = persistence.unsafeRetrieve(itemType, storageList, \
                        additionalData)

                # Cast the SWIGged Persistable to a more useful type.
                exec 'finalItem = ' + pythonType + '.swigConvert(itemData)'

                # If Persistable and subclasses are NOT wrapped using SWIG_SHARED_PTR,
                # then one must make sure that the wrapper for the useful type owns
                # the pointer (rather than the wrapper for the original Persistable).
                # The following lines accomplish this:
                #itemData.this.disown()
                #finalItem.this.acquire()

                # Put the item on the clipboard
                clipboard.put(item, finalItem)
            
            # Propagate the clipboard to the output queue.
            self.outputQueue.addDataset(clipboard)
