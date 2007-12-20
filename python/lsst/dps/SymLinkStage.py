#!/usr/bin/env python
# -*- python -*-

"""
SymLinkStage provides a generic mechanism for symbolically linking files or
directories.
"""

import lsst.dps.Stage
import lsst.dps.Utils
import lsst.mwi.utils
import os
from lsst.mwi.logging import Log


class SymLinkStage (lsst.dps.Stage.Stage):
    """
    A Stage that symlinks files or directories.
    """

    def preprocess(self):
        """
        Persist the requested data in the master process before any (subclass)
        processing, if desired.
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        if self._policy.exists('RunMode') and \
            self._policy.getString('RunMode') == 'preprocess':
            self._link()
        
    def process(self):
        """
        Persist the requested data in the slice processes.
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        self.outputQueue.addDataset(self.activeClipboard)
        if self._policy.exists('RunMode') and \
        (self._policy.getString('RunMode') == 'preprocess' or \
        self._policy.getString('RunMode') == 'postprocess'):
            return
        self._link()
        
    def postprocess(self):
        """
        Persist the requested data in the master process after any (subclass)
        processing, if desired.
        """
        if self._policy.exists('RunMode') and \
            self._policy.getString('RunMode') == 'postprocess':
            self._link()
        self.outputQueue.addDataset(self.activeClipboard)

 
    def _link(self):
        """
        Persist the requested data in the master process before any
        (subclass) processing, if desired.
        """
        if not self._policy.exists('Links'):
            mylog = Log(Log.defaultLog(), "dps.SymLinkStage")
            mylog.log(Log.WARN, "No Links found")
            return

        additionalData = lsst.dps.Utils.dataPropertyToDict( \
                lsst.dps.Utils.createAdditionalData(self, \
                    self._policy, self.activeClipboard))

        linkPolicyList = self._policy.getPolicyArray('Links')
        for linkPolicy in linkPolicyList:
            sourcePath = linkPolicy.getString('sourcePath') % additionalData
            destPath = linkPolicy.getString('destPath') % additionalData
            lsst.mwi.utils.Trace("dps.SymLinkStage", 3, \
                    "linking %s to %s" % (sourcePath, destPath))
            parentDir = os.path.dirname(destPath)
            try:
                if parentDir and not os.path.exists(parentDir):
                    os.makedirs(parentDir)
                os.symlink(sourcePath, destPath)
            except OSError, e:
                # ignore "file exists" but re-raise anything else
                if e.errno != 17:
                    raise e
