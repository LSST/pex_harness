#!/usr/bin/env python
# -*- python -*-

import lsst.pex.harness.Stage
import lsst.pex.harness.Utils
import lsst.pex.logging
import os
from lsst.pex.logging import Log


class SymLinkStage (lsst.pex.harness.Stage.Stage):
    """
    SymLinkStage provides a generic mechanism for symbolically linking files
    or directories based on path templates from a Policy and data from the
    clipboard or stage information.
    """

    def preprocess(self):
        """
        Perform the link in the master process.  Desirable if the link is to
        be made once per pipeline execution.
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        if self._policy.exists('RunMode') and \
            self._policy.getString('RunMode') == 'preprocess':
            self._link()
        
    def process(self):
        """
        Perform the link in the slice processes.  Necessary if the link to be
        made depends on the slice number.
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
        Perform the link in the master process.  Functionally equivalent to
        performing it in preprocess().
        """
        if self._policy.exists('RunMode') and \
            self._policy.getString('RunMode') == 'postprocess':
            self._link()
        self.outputQueue.addDataset(self.activeClipboard)

 
    def _link(self):
        """
        Link one or more sourcePaths (from policy) to destPaths after
        formatting each with additionalData derived from the clipboard and
        stage information.
        """
        if not self._policy.exists('Links'):
            mylog = Log(Log.defaultLog(), "pex.harness.SymLinkStage")
            mylog.log(Log.WARN, "No Links found")
            return

        additionalData = lsst.pex.harness.Utils.propertySetToDict( \
                lsst.pex.harness.Utils.createAdditionalData(self, \
                    self._policy, self.activeClipboard))

        linkPolicyList = self._policy.getPolicyArray('Links')
        for linkPolicy in linkPolicyList:
            sourcePath = linkPolicy.getString('sourcePath') % additionalData
            destPath = linkPolicy.getString('destPath') % additionalData
            lsst.pex.logging.Trace("pex.harness.SymLinkStage", 3, \
                    "linking %s to %s" % (sourcePath, destPath))
            parentDir = os.path.dirname(destPath)
            if parentDir and not os.path.exists(parentDir):
                os.makedirs(parentDir)
            try:
                os.symlink(sourcePath, destPath)
            except OSError, e:
                # ignore "file exists" but re-raise anything else
                if e.errno != 17:
                    raise e
