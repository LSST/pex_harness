#!/usr/bin/env python
# -*- python -*-

"""
SymLinkStage provides a generic mechanism for symbolically linking files or
directories.
"""

import lsst.dps.Stage
import lsst.mwi.utils
import os
from lsst.mwi.logging import Log

def getDPValue(dp):
    try:
        value = dp.getValueString()
        return value
    except:
        pass
    try:
        value = dp.getValueInt64()
        return value
    except:
        pass
    try:
        value = dp.getValueInt()
        return value
    except:
        pass
    try:
        value = dp.getValueFloat()
        return value
    except:
        pass
    try:
        value = dp.getValueBool()
        return value
    except:
        pass
    raise Runtime, 'Unknown DataProperty value type in SymLinkStage'

def createAdditionalData(stage, stagePolicy, clipboard):
    """
    Extract additionalData values, as specified by policy, from the clipboard.
    """

    additionalData = {}

    # Parse array of "key=clipboard-key" or
    # "key=clipboard-key.dataproperty-key" mappings
    if stagePolicy.exists('AdditionalData'):
        dataPairs = stagePolicy.getStringArray('AdditionalData')
        for pair in dataPairs:
            (rename, name) = pair.split("=")
            if name.find(".") != -1:
                (clipKey, dpKey) = name.split(".", 1)
                dp = clipboard.get(clipKey).findUnique(dpKey)
                value = getDPValue(dp)
            else:
                value = clipboard.get(name)
            additionalData[rename] = value
            lsst.mwi.utils.Trace("dps.SymLinkStage", 3, \
                    "AdditionalData item: " + pair)

    # Add the predefined runId, sliceId, and universeSize keys

    additionalData['runId'] = stage.getRun()
    additionalData['sliceId'] = stage.getRank()
    additionalData['ccdId'] = "%02d" % (stage.getRank() + 1)
    additionalData['universeSize'] = stage.getUniverseSize()

    lsst.mwi.utils.Trace("dps.SymLinkStage", 3, \
            "additionalData:\n" + str(additionalData))

    return additionalData

class SymLinkStage (lsst.dps.Stage.Stage):
    """
    A Stage that symlinks files or directories.
    """

    def preprocess(self):
        """
        Persist the requested data in the master process before any
        (subclass) processing, if desired.
        """
        self.activeClipboard = self.inputQueue.getNextDataset()
        if not self._policy.exists('Links'):
            mylog = Log(Log.defaultLog(), "dps.SymLinkStage")
            mylog.log(Log.WARN, "No Links found")
            return

        additionalData = createAdditionalData(self, self._policy, \
                self.activeClipboard)

        linkPolicyList = self._policy.getPolicyArray('Links')
        for linkPolicy in linkPolicyList:
            sourcePath = linkPolicy.getString('sourcePath') % additionalData
            destPath = linkPolicy.getString('destPath') % additionalData
            lsst.mwi.utils.Trace("dps.SymLinkStage", 3, \
                    "linking %s to %s" % (sourcePath, destPath))
            os.symlink(sourcePath, destPath)
