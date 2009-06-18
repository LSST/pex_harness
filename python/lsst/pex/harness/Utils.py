#!/usr/bin/env python
# -*- python -*-

import lsst.daf.base
import lsst.pex.logging
import lsst.pex.exceptions

def createAdditionalData(stage, stagePolicy, clipboard):
    """
    Extract additionalData values, as specified by policy, from the clipboard.
    Also create predefined keys for runId, sliceId, and universeSize.
    This routine no longer performs slice-to-CCD mappings as of DC3a.
    Instead, they are now performed in the SliceInfoStage.
    """

    additionalData = lsst.daf.base.PropertySet()
    # Parse array of "key=clipboard-key" or
    # "key=clipboard-key.dataproperty-key" mappings
    if stagePolicy.exists('AdditionalData'):
        dataPairs = stagePolicy.getStringArray('AdditionalData')
        for pair in dataPairs:
            (rename, name) = pair.split("=")
            if name.find(".") != -1:
                (clipKey, psKey) = name.split(".", 1)
                cprops = clipboard.get(clipKey)
                if cprops is None:
                    raise RuntimeError, \
                          "Expected data not found on clipboard: "+ clipKey
                additionalData.copy(rename, cprops, psKey)
            else:
                cprops = clipboard.get(name)
                if cprops is None:
                    raise RuntimeError, \
                          "Expected data not found on clipboard: "+ name
                additionalData.set(rename, clipboard.get(name))
            lsst.pex.logging.Trace("pex.harness.Utils.createAdditionalData", 3, \
                    "AdditionalData item: " + pair)

    # Add the predefined runId, sliceId, and universeSize keys

    additionalData.set('runId', stage.getRun())
    additionalData.setInt('sliceId', stage.getRank())
    additionalData.setInt('universeSize', stage.getUniverseSize())

    lsst.pex.logging.Trace("pex.harness.Utils.createAdditionalData", 3, \
            "additionalData:\n" + additionalData.toString(False))

    return additionalData

def propertySetToDict(propertySet):
    """
    Convert a PropertySet to a Python dictionary.
    """
    dict = {}
    for i in propertySet.names():
        dict[i] = propertySet.get(i)
    return dict
