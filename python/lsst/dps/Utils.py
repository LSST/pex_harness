#!/usr/bin/env python
# -*- python -*-

import lsst.mwi.data
import lsst.mwi.utils

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
    raise Runtime, 'Unknown DataProperty value type in dps.Utils'

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
                dp = clipboard.get(clipKey).findUnique(dpKey)
                data = dp.getValue()
                value = getDPValue(dp)
            else:
                value = clipboard.get(name)
                data = value
            leaf = lsst.mwi.data.DataProperty(rename, data)
            dataProperty.addProperty(leaf)
            lsst.mwi.utils.Trace("dps.Utils.createAdditionalData", 3, \
                    "AdditionalData item: " + pair)

    # Add the predefined runId, sliceId, and universeSize keys

    leaf = lsst.mwi.data.DataProperty('runId', stage.getRun())
    dataProperty.addProperty(leaf)

    leaf = lsst.mwi.data.DataProperty('sliceId', stage.getRank())
    dataProperty.addProperty(leaf)

    ccdId = "%02d" % (stage.getRank() + 1)
    leaf = lsst.mwi.data.DataProperty('ccdId', ccdId)
    dataProperty.addProperty(leaf)

    leaf = lsst.mwi.data.DataProperty('universeSize', stage.getUniverseSize())
    dataProperty.addProperty(leaf)

    lsst.mwi.utils.Trace("dps.Utils.createAdditionalData", 3, \
            "additionalData:\n" + dataProperty.toString('\t', True))

    return dataProperty

def dataPropertyToDict(dataProperty):
    dict = {}
    for i in dataProperty.getChildren():
        dict[i.getName()] = getDPValue(i)
    return dict
