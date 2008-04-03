#!/usr/bin/env python
# -*- python -*-

import lsst.daf.base
import lsst.pex.logging
import lsst.pex.exceptions

def getDPValue(dp):
    """
    Extract a Python value from a DataProperty of unknown type by trying each
    type in turn until we don't get an exception.
    """
    try:
        value = dp.getValueString()
        return value
    except:
        pass
    try:
        value = dp.getValueInt()
        return value
    except:
        pass
    try:
        value = dp.getValueDouble()
        return value
    except:
        pass
    try:
        value = dp.getValueBool()
        return value
    except:
        pass
    try:
        value = dp.getValueInt64()
        return value
    except:
        pass
    try:
        value = dp.getValueFloat()
        return value
    except:
        pass
    raise lsst.pex.exceptions.LsstRuntime, 'Unknown DataProperty value type'

def createAdditionalData(stage, stagePolicy, clipboard):
    """
    Extract additionalData values, as specified by policy, from the clipboard.
    Also create predefined keys for runId, sliceId, ccdId, and universeSize.
    This routine effectively performs slice-to-CCD mappings in DC2.
    """

    dataProperty = \
        lsst.daf.base.DataProperty.createPropertyNode("additionalData")

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
            leaf = lsst.daf.base.DataProperty(rename, data)
            dataProperty.addProperty(leaf)
            lsst.pex.logging.Trace("pex.harness.Utils.createAdditionalData", 3, \
                    "AdditionalData item: " + pair)

    # Add the predefined runId, sliceId, ccdId, and universeSize keys

    leaf = lsst.daf.base.DataProperty('runId', stage.getRun())
    dataProperty.addProperty(leaf)

    leaf = lsst.daf.base.DataProperty('sliceId', stage.getRank())
    dataProperty.addProperty(leaf)

    if stagePolicy.exists('CcdFormula'):
        formula = stagePolicy.get('CcdFormula')
        formula = re.sub(r'@slice', r'stage.getRank()', formula)
        ccdId = eval(formula)
    else:
        incr = stagePolicy.get('CcdOffset', 1)
        ccdId = "%03d" % (stage.getRank() + incr)
    leaf = lsst.daf.base.DataProperty('ccdId', ccdId)
    dataProperty.addProperty(leaf)

    leaf = lsst.daf.base.DataProperty('universeSize', stage.getUniverseSize())
    dataProperty.addProperty(leaf)

    lsst.pex.logging.Trace("pex.harness.Utils.createAdditionalData", 3, \
            "additionalData:\n" + dataProperty.toString('\t', True))

    return dataProperty

def dataPropertyToDict(dataProperty):
    """
    Convert a DataProperty to a Python dictionary.
    """
    dict = {}
    for i in dataProperty.getChildren():
        dict[i.getName()] = getDPValue(i)
    return dict
