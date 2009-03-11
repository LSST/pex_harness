#!/usr/bin/env python
# -*- python -*-

import lsst.daf.base
import lsst.pex.logging
import lsst.pex.exceptions

def createAdditionalData(stage, stagePolicy, clipboard):
    """
    Extract additionalData values, as specified by policy, from the clipboard.
    Also create predefined keys for runId, sliceId, ccdId, and universeSize.
    This routine effectively performs slice-to-CCD mappings in DC2.
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
                data = clipboard.get(clipKey).get(psKey)
            else:
                data = clipboard.get(name)
            additionalData.set(rename, data)
            lsst.pex.logging.Trace("pex.harness.Utils.createAdditionalData", 3, \
                    "AdditionalData item: " + pair)

    # Add the predefined runId, sliceId, ccdId, and universeSize keys

    additionalData.set('runId', stage.getRun())
    additionalData.setInt('sliceId', stage.getRank())
    additionalData.setInt('universeSize', stage.getUniverseSize())

    if stagePolicy.exists('CcdFormula'):
        formula = stagePolicy.get('CcdFormula')
        formula = re.sub(r'@slice', r'stage.getRank()', formula)
        ccdId = eval(formula)
    else:
        incr = stagePolicy.get('CcdOffset', 1)
        ccdId = "%03d" % (stage.getRank() + incr)
    additionalData.set('ccdId', ccdId)

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
