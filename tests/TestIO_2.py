#!/usr/bin/env python

import lsst.pex.policy as pexPolicy
import lsst.pex.harness.simpleStageTester as SST
import lsst.pex.harness.IOStage as IOStage
import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.afw.image as afwImage
from lsst.pex.harness import Dataset

lm = dafBase.PropertySet()
lm.add("input", "tests")
lm.add("output", ".")
dafPersist.LogicalLocation.setLocationMap(lm)
ps = dafBase.PropertySet()
ps.add("field", "D4")
clip0 = {
    'inputDatasets': [
        Dataset('postIsr', visit=707911, snap=0, ccd=13, amp=0),
        Dataset('postIsr', visit=707911, snap=0, ccd=13, amp=1)
    ],
    'root': "raw",
    'ps': ps
}
policy = pexPolicy.Policy.createPolicy("tests/policy/TestIO_2.policy")
t1 = SST.SimpleStageTester(IOStage.InputStage(policy))
clip1 = t1.run(clip0, 0)
el = clip1.get("exposureSet")
assert len(el) == 2
e = afwImage.DecoratedImageU()
assert type(el[0]) == type(e)
assert type(el[1]) == type(e)
assert el[0].getWidth() == 1056
assert el[1].getHeight() == 1153
assert el[0].getMetadata().get("LSSTAMP") == 0
assert el[1].getMetadata().get("LSSTAMP") == 1
