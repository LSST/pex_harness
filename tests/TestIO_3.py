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
    'jobIdentity': {
        'name': "foo",
        'visit': 707911,
        'snap': 0,
        'ccd': 13,
        'filter': "r"
    },
    'root': ".",
    'ps': ps
}
policy = pexPolicy.Policy.createPolicy("tests/policy/TestIO_3.policy")
t1 = SST.SimpleStageTester(IOStage.OutputStage(policy))
clip1 = t1.run(clip0, 0)
ods = clip1.get("outputDatasets")
assert len(ods) == 1
assert ods[0].type == 'postIsr'
assert ods[0].ids['name'] == 'foo'
assert ods[0].ids['visit'] == 707911
assert ods[0].ids['ccd'] == 13
assert ods[0].ids['snap'] == 0
assert ods[0].ids['filter'] == 'r'
assert ods[0].ids['field'] == 'deep'
