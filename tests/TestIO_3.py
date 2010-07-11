#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#


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
