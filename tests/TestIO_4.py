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
import minMapper

lm = dafBase.PropertySet()
lm.add("input", "tests")
lm.add("output", ".")
dafPersist.LogicalLocation.setLocationMap(lm)

clip0 = {
    'jobIdentity': {
        'visit': 85751839, 'snap': 0,
        'raft': "R:2,3", 'sensor': "S:1,1", 'channel': "C:0,0"
    },
    'inputDatasets': [
        Dataset("raw", visit=85751839, snap=0,
            raft="R:2,3", sensor="S:1,1", channel="C:0,0")
    ]
}
policy = pexPolicy.Policy.createPolicy("tests/policy/TestIO_4.policy")
t1 = SST.SimpleStageTester(IOStage.InputStage(policy))
clip1 = t1.run(clip0, 0)
rci = clip1.get("rawCameraImage")
assert rci.getMetadata().get('LSSTAMP') == "R:2,3 S:1,1 C:0,0"
bias = clip1.get("biasExposure")
assert bias.getHeight() == 2001
dark = clip1.get("darkExposure")
assert dark.getWidth() == 513
flat = clip1.get("flatExposure")
assert flat.getHeight() == 2001
fringe = clip1.get("fringeExposure")
assert fringe.getWidth() == 513
