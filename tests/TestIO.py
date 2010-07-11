#! /usr/bin/env python

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


import lsst.pex.harness.Clipboard
import lsst.pex.harness.Queue
import lsst.pex.harness.IOStage

# Get the policies
outputPolicy = lsst.pex.policy.Policy.createPolicy("tests/policy/output.policy")
inputPolicy = lsst.pex.policy.Policy.createPolicy("tests/policy/input.policy")

# Create the queues
q1 = lsst.pex.harness.Queue.Queue()
q2 = lsst.pex.harness.Queue.Queue()
q3 = lsst.pex.harness.Queue.Queue()
q4 = lsst.pex.harness.Queue.Queue()

sysdata = {}
sysdata["name"] = "testPipeline"
sysdata["rank"] = 0
sysdata["stageId"] = 1
sysdata["universeSize"] = 100
sysdata["runId"] =  "testrun"
eventBrokerHost = "lsst4.ncsa.uiuc.edu"

# Create and initialize the stages
outputStage = lsst.pex.harness.IOStage.OutputStageParallel(outputPolicy, None, eventBrokerHost, sysdata)
outputStage.initialize(q2, q1)

sysdata["rank"] = -1
inputStage = lsst.pex.harness.IOStage.InputStageSerial(inputPolicy, None, eventBrokerHost, sysdata)
inputStage.initialize(q4, q3)
# Note: no direct connection between the stages!

# Create the event PropertySet
event = lsst.daf.base.PropertySet()
event.addString("visitId", "fov391")

# Create the clipboard and put the event on it
clip = lsst.pex.harness.Clipboard.Clipboard()
clip.put("tcsEvent", event)

# Create a PropertySet to persist
# dp = lsst.daf.base.DataProperty.createPropertyNode("sample")
ps = lsst.daf.base.PropertySet()
ps.addString("str", "foo")
ps.addInt("num", 42)
clip.put("theProperty", ps)

# Put the clipboard on the input queue
q1.addDataset(clip)

# Run the output stage like a slice
outputStage.applyProcess()

# Check the output queue: should have everything we put on the input
assert q2.size() == 1
clip2 = q2.getNextDataset()
assert clip2
assert clip2.get("tcsEvent")
assert clip2.get("theProperty")

# Create a brand new clipboard for the input stage
clip3 = lsst.pex.harness.Clipboard.Clipboard()

# Put the event on the clipboard
clip3.put("tcsEvent", event)

# Put the clipboard on the input queue
# No PropertySet on this queue!
q3.addDataset(clip3)

# Run the input stage like a master process
interQueue = inputStage.applyPreprocess()
inputStage.applyPostprocess(interQueue)

# Check the output queue: should have the event -- and now the DataProperty!
assert q4.size() == 1
clip4 = q4.getNextDataset()
assert clip4
assert clip4.get("tcsEvent")
assert clip4.get("theProperty")
ps2 = clip4.get("theProperty")
assert ps2.__class__ == lsst.daf.base.PropertySet

# print dp2.findUnique("str").getValueString()
# print dp2.findUnique("num").getValueInt()
# assert dp2.findUnique("str").getValueString() == \
#         dp.findUnique("str").getValueString()
# assert dp2.findUnique("num").getValueInt() == \
#         dp.findUnique("num").getValueInt()

