#!/usr/bin/env python

import lsst.dps.IOStage
import lsst.dps.Clipboard
import lsst.dps.Queue
import lsst.mwi.data
import lsst.mwi.policy

# Get the policies
outputPolicy = lsst.mwi.policy.Policy.createPolicy("test/policy/output.policy")
inputPolicy = lsst.mwi.policy.Policy.createPolicy("test/policy/input.policy")

# Create the queues
q1 = lsst.dps.Queue.Queue()
q2 = lsst.dps.Queue.Queue()
q3 = lsst.dps.Queue.Queue()
q4 = lsst.dps.Queue.Queue()

# Create and initialize the stages
outputStage = lsst.dps.IOStage.OutputStage(3, outputPolicy)
outputStage.initialize(q2, q1)
outputStage.setUniverseSize(100)
inputStage = lsst.dps.IOStage.InputStage(4, inputPolicy)
inputStage.initialize(q4, q3)
inputStage.setUniverseSize(100)
# Note: no direct connection between the stages!

# Create the event DataProperty
event = lsst.mwi.data.SupportFactory.createPropertyNode("root")
child = lsst.mwi.data.DataProperty("visitId", "fov391")
event.addProperty(child)

# Create the clipboard and put the event on it
clip = lsst.dps.Clipboard.Clipboard()
clip.put("tcsEvent", event)

# Create a DataProperty to persist
dp = lsst.mwi.data.SupportFactory.createPropertyNode("sample")
child = lsst.mwi.data.DataProperty("str", "foo")
dp.addProperty(child)
child = lsst.mwi.data.DataProperty("num", 42)
dp.addProperty(child)
clip.put("theProperty", dp)

# Put the clipboard on the input queue
q1.addDataset(clip)

# Run the output stage like a slice
outputStage.process()

# Check the output queue: should have everything we put on the input
assert q2.size() == 1
clip2 = q2.getNextDataset()
assert clip2
assert clip2.get("tcsEvent")
assert clip2.get("theProperty")

# Create a brand new clipboard for the input stage
clip3 = lsst.dps.Clipboard.Clipboard()

# Put the event on the clipboard
clip3.put("tcsEvent", event)

# Put the clipboard on the input queue
# No DataProperty on this queue!
q3.addDataset(clip3)

# Run the input stage like a master process
inputStage.preprocess()
inputStage.postprocess()

# Check the output queue: should have the event -- and now the DataProperty!
assert q4.size() == 1
clip4 = q4.getNextDataset()
assert clip4
assert clip4.get("tcsEvent")
assert clip4.get("theProperty")
dp2 = clip4.get("theProperty")
assert dp2.__class__ == lsst.mwi.data.DataProperty
print dp2.findUnique("str").getValueString()
print dp2.findUnique("num").getValueInt()
assert dp2.findUnique("str").getValueString() == \
        dp.findUnique("str").getValueString()
assert dp2.findUnique("num").getValueInt() == \
        dp.findUnique("num").getValueInt()
