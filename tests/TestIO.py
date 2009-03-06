#! /usr/bin/env python

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

# Create and initialize the stages
outputStage = lsst.pex.harness.IOStage.OutputStage(3, outputPolicy)
outputStage.initialize(q2, q1)
outputStage.setUniverseSize(100)
inputStage = lsst.pex.harness.IOStage.InputStage(4, inputPolicy)
inputStage.initialize(q4, q3)
inputStage.setUniverseSize(100)
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
outputStage.process()

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
inputStage.preprocess()
inputStage.postprocess()

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

