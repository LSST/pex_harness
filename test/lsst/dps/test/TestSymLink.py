#!/usr/bin/env python

import lsst.dps.SymLinkStage
import lsst.dps.Clipboard
import lsst.dps.Queue
import lsst.mwi.data
import lsst.mwi.policy
import lsst.mwi.utils

lsst.mwi.utils.Trace.setVerbosity("dps.SymLinkStage", 5)

# Get the policies
linkPolicy = lsst.mwi.policy.Policy.createPolicy("test/policy/symlink.policy")

# Create the queues
q1 = lsst.dps.Queue.Queue()
q2 = lsst.dps.Queue.Queue()

# Create and initialize the stage
linkStage = lsst.dps.SymLinkStage.SymLinkStage(1, linkPolicy)
linkStage.initialize(q2, q1)
linkStage.setUniverseSize(100)
linkStage.setRank(42)
linkStage.setRun("TEST")

# Create the event DataProperty
event = lsst.mwi.data.SupportFactory.createPropertyNode("root")
child = lsst.mwi.data.DataProperty("visitId", 3840391)
event.addProperty(child)
child = lsst.mwi.data.DataProperty("exposureId", 3840391)
event.addProperty(child)
child = lsst.mwi.data.DataProperty("filterName", "i")
event.addProperty(child)

# Create the clipboard and put the event on it
clip = lsst.dps.Clipboard.Clipboard()
clip.put("triggerVisitEvent", event)

# Put the clipboard on the input queue
q1.addDataset(clip)

# Run the link stage
linkStage.process()

# Check the output queue: should have everything we put on the input
assert q2.size() == 1
clip2 = q2.getNextDataset()
assert clip2
assert clip2.get("triggerVisitEvent")
