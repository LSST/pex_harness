#!/usr/bin/env python

import lsst.pex.harness.SymLinkStage
import lsst.pex.harness.Clipboard
import lsst.pex.harness.Queue
import lsst.daf.base
import lsst.pex.policy
import lsst.pex.logging

lsst.pex.logging.Trace.setVerbosity("pex.harness.SymLinkStage", 5)

# Get the policies
linkPolicy = lsst.pex.policy.Policy.createPolicy("test/policy/symlink.policy")

# Create the queues
q1 = lsst.pex.harness.Queue.Queue()
q2 = lsst.pex.harness.Queue.Queue()

# Create and initialize the stage
linkStage = lsst.pex.harness.SymLinkStage.SymLinkStage(1, linkPolicy)
linkStage.initialize(q2, q1)
linkStage.setUniverseSize(100)
linkStage.setRank(42)
linkStage.setRun("TEST")

# Create the event DataProperty
event = lsst.daf.base.DataProperty.createPropertyNode("root")
child = lsst.daf.base.DataProperty("visitId", 3840391)
event.addProperty(child)
child = lsst.daf.base.DataProperty("exposureId", 3840391)
event.addProperty(child)
child = lsst.daf.base.DataProperty("filterName", "i")
event.addProperty(child)

# Create the clipboard and put the event on it
clip = lsst.pex.harness.Clipboard.Clipboard()
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
