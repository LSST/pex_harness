#! /usr/bin/env python

import lsst.pex.harness.Clipboard
from lsst.daf.base import PropertySet

# Create a PropertySet
event = lsst.daf.base.PropertySet()

# Create the clipboard and put the PropertySet on it 
clip = lsst.pex.harness.Clipboard.Clipboard()
clip.put("tcsEvent", event)

assert clip
assert clip.get("tcsEvent1", "missing")

value1 = clip.get("tcsEvent1", "missing")
assert value1 == "missing"

ps2 = clip.get("tcsEvent")
assert ps2.__class__ == lsst.daf.base.PropertySet

try:
    value2 = clip.getItem("tcsEvent1")
except Exception, e:
    assert e.__class__.__name__ == "KeyError"
    # print "Exception class is " + str(e.__class__)
    # print "Exception class name is " + e.__class__.__name__
    # print "Exception " + "args[0] = " + e.args[0] 
    # print "Message is = " + str(e)  

