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

