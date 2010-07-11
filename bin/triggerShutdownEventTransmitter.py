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


import lsst.daf.base as datap
import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "Issueing shutdown event ...\n"

    shutdownTopic = "triggerShutdownEvent"
    activemqBroker = "lsst8.ncsa.uiuc.edu"

    externalEventTransmitter = events.EventTransmitter(activemqBroker, shutdownTopic )

    root = datap.DataProperty.createPropertyNode("root");

    externalEventTransmitter.publish("eventtype", root)
    print "Sent.\n"

