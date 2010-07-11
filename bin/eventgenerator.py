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


# event generator for DC2

import lsst.ctrl.events as events
import lsst.daf.base as datap

import sys

if __name__ == "__main__":
    brokernode = "lsst8.ncsa.uiuc.edu"
    if len(sys.argv) > 1:
        brokernode = sys.argv[1]
    if brokernode.find('.') < 0:
        brokernode += ".ncsa.uiuc.edu"
    print "brokernode =", brokernode
    x = events.EventTransmitter(brokernode, "triggerVisitEvent, mops1Event")

    for image in sys.stdin:
        d = image.split()
        
        root = datap.DataProperty.createPropertyNode("root")
        exposureId = datap.DataProperty("exposureId", int( d[0] ))
        visitId = datap.DataProperty("visitId", int( d[0] ))
        ra = datap.DataProperty("FOVRA", float( d[1] ))
        dec = datap.DataProperty("FOVDec", float( d[2] ))
        filter = datap.DataProperty("filterName", d[3] )
        visitTime = datap.DataProperty("visitTime", float( d[4] ))
        dateObs = datap.DataProperty("dateObs", d[5] )
        airMass = datap.DataProperty("airMass", float( d[6] ))
        exposureTime = datap.DataProperty("exposureTime", float( d[7] ))
        equinox = datap.DataProperty("equinox", float( d[8] ))

        root.addProperty(exposureId)
        root.addProperty(visitId)
        root.addProperty(ra)
        root.addProperty(dec)
        root.addProperty(filter)
        root.addProperty(visitTime)
        root.addProperty(dateObs)
        root.addProperty(airMass)
        root.addProperty(exposureTime)
        root.addProperty(equinox)

        x.publish("log", root)

