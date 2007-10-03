#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "mops1Event")

    root = datap.SupportFactory.createPropertyNode("root");

    FOVID   = datap.DataProperty("FOVID", "2006_10_02-04_13_00")
    FOVRA   = datap.DataProperty("FOVRA", 350.651000)
    FOVDec  = datap.DataProperty("FOVDec", -45.324000)
    FOVTime = datap.DataProperty("FOVTime", "20061002:04:13:00" )

    root.addProperty(FOVID)
    root.addProperty(FOVRA)
    root.addProperty(FOVDec)
    root.addProperty(FOVTime)

    externalEventTransmitter.publish("eventtype", root)

