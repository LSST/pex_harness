#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "IpdpImageEvent")

    root = datap.SupportFactory.createPropertyNode("root");

    VISITID = datap.DataProperty("visitid", "fov391")

    root.addProperty(VISITID)

    externalEventTransmitter.publish("eventtype", root)

