#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "triggerMatchMopEvent")

    root = datap.SupportFactory.createPropertyNode("root");

    visitId  = datap.DataProperty("visitId", 1)

    root.addProperty(visitId)

    externalEventTransmitter.publish("eventtype", root)

