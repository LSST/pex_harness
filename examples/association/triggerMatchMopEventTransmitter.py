#! /usr/bin/env python

import lsst.daf.base as datap
import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    activemqBroker = "lsst4.ncsa.uiuc.edu"

    externalEventTransmitter = events.EventTransmitter(activemqBroker, "triggerMatchMopEvent")

    root = datap.DataProperty.createPropertyNode("root");

    visitId  = datap.DataProperty("visitId", 1)

    root.addProperty(visitId)

    externalEventTransmitter.publish("eventtype", root)

