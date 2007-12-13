#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    activemqBroker = "lsst4.ncsa.uiuc.edu" 

    externalEventTransmitter = events.EventTransmitter(activemqBroker, "triggerAssociationEvent")

    root = datap.SupportFactory.createPropertyNode("root");

    visitId  = datap.DataProperty("visitId", 1)
    FOVRa    = datap.DataProperty("FOVRa", 273.48066298343)
    FOVDec   = datap.DataProperty("FOVDec", -27.125)

    root.addProperty(visitId)
    root.addProperty(FOVRa)
    root.addProperty(FOVDec)

    externalEventTransmitter.publish("eventtype", root)

