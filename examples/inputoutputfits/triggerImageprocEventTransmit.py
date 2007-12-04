#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "triggerImageprocEvent")

    root = datap.SupportFactory.createPropertyNode("root");

    EXPID = datap.DataProperty("exposureid", 871034)
    CCDID = datap.DataProperty("ccdid", 44)

    root.addProperty(EXPID)
    root.addProperty(CCDID)

    externalEventTransmitter.publish("eventtype", root)

