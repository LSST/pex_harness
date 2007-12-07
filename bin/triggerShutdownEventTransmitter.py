#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    shutdownTopic = "triggerShutdownEvent"

    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", shutdownTopic )

    root = datap.SupportFactory.createPropertyNode("root");

    externalEventTransmitter.publish("eventtype", root)

