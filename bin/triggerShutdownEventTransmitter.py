#! /usr/bin/env python

import lsst.daf.base as datap
import lsst.ctrlevents as events
import time

if __name__ == "__main__":
    print "starting...\n"

    shutdownTopic = "triggerShutdownEvent"

    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", shutdownTopic )

    root = datap.DataProperty.createPropertyNode("root");

    externalEventTransmitter.publish("eventtype", root)

