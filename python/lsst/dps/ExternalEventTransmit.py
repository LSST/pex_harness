#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "pipedata")

    root = datap.SupportFactory.createPropertyNode("root");
    misc1 = datap.DataProperty("string1","sparam1")
    misc2 = datap.DataProperty("string2","sparam2")
    doub = datap.DataProperty("float", 3.14159265379)

    root.addProperty(doub);
    root.addProperty(misc1);
    root.addProperty(misc2);
    externalEventTransmitter.send("eventtype", root)

