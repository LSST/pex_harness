#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "assocTriggerEvent")

    root = datap.SupportFactory.createPropertyNode("root");

    fovid   = datap.DataProperty("FOVID",  "2006_10_02-04_13_00")
    dialoc  = datap.DataProperty("DIALOC", "DIASource_database_table")
    dmatch  = datap.DataProperty("DMATCH", 10.0000000)

    root.addProperty(fovid)
    root.addProperty(dialoc)
    root.addProperty(dmatch)

    externalEventTransmitter.publish("eventtype", root)

