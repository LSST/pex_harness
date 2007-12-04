#! /usr/bin/env python

"""
Test Application Stage 2
"""

from lsst.dps.Stage import Stage


class IPDPStage(Stage):

    #------------------------------------------------------------------------
    def process(self):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python IPDPStage process : _rank %i stageId %d' % (self._rank, self.stageId)
        self.activeClipboard = self.inputQueue.getNextDataset()

        inputImage = self.activeClipboard.get("inputImage"); 

        # inputImageDataPropertyPtrType = lsst.mwi.data.DataPropertyPtrType(inputImage)
        print 'Python IPDPStage process(): _rank %i stageId %i value %s' % (self._rank, self.stageId, inputImage)

        self.outputQueue.addDataset(self.activeClipboard)


class ImageprocStage(Stage):

    #------------------------------------------------------------------------
    def process(self):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python ImageprocStage process : _rank %i stageId %d' % (self._rank, self.stageId)
        self.activeClipboard = self.inputQueue.getNextDataset()


        key = "triggerImageprocEvent"
        inputParamPropertyPtrType = self.activeClipboard.get(key); 

        dataPropertyKeyList = inputParamPropertyPtrType.findNames(r"^.")
        # print dataPropertyKeyList

        for dataPropKey in dataPropertyKeyList:
            print 'Python ipdp.ImageprocStage process() datapropKey', dataPropKey
            dpPtr = inputParamPropertyPtrType.findUnique(dataPropKey)
            if (dataPropKey == "exposureid"):
                self.exposureId = dpPtr.getValueInt()
                print 'Python ipdp.ImageprocStage process() ', key, dataPropKey, self.exposureId
            elif (dataPropKey == "ccdid"):
                self.ccdId = dpPtr.getValueInt()
                print 'Python ipdp.ImageprocStage process() ', key, dataPropKey, self.ccdId


        inputImage = self.activeClipboard.get('InputImage')

        print "-------------------------------------\n"
        print inputImage
        print "inputImage cols=%r, rows=%r" % (inputImage.getCols(), inputImage.getRows())
        # print 'Python ImageprocStage process(): _rank %i stageId %i value %s' % (self._rank, self.stageId, inputImage)
        print "-------------------------------------\n"

        self.activeClipboard.put('OutputImage', inputImage)

        self.outputQueue.addDataset(self.activeClipboard)




