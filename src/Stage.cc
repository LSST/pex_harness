// -*- lsst-c++ -*-
/** \file Stage.cc
  *
  * \ingroup dps
  *
  * \brief   Stage provides a super class for a particular ApplicationStage to inherit.
  *
  *          ApplicationStage's will overwrite the Stage API consisting of
  *              preprocess()
  *              process()
  *              postprocess()
  *          Serial processing will occur within preprocess() and postprocess(), and these
  *          Stage methods will be invoked from the main Pipeline.  The process() method of
  *          the Stage will be invoked from the parallel Slice worker.
  *
  * \author  Greg Daues, NCSA
  */

#include "lsst/dps/Stage.h"

using namespace lsst::dps;

Stage::Stage() {
}

Stage::Stage(int stageId) {
    _stageId = stageId;
}

Stage::~Stage() {
}

void Stage::initialize(Queue* outputQueue, Queue* inputQueue) {

    _outputQueue = outputQueue;
    _inputQueue  = inputQueue;

    return;
}

void Stage::preprocess() {

    activeClipboard = _inputQueue->getNextDataset();
    KeyVector keyVector = activeClipboard.getKeys();

    return;
}

void Stage::process() {
    return;
}

void Stage::postprocess() {
    _outputQueue->addDataset(activeClipboard);
    return;
}

void Stage::processNext() {
    return;
}

void Stage::shutdown() {
    return;
}

void Stage::setStageId(int stageId) {
    _stageId = stageId;
    return;
}

int Stage::getStageId() {
    return _stageId;
}

void Stage::setIsSerialInstance(int isSerialInstance) {
    _isSerialInstance = isSerialInstance;
    return;
}

int Stage::getIsSerialInstance() {
    return _isSerialInstance;
}

