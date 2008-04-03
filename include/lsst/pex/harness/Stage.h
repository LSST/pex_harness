// -*- lsst-c++ -*-
/** \file Stage.h
  *
  * \ingroup harness
  *
  * \brief   Stage provides a super class for a particular ApplicationStage to inherit.
  *
  * \author  Greg Daues, NCSA
  */

#ifndef LSST_DPS_STAGE_H
#define LSST_DPS_STAGE_H

using namespace std;

#include "mpi.h"
#include "Queue.h"

#include <string>
#include <iostream>
#include <unistd.h>

namespace lsst {

    namespace pex {

    	namespace harness {

/**
  * \brief   Stage provides a super class for a particular ApplicationStage to inherit.
  *
  *          ApplicationStage's will overwrite the Stage API consisting of
  *              preprocess()
  *              process()
  *              postprocess()
  *          Serial processing will occur within preprocess() and postprocess(), and these 
  *          Stage methods will be invoked from the main Pipeline.  The process() method of 
  *          the Stage will be invoked from the parallel Slice worker.
  */

class Stage {

public:
    Stage();        // constructor
    Stage(int id);  // constructor specifying id 

    ~Stage();       //  destructor

    void initialize(Queue* out, Queue* in);

    void preprocess();
    void process();
    void postprocess();

    void processNext();

    void shutdown();

    void setStageId(int id);
    int  getStageId();

    void setIsSerialInstance(int isSerial);
    int getIsSerialInstance();

private:
    int _stageId;
    int _isSerialInstance;

    Queue* _inputQueue; 
    Queue* _outputQueue; 

    Clipboard activeClipboard;
};

    	} // namespace harness

    } // namespace pex

} // namespace lsst

#endif // LSST_DPS_STAGE_H 

