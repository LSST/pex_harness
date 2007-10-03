// -*- lsst-c++ -*-
/** \file Slice.h
  *
  * \ingroup dps
  *
  * \brief   Slice represents a single parallel worker program.  
  *
  * \author  Greg Daues, NCSA
  */

#ifndef LSST_DPS_SLICE_H
#define LSST_DPS_SLICE_H

using namespace std;

#include "mpi.h"
#include "Stage.h"

#include <string>
#include <iostream>
#include <unistd.h>
#include <vector>

namespace lsst {

    namespace dps {

typedef vector<Stage> StageVector;

/**
  * \brief   Slice represents a single parallel worker program.  
  *
  *          Slice executes the loop of Stages for processing a portion of an Image (e.g.,
  *          single ccd or amplifier). The processing is synchonized with serial processing 
  *          in the main Pipeline via MPI communications.    
  */

class Slice {

public:
    Slice(); // constructor

    ~Slice(); // destructor

    void initialize();
    void start();
    void invokeBcast(int iStage);
    void invokeBarrier(int iStage);
    void shutdown();
    void setRank(int rank);
    int getRank();
    int getUniverseSize();


private:
    void initializeLogger();
    void initializeMPI();
    void configureSlice();
    void initializeStages();

    int _pid;
    int _rank;

    MPI_Comm sliceIntercomm;
    StageVector stageVector;
    int mpiError;
    int nStages;
    int universeSize;
    int bufferSize;
    FILE *Fp_logger;
    char logfile[50];

};

    } // namespace dps

} // namespace lsst

#endif // LSST_DPS_SLICE_H

