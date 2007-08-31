// -*- lsst-c++ -*-
/** \file Pipeline.h
  *
  * \ingroup dps
  *
  * \brief   Pipeline class manages the operation of a multi-stage parallel pipeline.
  *
  * \author  Greg Daues, NCSA
  */

#ifndef LSST_DPS_PIPELINE_H
#define LSST_DPS_PIPELINE_H

using namespace std;

#include "mpi.h"
#include "Stage.h"
#include "Queue.h"

#include <string>
#include <iostream>
#include <unistd.h>
#include <vector>

namespace lsst {

    namespace dps {

typedef vector<Stage> StageVector;   //  Stage& 
typedef vector<Queue*> QueueVector; 

/**
  * \brief   Pipeline class manages the operation of a multi-stage parallel pipeline.
  *
  *          Pipeline spawns Slice workers and coordinates serial and parallel processing 
  *          between the main thread and the workers by means of MPI communciations.
  *          Pipeline loops over the collection of Stages for processing on Image.
  *          The Pipeline is configured by reading a Policy file.
  */

class Pipeline {
public:
    Pipeline(); // constructor

    ~Pipeline(); // destructor

    void initialize();

    void start();
    void startSlices();  
    void startInitQueue();  
    void startStagesLoop();  
    void invokeProcess(int iStage);

    void shutdown();

    int getNStages();
    Stage getIthStage(int iStage);

private:
    void initializeLogger();
    void initializeMPI();
    void configurePipeline();  
    void initializeQueues();  
    void initializeStages();  


    int _pid;

    MPI_Comm sliceIntercomm;
    StageVector stageVector;
    QueueVector queueVector;
    int nStages;
    int nSlices;
    int bufferSize;
    int mpiError;
    int rank;
    int size;
    int universeSize;
    FILE *Fp_logger;
    char logfile[50];

};

    } // namespace dps 

} // namespace lsst

#endif // LSST_DPS_PIPELINE_H 

