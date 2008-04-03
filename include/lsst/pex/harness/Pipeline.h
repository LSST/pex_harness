// -*- lsst-c++ -*-
/** \file Pipeline.h
  *
  * \ingroup harness
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

namespace pex {

	namespace harness {

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
    void invokeShutdown();
    void invokeContinue();

    void shutdown();

    int getNStages();
    Stage getIthStage(int iStage);
    int getUniverseSize();

    void setRunId(char* runId);
    char* getRunId();

    void setPolicyName(char* policyName);
    char* getPolicyName();


private:
    void initializeLogger();
    void initializeMPI();
    void configurePipeline();  
    void initializeQueues();  
    void initializeStages();  


    int _pid;
    char* _runId;
    char* _policyName;

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

};

} // namespace harness 

} // namespace pex 

} // namespace lsst

#endif // LSST_DPS_PIPELINE_H 

