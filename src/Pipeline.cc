// -*- lsst-c++ -*-
/** \file Pipeline.cc
  *
  * \ingroup harness
  *
  * \brief   Pipeline class manages the operation of a multi-stage parallel pipeline.
  *
  *          Pipeline spawns Slice workers and coordinates serial and parallel processing
  *          between the main thread and the workers by means of MPI communciations.
  *          Pipeline loops over the collection of Stages for processing on Image.
  *          The Pipeline is configured by reading a Policy file.
  *
  * \author  Greg Daues, NCSA
  */

#include "lsst/pex/harness/Pipeline.h"
#include "lsst/pex/harness/Stage.h"

using namespace lsst::pex::harness;

Pipeline::Pipeline() {
}

Pipeline::~Pipeline() {
}

void Pipeline::initialize() {

    initializeLogger();

    initializeMPI();

    configurePipeline();

    return;
}

void Pipeline::initializeLogger() {

    _pid = getpid();
    return;
}

void Pipeline::initializeMPI() {
  
  /** jmyers:
   * MPI_Init removes MPI-related info (such as ./mpirun, --ncpus=#)
   * from the argc, argv.  Since we don't really have those, 
   * it takes NULL for both values meaning "do nothing."  Sending it
   * bogus numbers and parameters results in Seg Faults (at least using
   * my MPICH).
   **/

  mpiError = MPI_Init (NULL, NULL); 
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Comm_size(MPI_COMM_WORLD, &size); 
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    int flag;
    int *universeSizep;
    mpiError = MPI_Attr_get(MPI_COMM_WORLD, MPI_UNIVERSE_SIZE, &universeSizep, &flag);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }
    universeSize = *universeSizep;

    nSlices = universeSize-1;

    return;
}

void Pipeline::configurePipeline() {
    bufferSize = 256;
    return;
}

int Pipeline::getNStages() {
    return nStages;
}

int Pipeline::getUniverseSize() {
    return universeSize;
}

Stage Pipeline::getIthStage(int iStage) {
    Stage ithStage = stageVector.at(iStage-1);
    return ithStage;

}

void Pipeline::startSlices() {

    int *array_of_errcodes;
    array_of_errcodes = (int *)malloc(4 * sizeof (int));

    int errcodes[nSlices];
    char *myexec  = "runSlice.py";
    char *argv[] = {_policyName, _runId, NULL};

    mpiError = MPI_Comm_spawn(myexec, argv, nSlices, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &sliceIntercomm, errcodes); 

    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    return;
}

void Pipeline::startInitQueue() {

    /*   Make a simple Clipboard  */ 
    Clipboard *board1;
    board1 = new Clipboard();
    bool bool1 = true;

    string *key1;
    string *value1;
    key1 = new string("primary_image");
    value1 = new string("/gpfs_scratch1/daues/LSSTDC2/image1.fits");

    board1->put(*key1, value1, bool1);

    /* Add Clipboard to Queue q1 */
    Queue *q1;
    q1 = queueVector.at(0);
    q1->addDataset(*board1);

    return;
}

void Pipeline::startStagesLoop() {

    for (int iStage = 0; iStage < nStages; iStage++) {
        Stage ithStage = stageVector.at(iStage);
        ithStage.preprocess();
       
        invokeProcess(iStage);  

        ithStage.postprocess();
    } 

    return;
}


void Pipeline::invokeShutdown() {

    char procCommand[bufferSize];

    strcpy(procCommand, "SHUTDOWN");  

    mpiError = MPI_Bcast((void *)procCommand, bufferSize, MPI_CHAR, MPI_ROOT, sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    return;

}

void Pipeline::invokeContinue() {

    char procCommand[bufferSize];

    strcpy(procCommand, "CONTINUE");  

    mpiError = MPI_Bcast((void *)procCommand, bufferSize, MPI_CHAR, MPI_ROOT, sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    return;

}

void Pipeline::invokeProcess(int iStage) {

    char processCommand[nSlices][bufferSize];
    for (int k = 0 ; k < nSlices; k++) {
        strcpy(processCommand[k], "PROCESS");
    }

    char procCommand[bufferSize];

    strcpy(procCommand, "PROCESS");  

    mpiError = MPI_Bcast((void *)procCommand, bufferSize, MPI_CHAR, MPI_ROOT, sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Bcast(&iStage, 1, MPI_INT, MPI_ROOT, sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Barrier(sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    return;
}

void Pipeline::start() {

    startSlices();

    startInitQueue();

    startStagesLoop();

}

void Pipeline::shutdown() {

    MPI_Finalize(); 
    exit(0);

}

void Pipeline::setRunId(char* runId) {
    _runId = runId;
    return;
}

char* Pipeline::getRunId() {
    return _runId;
}

void Pipeline::setPolicyName(char* policyName) {
    _policyName = policyName;
    return;
}

char* Pipeline::getPolicyName() {
    return _policyName;
}


