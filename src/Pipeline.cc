// -*- lsst-c++ -*-
/** \file Pipeline.cc
  *
  * \ingroup dps
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

#include "lsst/dps/Pipeline.h"
#include "lsst/dps/Stage.h"

using namespace lsst::dps;

Pipeline::Pipeline() {
}

Pipeline::~Pipeline() {
}

void Pipeline::initialize() {

    initializeLogger();

    initializeMPI();

    configurePipeline();

    // initializeQueues();
    // initializeStages();

    return;
}

void Pipeline::initializeLogger() {

    _pid = getpid();
    sprintf( logfile, "Pipeline_%d.log", _pid);
    Fp_logger = fopen(logfile, "w");
    fprintf(Fp_logger, "Pipeline::initialize() : Opened Pipeline Log \n");
    fflush(Fp_logger); // added by jmyers
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

    fprintf(Fp_logger, "Pipeline::initializeMPI(): _rank size %d %d \n", rank, size);
    fprintf(Fp_logger, "Pipeline::initializeMPI(): _rank usize %d %d \n", rank, universeSize);
    fflush(Fp_logger); // added by jmyers

    nSlices = universeSize-1;

    return;
}

void Pipeline::configurePipeline() {
    // nStages = 5;
    // nSlices = 4;
    bufferSize = 256;
    return;
}

void Pipeline::initializeQueues() {

    for (int i = 0;  i < nStages; i++) {
        Queue *q;
        q = new Queue();
        queueVector.push_back(q);
    }
    return;

}

void Pipeline::initializeStages() {

    for (int i = 0;  i < nStages; i++) {
        Stage *s;
        s = new Stage(i);

        Queue *in;
        Queue *out;

        in  = queueVector.at(i);
        if ( i < nStages - 1 ) {
            out = queueVector.at(i+1);
        }
        s->initialize(out, in); 

        stageVector.push_back(*s); 
    }
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

    fprintf(Fp_logger, "Pipeline::startSlices() : Entry \n");
    fflush(Fp_logger); // added by jmyers

    int *array_of_errcodes;
    array_of_errcodes = (int *)malloc(4 * sizeof (int));

    int errcodes[4];
    char *myexec  = "runSlice.py";

    fprintf(Fp_logger, "Pipeline::startSlices() : Spawning \n");
    fflush(Fp_logger); // added by jmyers

    mpiError = MPI_Comm_spawn(myexec, MPI_ARGV_NULL, nSlices, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &sliceIntercomm, errcodes); 

    fprintf(Fp_logger, "Pipeline::startSlices() : Spawned \n");
    fflush(Fp_logger); // added by jmyers

    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }


    return;
}

void Pipeline::startInitQueue() {

    fprintf(Fp_logger, " Pipeline::startInitQueue(): Entry \n");
    fflush(Fp_logger); // added by jmyers

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

    fprintf(Fp_logger, "Pipeline::startStagesLoop(): Entry\n");
    fflush(Fp_logger); // added by jmyers

    for (int iStage = 0; iStage < nStages; iStage++) {
        Stage ithStage = stageVector.at(iStage);
        ithStage.preprocess();
       
        invokeProcess(iStage);  

        ithStage.postprocess();
    } 

    return;
}


void Pipeline::invokeShutdown() {

    fprintf(Fp_logger, "Pipeline::invokeShutdown(): Entry \n");
    fflush(Fp_logger); 

    char procCommand[bufferSize];

    strcpy(procCommand, "SHUTDOWN");  

    fprintf(Fp_logger, "Pipeline::invokeShutdown(): Before Bcast \n");
    fflush(Fp_logger); 

    mpiError = MPI_Bcast((void *)procCommand, bufferSize, MPI_CHAR, MPI_ROOT, sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    fprintf(Fp_logger, "Pipeline::shutdownProcess(): After Bcast \n");
    fflush(Fp_logger); 

    return;

}

void Pipeline::invokeContinue() {

    fprintf(Fp_logger, "Pipeline::invokeContinue(): Entry \n");
    fflush(Fp_logger); 

    char procCommand[bufferSize];

    strcpy(procCommand, "CONTINUE");  

    fprintf(Fp_logger, "Pipeline::invokeContinue(): Before Bcast \n");
    fflush(Fp_logger); 

    mpiError = MPI_Bcast((void *)procCommand, bufferSize, MPI_CHAR, MPI_ROOT, sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    fprintf(Fp_logger, "Pipeline::invokeContinue(): After Bcast \n");
    fflush(Fp_logger); 

    return;

}

void Pipeline::invokeProcess(int iStage) {

    fprintf(Fp_logger, "Pipeline::invokeProcess(): Entry %d\n", iStage);
    fflush(Fp_logger); // added by jmyers

    char processCommand[nSlices][bufferSize];
    /* char bogusCommand[nSlices][bufferSize]; */ 
    for (int k = 0 ; k < nSlices; k++) {
        strcpy(processCommand[k], "PROCESS");
    }

    char procCommand[bufferSize];

    strcpy(procCommand, "PROCESS");  

    fprintf(Fp_logger, "Pipeline::invokeProcess(): Before Bcast %d\n", iStage);
    fflush(Fp_logger); // added by jmyers

    /*
    mpiError = MPI_Scatter((void *)processCommand, bufferSize, MPI_CHAR, bogusCommand, bufferSize, MPI_CHAR, MPI_ROOT, sliceIntercomm);
    */ 

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
    fprintf(Fp_logger, "Pipeline::invokeProcess(): After Bcast %d\n", iStage);
    fflush(Fp_logger); // added by jmyers

    fprintf(Fp_logger, "Pipeline::invokeProcess(): Calling Barrier %d\n", iStage); 
    fflush(Fp_logger); // added by jmyers

    mpiError = MPI_Barrier(sliceIntercomm);
    if (mpiError != MPI_SUCCESS) {
        MPI_Finalize();
        exit(1);
    }

    fprintf(Fp_logger, "Pipeline::invokeProcess(): Past Barrier %d\n", iStage);
    fflush(Fp_logger); // added by jmyers

    sleep(10);
    return;

}

void Pipeline::start() {

    startSlices();

    startInitQueue();

    startStagesLoop();

}

void Pipeline::shutdown() {

    fprintf(Fp_logger, "Pipeline::shutdown(): \n");
    fflush(Fp_logger);
    fclose(Fp_logger);

    MPI_Finalize(); 
    exit(0);

}

