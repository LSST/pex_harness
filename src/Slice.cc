// -*- lsst-c++ -*-
/** \file Slice.cc
  *
  * \ingroup dps
  *
  * \brief   Slice represents a single parallel worker program.
  *
  *          Slice executes the loop of Stages for processing a portion of an Image (e.g.,
  *          single ccd or amplifier). The processing is synchonized with serial processing
  *          in the main Pipeline via MPI communications.
  *
  * \author  Greg Daues, NCSA
  */

#include "lsst/dps/Slice.h"

using namespace lsst::dps;

Slice::Slice() {
}

Slice::~Slice() {
}

void Slice::initializeLogger() {

    _pid = getpid();
    sprintf( logfile, "Slice_%d.log", _pid);
    Fp_logger = fopen(logfile, "w");

    fprintf(Fp_logger, "Slice::initializeLogger(): Opened Slice Log %d\n", _pid);
    fflush(Fp_logger); // added by jmyers
    return;
}

void Slice::initializeMPI() {

    /** See comments in Pipeline.cc's void Pipeline::initializeMPI 
     * -- jmyers
     **/

    /* 
    char command[50];
    sprintf( command, "/usr/bin/env > EnvSlice_%d.log", _pid);
    std::system(command); 
    */ 

    fprintf(Fp_logger, "Slice::initializeMPI(): \n");
    fflush(Fp_logger); // added by jmyers

    mpiError = MPI_Init(NULL, NULL);  
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Comm_get_parent(&sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    if (sliceIntercomm == MPI_COMM_NULL) {
        MPI_Finalize();
        exit(1);
    }

    int intercommsize;
    int intercommrank;

    mpiError = MPI_Comm_remote_size(sliceIntercomm, &intercommsize);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    if (intercommsize != 1) {
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Comm_rank(sliceIntercomm, &intercommrank);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    _rank = intercommrank;

    fprintf(Fp_logger, "Slice::initializeMPI() : _rank %d\n", _rank);
    fflush(Fp_logger); // added by jmyers


    int flag;
    int *universeSizep;
    mpiError = MPI_Attr_get(sliceIntercomm, MPI_UNIVERSE_SIZE, &universeSizep, &flag);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }
    universeSize = *universeSizep;

    fprintf(Fp_logger, "Slice::initializeMPI(): _rank usize %d %d \n", _rank, universeSize);
    fflush(Fp_logger); // added by jmyers


    return;
}

void Slice::configureSlice() {

    nStages = 5;
    bufferSize = 256;
    return;
}

void Slice::initializeStages() {

    for (int i = 0;  i < nStages; i++) {
        Stage *s;
        s = new Stage(i);

        stageVector.push_back(*s);
    }

    return;
}

void Slice::initialize() {

    initializeLogger();

    initializeMPI();

    configureSlice();

    initializeStages();

    return;
}

void Slice::invokeBcast(int iStage) {

    char runCommand[bufferSize];
    char bogusCommand[bufferSize];
    int kStage;

    fprintf(Fp_logger, "Slice::invokeBcast() : bufferSize  %d \n", bufferSize);
    fflush(Fp_logger); // added by jmyers

    mpiError = MPI_Bcast(runCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    fprintf(Fp_logger, "Slice::invokeBcast() : runCommand  %s\n", runCommand);
    fflush(Fp_logger); // added by jmyers

    mpiError = MPI_Bcast(&kStage, 1, MPI_INT, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

}

void Slice::invokeBarrier(int iStage) {

    fprintf(Fp_logger, "Slice::invokeBarrier() : Done processing Stage %d . Calling Barrier. \n", iStage);
    fflush(Fp_logger); // added by jmyers

    mpiError = MPI_Barrier(sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    fprintf(Fp_logger, "Slice::invokeBarrier() : Past Barrier. \n");
    fflush(Fp_logger); // added by jmyers
}


void Slice::start() {

    char runCommand[bufferSize];
    char bogusCommand[bufferSize];
    int kStage;

    fprintf(Fp_logger, "Slice::start() : bufferSize  %d \n", bufferSize);
    fprintf(Fp_logger, "Slice::start() : Beginning Stages loop \n");
    fflush(Fp_logger); // added by jmyers

    for (int iStage = 0; iStage < nStages; iStage++) {

        /* 
          mpiError = MPI_Scatter(bogusCommand, bufferSize, MPI_CHAR, runCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);
        */ 

        mpiError = MPI_Bcast(runCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);

        if (mpiError != MPI_SUCCESS){
            MPI_Finalize();
            exit(1);
        }

        fprintf(Fp_logger, "Slice::start() : runCommand  %s\n", runCommand);
	fflush(Fp_logger); // added by jmyers

        mpiError = MPI_Bcast(&kStage, 1, MPI_INT, 0, sliceIntercomm);

        if (mpiError != MPI_SUCCESS){
            MPI_Finalize();
            exit(1);
        }

        fprintf(Fp_logger, "Slice::start() : processing Stage %d (= kStage)\n", kStage);
	fflush(Fp_logger); // added by jmyers

        Stage kthStage = stageVector.at(kStage);
        kthStage.process();

        sleep(5);

        fprintf(Fp_logger, "Slice::start() : Done processing Stage %d . Calling Barrier. \n", kStage);
	fflush(Fp_logger); // added by jmyers

        mpiError = MPI_Barrier(sliceIntercomm);
        if (mpiError != MPI_SUCCESS){
            MPI_Finalize();
            exit(1);
        }

        fprintf(Fp_logger, "Slice::start() : Past Barrier. \n");
	fflush(Fp_logger); // added by jmyers

    }

    fprintf(Fp_logger, "Slice::start() : Past Stages loop. \n");
    fflush(Fp_logger); // added by jmyers

    return;
}

void Slice::shutdown() {

    fclose(Fp_logger);

    MPI_Finalize();

    return;
}

void Slice::setRank(int rank) {
    _rank = rank;
}

int Slice::getRank() {
    return _rank;
}

int Slice::getUniverseSize() {
    return universeSize;
}



