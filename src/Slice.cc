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

    return;
}

void Slice::initializeMPI() {

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


    int flag;
    int *universeSizep;
    mpiError = MPI_Attr_get(sliceIntercomm, MPI_UNIVERSE_SIZE, &universeSizep, &flag);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }
    universeSize = *universeSizep;

    return;
}

void Slice::configureSlice() {

    bufferSize = 256;
    return;
}

void Slice::initialize() {

    initializeLogger();

    initializeMPI();

    configureSlice();

    return;
}

void Slice::invokeShutdownTest() {

    char shutdownCommand[bufferSize];

    mpiError = MPI_Bcast(shutdownCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }


    if(strcmp(shutdownCommand, "SHUTDOWN")) {
    }
    else {
        shutdown();
    }

}

void Slice::invokeBcast(int iStage) {

    char runCommand[bufferSize];
    int kStage;


    mpiError = MPI_Bcast(runCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Bcast(&kStage, 1, MPI_INT, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

}

void Slice::invokeBarrier(int iStage) {

    mpiError = MPI_Barrier(sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

}


void Slice::shutdown() {

    MPI_Finalize();
    exit(0);
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



