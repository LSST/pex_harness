// jmyers - oct. 2006

// C++ Harness Notes:

// I'm just going to leave the logging facilities as they were for now.
// the NetLogger C interface is all done with macros, and everything is global.  This
// is rather un-OOP, but myself and Robyn both liked the interface
// that we had.  

// Someday we should really do a logging facility that looks more like the
// Python one.

// We might also try to avoid redundancy eventually and make 
// a standard main_program() that is called by all int main()s in all Pipeline
// processes, since they all look a lot alike.
// For now I think that might get slightly too hairy and might be obfuscation
// in the name of technical correctness (not having redundant code).

// TBD:  I noticed that the command-line option PipelineExe is never used.  I'm 
// actually a little vague on what it should be - at first I assumed this was
// the name of the stageleader executable, but the definition is actually a little vague.
// ergo, none of these pipelineExe variables (in the PipelineOptions or PieplineManager classes)
// are actually set - and in startStageLeaders, we just use a static string. TO DO: do the Right Thing!

// TBD / BUGFIX / HACK WARNING:
// MPICH defines MPI::ROOT and gets it wrong.  LAM doesn't define it at all.
// See below for my quick, dumb fix.

#ifndef MPICH
#include <lam_config.h> 
#endif

#include <mpi.h> 
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h> /* for getopt  and unlink */
#include <libgen.h> /* for basename */
#include <getopt.h>
#include <iostream>
#include "nl_log.h"
#include "LSSTException.hpp"
#include "pipelineHarness.h"


#define NOTHIN_BUT_DEBUG 0


/*and now, a hack to get around the MPI::ROOT problem: */
/* in MPICH: MPI::ROOT is broken.  It should be 0, but it's -3.
   in LAM, MPI::ROOT doesn't even exist. So let's just change 
   the whole thing to MPI_ROOT...   
*/

namespace MPI {
  ROOT = MPI_ROOT;
}

/* MPICH and LAM:  *please* fix this! Then someone at LSST:
   get rid of this stupid hack! */




using namespace std;


/* helper classes */

class PipelineOptions {
public:
  string policyDir;
  string diskPolicy;
  string pipelinePolicy;
  string pipelineExe;
  string inputQ;
  string numStageStr;
  string nodesList;
  int numSlice;
  int numStage;
};


class PipelineOptionReader {
public: 
  PipelineOptions readOptionsFromCommandLine(int argc, char** argv);
};




/* 
   the actual pipeline manager class
*/



class PipelineManager {
public:
  PipelineManager(PipelineOptions);
  ~PipelineManager();
  void runPipeline();
  void terminatePipeline();
private:

  string getStageExec(int stageNum, string WD);
  void retrieveMPIInfo();
  void spawnStageLeaders();
  void collectStageLeaderPIDs();

  // -------variables------------ //
  
  /* input */
  string PWD;
  string policyDir;
  string diskPolicy;
  string pipelinePolicy;
  string pipelineExe;
  string inputQ;
  string numStageStr;
  string nodesList;
  int numSlice;
  int numStage;


  /* for keeping tack of our stageleaders and killing them*/
  string* stageleaderHostList;
  int* stageleaderPidList;
  int stageleaderPidListLen;
  
  /* data from MPI*/
  int world_size;
  int world_rank;
  MPI::Intercomm everyone;
  int universe_size;
};



/* helper functions used by int main are not declared here.  This is for class defs only.*/


