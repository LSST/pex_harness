
/* stageslice */ 
/* Input args
   argv[1] - StageNumber     - the number of this stage within the pipeline
   argv[2] - NumSlices       - the number of slices 
   argv[3] - WorkDir         - full path name to working directory that will be used for log files
   argv[4] - StageWorkerPath - full path name to executable that does the work of the stage
   argv[5] - policyDir       - name of pipeline policy file 
   argv[6] - pipelinePolicy  - name of pipeline policy file 
   argv[7] - diskPolicy      - name of disk locator policy file
 */
 
#ifndef MPICH
#include <lam_config.h>
#endif

#include <mpi.h> 
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <stdlib.h>
#include "nl_log.h"
#include "pipelineHarness.h"
                                                                                

typedef void (*sighandler_t)(int);
sighandler_t signal(int signum, sighandler_t handler);

time_t time(time_t *t);
char *getenv(const char *name);

static int notKilled = 1;


/*--------------------------------------------------------------------------*/
void handleTERM(int signal)
{
  fprintf(stderr,"stageslice_received_sigTerm.");
  fflush(stderr);

  NL_fatal("err", "Received_SIGTERM, Terminating", "PROG=s", "StageSlice");
  notKilled = 0;
  exit(0);
}
/*--------------------------------------------------------------------------*/

int main(int argc, char *argv[]) 
{ 
   int size; 
   MPI_Comm parent, stage_comm; 
   int myid;
   int numprocs, numSlices;
   int i;

   /* copied argv values */
   int stageNum;
   char WD[MAX_PATH_SIZE];          /* working directory for log files, etc */
   char StageWorkerPath[MAX_PATH_SIZE];
   char pipelinePolicy[MAX_PATH_SIZE];
   char diskPolicy[MAX_PATH_SIZE];
   char logfile[MAX_PATH_SIZE];
   char username[512];
   char policyDir[MAX_PATH_SIZE];

   int ret;
   char process_cmd[MAX_PATH_SIZE];
   time_t t;

   /* process argv */
   stageNum = atoi(argv[1]);
   numSlices = atoi(argv[2]);
   strncpy(WD, argv[3], MAX_PATH_SIZE);
   strncpy(StageWorkerPath, argv[4], MAX_PATH_SIZE);
   strncpy(policyDir, argv[5], MAX_PATH_SIZE);
   strncpy(pipelinePolicy, argv[6], MAX_PATH_SIZE);
   strncpy(diskPolicy, argv[7], MAX_PATH_SIZE);

   /* set LSST_POLICY_DIR as environment variable */
   if (setenv(LSST_POLICY_DIR,policyDir,1) < 0) {
     fprintf(stderr,"FATAL: unable to set environment var for LSST_POLICY_DIR:%s\n", policyDir);
     fflush(stderr);
     exit(1);
   }
                                                                                
   /* initialize logging now that $LSST_POLICY_DIR is available */
   int mypid;
   mypid = getpid();
   strcpy(username, getenv("USER"));
   if (username == NULL)
     {
       fprintf(stderr, "manager.c: Got $USER == NULL!\n");
     }

   sprintf(logfile, "/tmp/lsst.harness.stageslice.%i.%s.log", mypid, username);
   NL_logger_module("err1", logfile, NL_LVL_ERROR, NL_TYPE_APP, "");

   if (MPI_Init(&argc, &argv) != MPI_SUCCESS){
     NL_fatal("err1","Failed_MPI_Init", "PROG=s", "StageSlice");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }

   if (MPI_Comm_get_parent(&parent) != MPI_SUCCESS){
     NL_fatal("err1","Failed_MPI_Comm_get_parent", "PROG=s", "StageSlice");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }
   if (parent == MPI_COMM_NULL) {
     NL_fatal("err1","parentIntercomm==MPI_COMM_NULL", "PROG=s", "StageSlice");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }

   if (MPI_Comm_rank(MPI_COMM_WORLD,&myid) != MPI_SUCCESS){
     NL_fatal("err1","Failed_MPI_Comm_ranki within peers","PROG=s","StageSlice");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }

   /* now that have good and unique id - switch to uniquely named log file
      so that falt files are not overwritten
   */
   /*NL_logger_del(); */
   /* jmyers - create a NetLogger logging "module" for debugging info,
      and one for errors.  Modules declared in this way are
      global and cleaned up for us (as well as thread-safe!) */
                                                                                
   /* this module, "dbg", is NL_TYPE_DBG type, so it will
      log:  1) hostname 2) source file 3) line of code
      automatically, and in addition to anything else we add */


   NL_logger_module_const("dbg", logfile, NL_LVL_INFO,
              "STAGE:iSLICE:iMYPID:iHOST:sPROG:s",stageNum,myid,mypid, ipaddr(), "stageslice" );
   NL_logger_module_const("err", logfile, NL_LVL_INFO,
              "STAGE:iSLICE:iMYPID:iHOST:sPROG:s",stageNum,myid,mypid, ipaddr(), "stageslice" );
   NL_info("dbg", "stageslice.main.start","","");

   if (MPI_Comm_remote_size(parent, &size) != MPI_SUCCESS){
     NL_fatal("err","stageslice:_Failed MPI_Comm_remote_size5", "","" );
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }
   if (size != 1) {
     NL_fatal("err","Something_wrong_with_parent", "","");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }

   NL_info("dbg", "stageslice.MPI_Intercomm_merge.start", "", "");
   if (MPI_Intercomm_merge(parent, 1, &stage_comm) != MPI_SUCCESS){
     NL_fatal("err","Failed_MPI_Intercomm_merge", "","");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }


   NL_info("dbg", "stageslice.MPI_Intercomm_merge.end", "", "");

   /* 
    * The manager is represented as the process with rank 0 in (the remote 
    * group of) MPI_COMM_PARENT.  If the stageslices need to communicate among 
    * themselves, they can use MPI_COMM_WORLD. 
    */ 
   if (MPI_Comm_size(MPI_COMM_WORLD,&numprocs) != MPI_SUCCESS){
     NL_fatal("err","stageslice:_Failed_MPI_Comm_size", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }


   NL_info("dbg", "stageslice.getParent_MPIInfo.start", "","");
   int new_remote_size, new_local_size;
   if (MPI_Comm_remote_size(parent, &new_remote_size) != MPI_SUCCESS){
     NL_fatal("err","stageslice:_Failed MPI_Comm_remote_size", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }
   if (MPI_Comm_size(parent, &new_local_size) != MPI_SUCCESS){
     NL_fatal("err","Failed_Parent_MPI_Comm_size", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }
   NL_info("dbg", "stageslice.getParent_MPIInfo.end",
           "LOCAL_SIZE=iPARENT_SIZE=i",
           new_local_size,new_remote_size);
                                                                                

   NL_info("dbg", "stageslice.MPI_Gather.start", "", "");
   /* transmit pid and hostname to manager */
   int bogus;
   int pid = mypid;
   if (MPI_Gather((void *)&pid, 1, MPI_INT, &bogus, 1, MPI_INT, 0, parent) != MPI_SUCCESS) {
     NL_fatal("err","Failed_MPI_Gather", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }

   char hostName[128];
   gethostname(hostName, 128);

   if (MPI_Gather((void *)hostName, 128, MPI_CHAR, &bogus, 128, MPI_CHAR, 0, parent) != MPI_SUCCESS) {
     NL_fatal("err","Failed_MPI_Gather", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
     }

   NL_info("dbg", "stageslice.MPI_Gather.end", "", "");

   /* activate signal handler */
   signal(SIGTERM, handleTERM);

   /* Process the CCDs on this node */
   NL_info("dbg","stageslice.main.Systemsloop.start", "", "");
   int loop = 0;
   while (notKilled) {
     loop++;
     t = time(0);
     NL_info("dbg","stageslice.main.Top.MPI_Barrier.start","ITERATION=i", loop);

     if (MPI_Barrier(stage_comm) != MPI_SUCCESS){
         NL_fatal("err","Failed_TOP_MPI_Barrier", "", "");
         NL_logger_del();
         MPI_Finalize();
         exit(1);
     }
     t = time(0);
     NL_info("dbg","stageslice.main.Top.MPI_Barrier.end","ITERATION=i", loop);

     for ( i = myid; i< numSlices; i += numprocs){
       sprintf(process_cmd, "%s --pipelinePolicy=%s --diskPolicy=%s --stageNum=%d --sliceNum=%d --callerPid=%i", StageWorkerPath, pipelinePolicy, diskPolicy, stageNum,i, mypid);

       NL_info("dbg","stageslice.main.system.start",
               "ITERATION=iCOMMAND=s", loop,process_cmd);
       ret = system(process_cmd);
       NL_info("dbg","stageslice.main.system.end",
               "ITERATION=iRETURN=i", loop,ret);
     }
     
     // not needed when we run with resource consumers: sleep(10);
     
     NL_info("dbg","stageslice.main.Bottom.MPI_Barrier.start",
               "ITERATION=i", loop);
     if (MPI_Barrier(stage_comm) != MPI_SUCCESS){
         NL_fatal("err","Failed_Bottom_MPI_Barrier", "", "");
         NL_logger_del();
         MPI_Finalize();
         exit(1);
     }
     NL_info("dbg","stageslice.main.Bottom.MPI_Barrier.end","ITERATION=i",loop);
   }

   NL_info("dbg","stageslice.main.Systemsloop.end", "", "");
   
   if (MPI_Finalize() != MPI_SUCCESS){
     NL_fatal("err","Failed MPI_Finalize", "", "");
     NL_logger_del();
     exit(1);
     }
   NL_info("dbg","stageslice.main.end", "", "");
   NL_logger_del();
   exit(0); 
}
