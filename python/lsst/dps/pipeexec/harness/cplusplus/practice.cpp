#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <string>
#include <iostream>




int main(int argc, char ** argv)
{
  MPI::Init(argc, argv);
  MPI::COMM_WORLD.Set_errhandler(MPI::ERRORS_THROW_EXCEPTIONS);
  try
    {
      int worldSize;
      worldSize = MPI::COMM_WORLD.Get_size();
      std::cout << "World size = " << worldSize << std::endl;

      int myRank;
      myRank = MPI::COMM_WORLD.Get_rank();
      std::cout << "My rank = " << myRank << std::endl;

      int universeSize;
      int * universeSizep;

      MPI::COMM_WORLD.Get_attr(MPI::UNIVERSE_SIZE, &universeSizep);
      universeSize = *universeSizep;
      std::cout << "My universeSize = " << universeSize << std::endl;

      MPI::COMM_WORLD.Barrier();

      MPI::Intercomm parentComm;
      parentComm.Get_parent(); // awkward syntax!

      if (parentComm == MPI::COMM_NULL)
	std::cout << "parentComm is null, as expected! " << std::endl;
      else
	std::cout << "parentComm is NOT null! Uh-oh! " << std::endl;
  
      char child_argv[2][256];
      sprintf(child_argv[0], "Hello, world from spawned proc\n");
      child_argv[1][0] = '\0';
      MPI::COMM_WORLD.Spawn((const char*)"echo", (const char**)child_argv, 2, MPI::INFO_NULL, MPI_ROOT);
      /* disconceringly, i don't see the output of the above, but it *does* run... */



      // the following is syntactically correct for C++ but crashes - 
      // i'd assume because there's no one to gather *from*, but actually
      // it seems to be because MPI::ROOT ==  -3? Curious...
      std::cout << "MPI::ROOT = " << MPI_ROOT << ", MPI_ROOT = " << MPI_ROOT << std::endl;
      int bogus;
      MPI::COMM_WORLD.Gather(&bogus, 1, MPI::INT, /*standin for PID*/0, 1, MPI::INT, MPI_ROOT);
      MPI::Finalize();
      std::cout << "Whoohoo, all went well" << std::endl;
    }
  catch (MPI::Exception e)
    {
      std::cerr << "Received MPI Exception Number " << e.Get_error_code() << ", terminating! " << std::endl;
      MPI::Finalize(); 
      return -1;
    }
  return 0;
}
