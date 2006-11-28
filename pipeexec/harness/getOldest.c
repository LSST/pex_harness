#include <stdio.h>              /* standard input/output routines.    */
#include <dirent.h>             /* readdir(), etc.                    */
#include <sys/types.h>          /* stat(), etc.                       */
#include <sys/stat.h>           /* stat(), etc.                       */
#include <string.h>             /* strstr(), etc.                     */
#include <unistd.h>             /* getcwd(), etc.                     */
#include "pipelineHarness.h"


char oldest_file[MAX_PATH_SIZE];


int get_oldster(char* dir_path,int len, char *nextEntry) {
    struct stat dir_stat;       /* used by stat().        */
    struct dirent *entry;
    time_t oldest_time;
    int firstfile = 1;
    DIR* dir;           /* pointer to the scanned directory. */
    char cwd[MAX_PATH_SIZE+1];	/* current working directory.*/
    fprintf(stderr,"get_oldster: dir_path: %s\n",dir_path);


    /* first, save path of current working directory */
    if (!getcwd(cwd, MAX_PATH_SIZE+1)) {
	perror("get_oldster: getcwd:");
	return(-1);
    }

    /* make sure the given path refers to a directory. */
    if (stat(dir_path, &dir_stat) == -1) {
    perror("get_oldster: stat:");
    return(-1);
    }

    if (!S_ISDIR(dir_stat.st_mode)) {
    fprintf(stderr, "get_oldster: '%s' is not a directory\n", dir_path);
    return(-1);
    }
    
    /* change into the given directory. */
    if (chdir(dir_path) == -1) {
    fprintf(stderr, "get_oldster: Cannot change to directory '%s': ", dir_path);
    perror("get_oldster: ");
    return(-1);
    }

    while ( firstfile ) {
	    /* open the directory for reading */
	    dir = opendir(".");
	    if (!dir) {
	    fprintf(stderr, "get_oldster: Cannot read directory '%s': ", dir_path);
	    perror("get_oldster: ");
            return(-1);
	    }
	
	    /* scan the directory */
	    while ((entry = readdir(dir))) {
	
	        if (stat(entry->d_name, &dir_stat) == -1) {
	        perror("get_oldster: stat:");
	        continue;
	        }

                /* skip the "." and ".." entries, to avoid loops. */
                if (strcmp(entry->d_name, ".") == 0)
                    continue;
                if (strcmp(entry->d_name, "..") == 0)
                    continue;
	
	        if (firstfile) {
	            strncpy(oldest_file,entry->d_name,MAX_PATH_SIZE);
	            oldest_time = dir_stat.st_mtime;
	            firstfile = 0;
	            }
	        else {
	            if (dir_stat.st_mtime < oldest_time) {
	                strncpy(oldest_file,entry->d_name,MAX_PATH_SIZE);
	                oldest_time = dir_stat.st_mtime;
	            }
	        }
	    }
	closedir(dir);
        if (firstfile)
            sleep(1);
        }
    /* finally, restore the original working directory. */
    if (chdir(cwd) == -1) {
	fprintf(stderr, "get_oldster: Cannot chdir back to '%s': ", cwd);
	perror("get_oldster:");
	return(-1);
    }
	
    fprintf(stderr,"get_oldster: Oldest entry in: %s is: %s\n",dir_path,oldest_file);
    fflush(stderr);

    snprintf(nextEntry,len,"%s/%s",dir_path,oldest_file);
    return(0);

}
