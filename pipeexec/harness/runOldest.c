#include <stdio.h>              /* standard input/output routines.    */


extern int get_oldster();

int main(int argc, char* argv[]) {
	

	char oldest_file[257];
	int len;

	oldest_file[257]='\0';
	len = 256;
        if( get_oldster(argv[1],len,&oldest_file) != 0)
		printf("Oldest entry in directory: %s was not found\n",argv[1]);
	else
		printf("Oldest entry in directory: %s is: %s\n",argv[1],oldest_file);

exit(0);

}
