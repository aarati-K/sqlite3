#include <iostream>
#include <cstdlib>
#include <fstream>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <ctime>
#include <string.h>

using namespace std;

#define KB 1024L
#define BT_PGSIZE (64*KB)
#define MB (KB*KB)
#define READ_CHUNK_SIZE (2*MB) // Same as the write chunk size
#define READ_SIZE_IN_PGS (READ_CHUNK_SIZE/BT_PGSIZE)
#define WRITE_OFFSET (32*KB)

#define INFILE "/home/aarati/workspace/sqlite/64kb_template.db"
#define OUTFILE "/home/aarati/workspace/sqlite/64kb_good_layout.db"

long getTimeDiff(struct timeval startTime, struct timeval endTime) {
    return (long)((endTime.tv_sec - startTime.tv_sec)*1000000 +
        (endTime.tv_usec - startTime.tv_usec));
}

int main() {
	cout << "Generating good layout for 32KB page size not implemented" << endl;
	return;

	// void* wbuf;
	// char *offset_buf, *rbuf;
	// int infd, outfd;
	// long filesize;
	// int num_bt_pgs, num_write_chunks;
	// int rc, j;
	// float read_size, i;
	// struct stat sb;
	// struct timeval startTime, endTime;
	// long timeTaken;

	// infd = open(INFILE, O_RDONLY);
	// if (infd < 0) {
	// 	cout << "Could not open in file" << endl;
	// 	exit(1);
	// }

	// outfd = open(OUTFILE, O_RDWR | O_DIRECT | O_SYNC);
	// if (outfd < 0) {
	// 	cout << "Could not open out file" << endl;
	// 	exit(1);
	// }

	// if (posix_memalign(&wbuf, 4*KB, READ_CHUNK_SIZE) != 0) {
 //        cout << "posix_memalign failed" << endl;
 //        exit(2);
 //    }

 //    rbuf = (char*)malloc(READ_CHUNK_SIZE);
 //    offset_buf = (char*)malloc(WRITE_OFFSET);
 //    if (!rbuf || !offset_buf) {
 //    	cout << "mem error" << endl;
 //    	exit(1);
 //    }

 //    rc = stat(INFILE, &sb);
	// filesize = (long)sb.st_size;
	// cout << "File size: " << filesize << endl;
	// cout << "Is multiple of BT_PGSIZE? " << ((filesize % BT_PGSIZE) == 0) << endl;

	// num_bt_pgs = filesize/BT_PGSIZE;
	// cout << "num_bt_pgs: " << num_bt_pgs << endl;

	// i = (float)num_bt_pgs;
	// timeTaken = 0;

	// // Add padding of WRITE_OFFSET size at the start of the file
	// memset(offset_buf, '0', WRITE_OFFSET);
	// read_size = READ_CHUNK_SIZE - WRITE_OFFSET;
	// read(infd, rbuf, int(read_size));
	// memcpy(wbuf, offset_buf, WRITE_OFFSET);
	// memcpy(wbuf + WRITE_OFFSET, rbuf, int(read_size));

	// gettimeofday(&startTime, NULL);
	// write(outfd, wbuf, READ_CHUNK_SIZE);
	// fsync(outfd);
	// gettimeofday(&endTime, NULL);
	// timeTaken += getTimeDiff(startTime, endTime);

	// i -= float(read_size)/BT_PGSIZE;
	// cout << "i before start of loop: " << endl; // sanity check
	// while (i > 0) {
	// 	if (i-READ_SIZE_IN_PGS >= 0) {
	// 		read_size = READ_SIZE_IN_PGS;
	// 	} else {
	// 		read_size = i;
	// 	}
	// 	i -= read_size;
	// 	read(infd, (char*)wbuf, int(read_size*BT_PGSIZE));

	// 	gettimeofday(&startTime, NULL);
	// 	write(outfd, (char*)wbuf, int(read_size*BT_PGSIZE));
	// 	fsync(outfd);
	// 	gettimeofday(&endTime, NULL);

	// 	timeTaken += getTimeDiff(startTime, endTime);
	// }
	// cout << "File creation time with bad layout " << timeTaken << endl;

	// close(infd);
	// close(outfd);
	// free(wbuf);
	// free(rbuf);
	// free(offset_buf);
	// return 0;
}
