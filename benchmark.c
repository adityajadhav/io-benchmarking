#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "mpi.h"

/*
 * Macro for 'handle_error' helper, in case if someting
 * goes wrong, the library will try to tell you what error is it.
 */
#define MPI_CHECK(fn) { int errcode; errcode = (fn); \
	if (errcode != MPI_SUCCESS) handle_error(errcode, #fn ); }

// Error checking handler
static void handle_error(int errcode, char *str) {
	char msg[MPI_MAX_ERROR_STRING];
	int resultlen;
	MPI_Error_string(errcode, msg, &resultlen);
	fprintf(stderr, "%s: %s\n", str, msg);
	/* Aborting on error might be too aggressive.  If
	 * you're sure you can
	 * continue after an error, comment or remove
	 * the following line */
	MPI_Abort(MPI_COMM_WORLD, 1);
}

// Initialize Variable for
int rank, nprocs;

/* Output file name*/
char *outputFileName;

/* MB per rank, Default is set to 1MB */
int mbPerRank = 1;

/* Output file name*/
char *outputResultFileName;

/* Name given to 0th process */
#define MASTER 0

/* Buffer size for part 1*/
#define BUFFER_SIZE 10

/* Prototype */
void partOneWriteRankToCommonFile();
void partSecondBenchmark();

/* Set the program parameters from the command-line arguments */
void parameters(int argc, char **argv) {
	if (argc == 4) {
		int length = strlen(argv[1]);
		outputFileName = (char*) malloc(length + 1);
		outputFileName = argv[1];

		length = strlen(argv[3]);
		outputResultFileName = (char*) malloc(length + 1);
		outputResultFileName = argv[3];

		mbPerRank = atoi(argv[2]);
		if (MASTER == rank) {
			printf("\nOutput file name for part 1 : %s\n", outputFileName);
			printf("MB per Rank = %dMB\n", mbPerRank);
			printf("Output Result File Name = %s\n", outputResultFileName);

		}
	} else {
		if (MASTER == rank) {
			printf("Usage: %s <output_file_name> <MB_per_rank> <output_results_file_name>\n", argv[0]);
		}
		exit(0);
	}

}

int main(int argc, char **argv) {

// Initialize the MPI environment
	MPI_Init(&argc, &argv);

// Get the rank of the process
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

// Get the number of processes
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

// Process program parameters
	parameters(argc, argv);

	/*
	 * Part 1 : Each MPI process to write its rank to a common
	 * file using the MPI-IO interface.
	 */
	partOneWriteRankToCommonFile();

	/*
	 *  Part 2: The second part is to build a parallel I/O benchmark using MPI-IO.
	 *  In this benchmark, you will open a temporary file, write random data to a file,
	 *  and close the file. Then you will open it again,
	 *  read the data from the file and close it again.
	 *  All operations are performed by each rank to a common file
	 */
	partSecondBenchmark();

// Finalize the MPI environment.
	MPI_Finalize();

	return 0;
}

/*
 * Method Name: partOneWriteRankToCommonFile
 * Purpose: Each process will writes its rank to common file
 */
void partOneWriteRankToCommonFile() {

// variable declaration for buffer, file
	int i, buf[BUFFER_SIZE];
	double stim, wtim;
	MPI_File file;

// Initialize buffer with rank
	for (i = 0; i < BUFFER_SIZE; i++)
		buf[i] = rank;

// File open if file is not exit then it will create a new file
	MPI_CHECK(MPI_File_open(MPI_COMM_WORLD, outputFileName, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &file));

// set view i.e offset to each process
	MPI_CHECK(MPI_File_set_view(file, rank * BUFFER_SIZE * sizeof(int), MPI_INT, MPI_INT, "native", MPI_INFO_NULL));

	MPI_Barrier(MPI_COMM_WORLD);

	if (MASTER == rank)
		stim = MPI_Wtime();
//write buffer into file
	MPI_CHECK(MPI_File_write(file, buf, BUFFER_SIZE, MPI_INT, MPI_STATUS_IGNORE));

	MPI_Barrier(MPI_COMM_WORLD);

	if (MASTER == rank) {
		wtim = MPI_Wtime() - stim;
		printf("Time taken to write into file for part 1 :  %10.5f\n", wtim);
	}

//closing the file
	MPI_CHECK(MPI_File_close(&file));

}

/*
 * Method Name: partSecondBenchmark
 * Purpose: parallel I/O benchmark, Each process will writes and read number of MBs
 * input given by user from the common file
 */
void partSecondBenchmark() {

	MPI_File fhw;

	char *buf;
	double writeStim, writeWtim, readStim, readWtim, overallStim, overallWtim;
	FILE *filePtr;
	overallStim = MPI_Wtime();
	double maxBandwidth;

	int size = ((1024 * 1024)) * mbPerRank;

	//char buf[size];
	buf = (char*) malloc(size);

	/******* write to the file *********/

	// create a file if not exit 
	MPI_CHECK(MPI_File_open(MPI_COMM_WORLD, "temp", MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fhw));
	// set offset
	MPI_CHECK(MPI_File_set_view(fhw, rank * size * sizeof(char), MPI_CHAR, MPI_CHAR, "native", MPI_INFO_NULL));
	


	if (MASTER == rank)
		writeStim = MPI_Wtime();

	MPI_Barrier(MPI_COMM_WORLD);
	// write into file
	MPI_CHECK(MPI_File_write(fhw, buf, size, MPI_CHAR, MPI_STATUS_IGNORE));

	MPI_Barrier(MPI_COMM_WORLD);
	
	if (MASTER == rank)
		writeWtim = MPI_Wtime() - writeStim;

	// close the file
	MPI_CHECK(MPI_File_close(&fhw));

	//MPI_Allreduce(&writeWtim, &new_write_tim, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);



	/******* Read from the file *********/

	// open file in read only mode
	MPI_CHECK(MPI_File_open(MPI_COMM_WORLD, "temp", MPI_MODE_RDONLY, MPI_INFO_NULL, &fhw));

	// set the offset
	MPI_CHECK(MPI_File_set_view(fhw, rank * size * sizeof(char), MPI_CHAR, MPI_CHAR, "native", MPI_INFO_NULL));


	if (MASTER == rank)
		readStim = MPI_Wtime();

	MPI_Barrier(MPI_COMM_WORLD);

	// read from the file
	MPI_CHECK(MPI_File_read(fhw, buf, size, MPI_BYTE, MPI_STATUS_IGNORE));

	MPI_Barrier(MPI_COMM_WORLD);

	if (MASTER == rank)
		readWtim = MPI_Wtime() - readStim;

	// close the file 
	MPI_CHECK(MPI_File_close(&fhw));


	//MPI_Allreduce(&readWtim, &read_tim, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);

	MPI_Barrier(MPI_COMM_WORLD);

	overallWtim = MPI_Wtime() - overallStim;

	if (MASTER == rank) {

		// remove the temporary file 
		remove("temp");

		// open file to write the results 
		filePtr = fopen(outputResultFileName, "w+");
		if (filePtr == NULL) {
			printf("unable to create/override the file");
		}
		fprintf(filePtr, "\nWrite time = %f s.\n", writeWtim);
		fprintf(filePtr, "\nRead time = %f s.\n", readWtim);
		fprintf(filePtr, "\nOverall time = %f s.\n", overallWtim);
		if (writeWtim < readWtim) {
			maxBandwidth = (size * nprocs) / (writeWtim*1024*1024);
			fprintf(filePtr, "\nMaximum Bandwidth = %f MB/s.\n", maxBandwidth);

		} else {
			maxBandwidth = (size * nprocs) / (readWtim*1024*1024);
			fprintf(filePtr, "\nMaximum Bandwidth = %f MB/s.\n", maxBandwidth);

		}
		fclose(filePtr);
	}
}

