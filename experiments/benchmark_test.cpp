#include <iostream>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <math.h>
#include <ctime>
#include <vector>
#include <fstream>
#include "../sqlite3.h"

using namespace std;

#define DB_FILE "/home/aarati/workspace/sqlite/64kb_bad_layout.db"
#define INPUT_FILE "/mnt/hdd/record/sqlite/initial_keys.txt"

#define NUM_ITERATIONS 100
#define NUM_INSERTIONS_PER_ITERATION 50000
#define NUM_FETCH_PER_ITERATION 50000
#define KEY_BASE 10000000

// Change both of these together
#define NUM_INTERATIONS_PER_TRANSACTION 1
#define OUTPUT_FILE "/mnt/hdd/record/sqlite3/latency_64kb_bad_1.txt"

char* generate_insert_stmt(int i) {
	char* temp = "vjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfh";
	return sqlite3_mprintf(0, "INSERT INTO KV VALUES (%d, '%q');", i, temp);
}

char* generate_select_stmt(int i) {
	// char* temp = "vjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfh";
	// return sqlite3_mprintf(0, "INSERT INTO KV VALUES (%d, '%q');", i, temp);
	return sqlite3_mprintf(0, "SELECT V FROM KV WHERE K = %d;", i);
}

long getTimeDiff(struct timeval startTime, struct timeval endTime) {
    return (long)((endTime.tv_sec - startTime.tv_sec)*1000000 +
        (endTime.tv_usec - startTime.tv_usec));
}

int main() {
	sqlite3 *sqldb;
	ifstream input(INPUT_FILE);
	ofstream output(OUTPUT_FILE);
	unsigned int key;
	char* stmt;
	int rc;
	int insert_count;
	struct timeval startTime, endTime;
	int i, j, k;
	long time_taken;
	vector<int> keys_to_insert;
	vector<int> keys_to_fetch;
	vector<int> keys_present;
	vector<int>::iterator it;

	if (!input.is_open() || !output.is_open()) {
		cout << "Could not open input/output file" << endl;
		exit(1);
	}

	rc = sqlite3_open(DB_FILE, &sqldb);
	if (rc != SQLITE_OK) {
		cout << "Failed opening sqldb" << endl;
		goto close_files;
	}

	// Common seed
	srand(0);

	// initialize keys_present
	for (i=0; i<KEY_BASE; i++) {
		keys_present.push_back(i);
	}

	for(i=0; i<NUM_ITERATIONS; i++) {
		for (j=0; j<NUM_INSERTIONS_PER_ITERATION; j++) {
			input >> key;
			keys_to_insert.push_back(key + KEY_BASE);
		}

		// Start inserting keys
		time_taken = 0;
		insert_count = 0;
		for (it=keys_to_insert.begin(); it!=keys_to_insert.end(); it++) {
			stmt = generate_insert_stmt((*it));

			gettimeofday(&startTime, NULL);
			if (insert_count == 0) {
				// Begin transaction
				sqlite3_exec(sqldb, "BEGIN;", 0, 0, 0);
				if (rc != SQLITE_OK) {
					cout << "Failed begin transaction iteration: " << i << endl;
					goto out; 
				}
			}

			rc = sqlite3_exec(sqldb, stmt, 0, 0, 0);
			insert_count += 1;
			if (insert_count == NUM_INTERATIONS_PER_TRANSACTION) {
				insert_count = 0;
				rc = sqlite3_exec(sqldb, "COMMIT;", 0, 0, 0);
				if (rc != SQLITE_OK) {
					cout << "Failed commit transaction iteration: " << i << endl;
					goto out; 
				}
			}
			gettimeofday(&endTime, NULL);

			if (rc != SQLITE_OK) {
				cout << "Failed inserting key " << (*it) << endl;
				goto out;
			}
			time_taken += getTimeDiff(startTime, endTime);
			sqlite3_free(stmt);
		}

		output << time_taken << endl;

		keys_present.insert(keys_present.end(), keys_to_insert.begin(), keys_to_insert.end());
		keys_to_insert.clear();

		// Generate random keys to fetch
		for (j=0; j<NUM_FETCH_PER_ITERATION; j++) {
			key = keys_present[rand() % keys_present.size()];
			keys_to_fetch.push_back(key);
		}

		time_taken = 0;
		for (it=keys_to_fetch.begin(); it!=keys_to_fetch.end(); it++) {
			stmt = generate_select_stmt(*it);

			gettimeofday(&startTime, NULL);
			rc = sqlite3_exec(sqldb, stmt, 0, 0, 0);
			gettimeofday(&endTime, NULL);

			if (rc != SQLITE_OK) {
				cout << "Failed fetching key " << (*it) << endl;
			}
			time_taken += getTimeDiff(startTime, endTime);
			sqlite3_free(stmt);
		}

		output << time_taken;
		keys_to_fetch.clear();

	}

	out:
		sqlite3_close(sqldb);

	close_files:
		input.close();
		output.close();

	return 0;
}