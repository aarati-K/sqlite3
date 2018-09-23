#include <iostream>
#include "../sqlite3.h"
#include <cstdlib>
#include <fstream>

#define DB_NAME "/home/aarati/workspace/sqlite/test.db"

using namespace std;

int main() {
	sqlite3* sqldb;
	int rc;
	char* stmt;

	rc = sqlite3_open(DB_NAME, &sqldb);
	if (rc != SQLITE_OK) {
		cout << "Could not open database" << endl;
		exit(1);
	}

	stmt = "CREATE TABLE KV( K INT PRIMARY KEY NOT NULL, V CHAR(100) NOT NULL);";
	rc = sqlite3_exec(sqldb, stmt, 0, 0, 0);
	if (rc != SQLITE_OK) {
		cout << "Failed creating table" << endl;
		goto out;
	}

	stmt = "INSERT INTO KV VALUES (1, 'vjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfh');";
	rc = sqlite3_exec(sqldb, stmt, 0, 0, 0);
	if (rc != SQLITE_OK) {
		cout << "Failed inserting value" << endl;
		goto out;
	}

	out:
		// Close the db and return
		cout << "Success opening database" << endl;
		sqlite3_close(sqldb);

	return 0;
}