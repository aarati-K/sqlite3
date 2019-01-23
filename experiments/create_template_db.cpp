#include <iostream>
#include "../sqlite3.h"
#include <cstdlib>
#include <fstream>

#define DB_NAME "/home/aarati/workspace/sqlite/32kb_template.db"
#define INPUT_FILE "/mnt/hdd/record/sqlite/initial_keys.txt"

using namespace std;

char* generate_insert_stmt(int i) {
	char* temp = "vjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfhvjdkelanfh";
	return sqlite3_mprintf("INSERT INTO KV VALUES (%d, '%q');", i, temp);
}

int main() {
	ifstream input(INPUT_FILE);
	sqlite3 *sqldb;
	int rc;
	char* stmt;
	int key;

	// Open the input file
	if (!input.is_open()) {
		cout << "Could not open input file" << endl;
		exit(1);
	}

	// Open the DB file
	rc = sqlite3_open(DB_NAME, &sqldb);
	if (rc != SQLITE_OK) {
		cout << "Could not open DB file" << endl;
		exit(1);
	}

	stmt = "CREATE TABLE KV( K INT PRIMARY KEY NOT NULL, V CHAR(100) NOT NULL);";
	rc = sqlite3_exec(sqldb, stmt, 0, 0, 0);
	if (rc != SQLITE_OK) {
		cout << "Table already created" << endl;
		goto out;
	}

	// Begin a transaction
	rc = sqlite3_exec(sqldb, "BEGIN;", 0, 0, 0);
	if (rc != SQLITE_OK) {
		cout << "Begin transaction failed" << endl;
		goto out;
	}

	while (input >> key) {
		stmt = generate_insert_stmt(key);
		rc = sqlite3_exec(sqldb, stmt, 0, 0, 0);
		if (rc != SQLITE_OK) {
			cout << "Inserting key " << key << " failed" << endl;
			goto out;
		}
		sqlite3_free(stmt);
	}

	// End transaction
	rc = sqlite3_exec(sqldb, "COMMIT;", 0, 0, 0);
	if (rc != SQLITE_OK) {
		cout << "Could not commit transaction" << endl;
	}

	out:
		sqlite3_close(sqldb);
		input.close();

	return 0;
}
