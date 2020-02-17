#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

class DBFile {
private:
    off_t currentlyBeingWritenPageNumber;
    off_t currentlyBeingReadPageNumber;

    bool writePerformedAfterOpen;
    bool isFileOpen;

    File file;

    Page* readBufferPage;
    Page* writeBufferPage;

    void addRecordToBuffer(Record &rec);
    int readRecordFromBuffer(Record &rec);

public:
    // constructor
	DBFile ();
    ~DBFile ();

    // Create File instance according to the file_type(fType)
    // fpath = file path where file should be created.
    // return 1 on successful creation of the file.
	int Create (const char *fpath, fType file_type, void *startup);

	// Open file if present.
	// Returns 0 if file is not present or file is already open.
	// Returns 1 if the file opened successfully.
	int Open (const char *fpath);

	// Close the currently opened file.
	// Returns 0 if the file is already closed.
	// Returns 1 if the file is closed successfully.
	int Close ();

	// Load the database with the records.
	// First parameter should be Schema of the records.
	// Second parameter should be path from where data to be loaded.
	void Load (Schema &myschema, const char *loadpath);

	// Moves the current read pointer to the first record of the file.
	void MoveFirst ();

	// Add record to the file.
	// After this call, addme will no longer have anything inside of it
	void Add (Record &addme);

	// Get next record from the file.
	// Record will be fetched inside the parameter i.e fetchme.
	int GetNext (Record &fetchme);

	// Get next record from the file which satisfies the passed cnf.
	// Record will be fetched inside the first parameter i.e fetchme.
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif