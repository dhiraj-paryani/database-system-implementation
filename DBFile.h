#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

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
	DBFile ();
    ~DBFile ();

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
