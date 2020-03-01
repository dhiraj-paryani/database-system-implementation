#ifndef HEAP_H
#define HEAP_H

#include "GenericDBFile.h"

#include <iostream>

class Heap : public GenericDBFile {
private:
    File file;
    bool isFileOpen;

    off_t currentlyBeingWritenPageNumber;
    off_t currentlyBeingReadPageNumber;

    bool writePerformedAfterOpen;

    Page* readBufferPage;
    Page* writeBufferPage;

    void addRecordToBuffer(Record &rec);
    int readRecordFromBuffer(Record &rec);

public:
    // constructor and destructor
    Heap();
    ~Heap ();

    int Create (const char *f_path);
    int Open (const char *fpath);

    void Add (Record &addme);
    void Load (Schema &myschema, const char *loadpath);

    void MoveFirst ();
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);

    int Close ();
};
#endif