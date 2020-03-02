#ifndef HEAP_H
#define HEAP_H

#include "GenericDBFile.h"

#include <iostream>

class Heap : public GenericDBFile {
private:

    off_t currentlyBeingWritenPageNumber;
    Page writeBufferPage;

public:
    // constructor and destructor
    Heap();
    ~Heap ();

    void SwitchToWriteMode();

    void SwitchToReadMode();

    void AddToDBFile(Record &addme);

    int GetNextFromDBFile(Record &fetchme);

    int GetNextFromDBFile(Record &fetchme, CNF &cnf, Record &literal);
};
#endif