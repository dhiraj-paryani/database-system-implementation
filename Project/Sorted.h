#ifndef SORTED_H
#define SORTED_H

#include "GenericDBFile.h"
#include "File.h"
#include "BigQ.h"
#include "Heap.h"
#include "Pipe.h"

struct SortInfo {
    OrderMaker *myOrder;
    int runLength;
};

class Sorted : public GenericDBFile {

private:
    SortInfo* sortInfo;

    int pipeBufferSize;
    Pipe* inputPipe;
    Pipe* outputPipe;

    OrderMaker* queryOrderMaker;
    bool useSameQueryOrderMaker;


    void MergeAndCreateNewSortedFile();

    int GetNextForSortedFile(Record &fetchme, CNF &cnf, Record &literal);
        int CheckForQuery(Record &fetchme, Record &literal);
        bool CheckForCNF(Record &fetchme, CNF &cnf, Record &literal);
        off_t BinarySearch(off_t low, off_t high, Record &literal);
public:
    // constructor and destructor
    explicit Sorted(SortInfo* sortInfo);
    ~Sorted ();

    void SwitchToWriteMode();

    void SwitchToReadMode();

    void AddToDBFile(Record &addme);

    int GetNextFromDBFile(Record &fetchme);

    int GetNextFromDBFile(Record &fetchme, CNF &cnf, Record &literal);
};
#endif