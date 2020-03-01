#ifndef SORTED_H
#define SORTED_H

#include "GenericDBFile.h"
#include "File.h"
#include "BigQ.h"
#include "Heap.h"
#include "Pipe.h"

struct SortInfo { OrderMaker *myOrder; int runLength; };

class Sorted : public GenericDBFile {

private:
    File* file;
    string fileName;
    bool isFileOpen;

    SortInfo* sortInfo;

    bool currentlyInWriteMode;

    off_t currentlyBeingReadPageNumber;
    Page* readBufferPage;

    int pipeBufferSize;
    Pipe* inputPipe;
    Pipe* outputPipe;

    OrderMaker* queryOrderMaker;
    bool useSameQueryOrderMaker;

    ComparisonEngine cng;

    Schema* customerSchema = new Schema ("catalog", "customer");

    void CreateBigQIfRequired();
        void CreateBigQ();

    void MergeAndCreateNewSortedFileIfRequired();
        void MergeAndCreateNewSortedFile();

    int readRecordFromBuffer(Record &rec);

    int GetNextForSortedFile(Record &fetchme, CNF &cnf, Record &literal);
        int SequentialSearchWithCNF(Record &fetchme, CNF &cnf, Record &literal);
        int SequentialSearchWithQueryAndCNF(Record &fetchme, CNF &cnf, Record &literal);
        int SequentialSearchWithQueryAndCNF(Record &fetchme, CNF &cnf, Record &literal, Record &firstQueryMatchedRecord);
        int SequentialSearchOnPageWithQuery(Page *page, Record* literal, Record* searchedRecord);
    off_t BinarySearch(off_t low, off_t high, Record &literal);
public:
    // constructor and destructor
    explicit Sorted(SortInfo* sortInfo);
    ~Sorted ();

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