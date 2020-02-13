#ifndef BIGQ_H
#define BIGQ_H

#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

struct WorkerThreadData {
    Pipe* inputPipe;
    Pipe* outputPipe;
    OrderMaker* sortOrder;
    int runLength;

    File bigQFile;
    off_t currentPageIndex;
    int numberOfRuns;

    int currentRunNumberOfRecords;
    Record* currentRunRecords;
    int currentRunRecordIndex;
};

struct PriorityQueueItem {
    Page* page;
    Record* head;
    OrderMaker* sortOrder;

    int currentPageNumber;
    int maxPageNumberOfCurrentRun;
};

struct CustomCompare {
    bool operator() (const PriorityQueueItem& lhs, const PriorityQueueItem& rhs) {
        ComparisonEngine cmp;
        return cmp.Compare(lhs.head, rhs.head, rhs.sortOrder) > 0;
    }
};

void *TPMMS(void *threadData);
    void RunGeneration (struct WorkerThreadData *threadData);
        int AddRecordToCurrentRun (struct WorkerThreadData *workerThreadData, Record* addMe);
        void CreateRun (struct WorkerThreadData *workerThreadData);
            void ApplyQuickSortOnCurrentRun (struct WorkerThreadData *workerThreadData);
                void QuickSort (Record* recordsArray, int low, int high, OrderMaker* sortOrder);
                    int Partition (Record* recordsArray, int low, int high, OrderMaker* sortOrder);
                void SwapRecords (Record* recordsArray, int index1, int index2);
            void WriteCurrentRunOnTheFile (struct WorkerThreadData *workerThreadData);
            void ClearCurrentRunPages (struct WorkerThreadData *workerThreadData);
    void MergeRuns(struct WorkerThreadData *workerThreadData);

class BigQ {
private:
    pthread_t workerThread;

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
