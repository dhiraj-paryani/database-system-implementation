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
    char bigQFileName[100];
    int numberOfRuns;

    Page* currentRunPages;
    int currentRunPageNumber;
};

struct PriorityQueueItem {
    Page* page;
    Record* head;

    int currentPageNumber;
    int maxPageNumberOfCurrentRun;
};

struct RecordComparator {
    OrderMaker* sortOrder;

    RecordComparator(OrderMaker* sortorder) {
        this->sortOrder = sortorder;
    }

    bool operator() (Record* lhs, Record* rhs) {
        ComparisonEngine cmp;
        return cmp.Compare(lhs, rhs, this->sortOrder) > 0;
    }

    bool operator() (const PriorityQueueItem& lhs, const PriorityQueueItem& rhs) {
        ComparisonEngine cmp;
        return cmp.Compare(lhs.head, rhs.head, this->sortOrder) > 0;
    }
};


void *TPMMS(void *threadData);
    void InitializeWorkerThreadData(struct WorkerThreadData *threadData);
    void RunGeneration (struct WorkerThreadData *threadData);
    int AddRecordToCurrentRun(struct WorkerThreadData *threadData, Record* nextRecord);
        void CreateRun (struct WorkerThreadData *workerThreadData);
            void SortAndStoreCurrentRun(struct WorkerThreadData *workerThreadData);
                void LoadPriorityQueue(struct WorkerThreadData *workerThreadData,priority_queue<Record*, vector<Record*>, RecordComparator>* pq);
        void ClearCurrentRunPages (struct WorkerThreadData *workerThreadData);
    void MergeRuns(struct WorkerThreadData *workerThreadData);
        void LoadMergeRunPriorityQueue(struct WorkerThreadData *workerThreadData,
                priority_queue<PriorityQueueItem, vector<PriorityQueueItem>, RecordComparator>& pq);
        void LoadOutputPipeWithPriorityQueueData(struct WorkerThreadData *workerThreadData,
                priority_queue<PriorityQueueItem, vector<PriorityQueueItem>, RecordComparator>& pq);
    void CleanUp(struct WorkerThreadData *workerThreadData);

class BigQ {
private:
    pthread_t workerThread;

public:
    BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ ();
};

#endif