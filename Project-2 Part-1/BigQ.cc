#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortOrder, int runlen) {

    // Creating buffer array to store current run's records.
    int currentRunNumberOfRecords = runlen * PAGE_SIZE;
    Record* currentRunRecords = new (std::nothrow) Record[currentRunNumberOfRecords];

    if (currentRunRecords == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    // Initialize worker thread data.
    WorkerThreadData my_data = {.inputPipe = &in,
                                .outputPipe = &out,
                                .sortOrder = &sortOrder,
                                .runLength = runlen,
                                .currentPageIndex = 0,
                                .numberOfRuns = 0,
                                .currentRunNumberOfRecords = currentRunNumberOfRecords,
                                .currentRunRecords = currentRunRecords,
                                .currentRunRecordIndex = 0
                                };

    // Create the worker thread.
    pthread_create(&workerThread, NULL, TPMMS, (void *) &my_data);
    pthread_exit(NULL);
}

BigQ::~BigQ () {
}

void *TPMMS(void *threadData) {
    WorkerThreadData* my_data = (struct WorkerThreadData*) threadData;

    // Create new file to store runs.
    char rpath[100];
    sprintf (rpath, "%d.bin", pthread_self());
    my_data->bigQFile.Open(0, rpath);

    // Phase-1 (Generate sorted runs of runLength pages)
    RunGeneration(my_data);

    // Phase-2 (Merge sorted runs)
    MergeRuns(my_data);

    // Closing and deleting the file.
    my_data->bigQFile.Close();
    remove(rpath);

    // Shut down output pipe.
    my_data->outputPipe->ShutDown();
    delete[] my_data->currentRunRecords;
    pthread_exit(NULL);
}

void RunGeneration(struct WorkerThreadData *threadData) {
    Record* nextRecord = new Record();
    while(threadData->inputPipe->Remove(nextRecord) == 1) {
        if(AddRecordToCurrentRun(threadData, nextRecord) == 0) {
            CreateRun(threadData);
            AddRecordToCurrentRun(threadData, nextRecord);
        }
    }
    if (threadData->currentRunRecordIndex != 0) {
        CreateRun(threadData);
    }
}

int AddRecordToCurrentRun(struct WorkerThreadData *workerThreadData, Record* addMe) {
    if (workerThreadData->currentRunRecordIndex == workerThreadData->currentRunNumberOfRecords) return 0;
    workerThreadData->currentRunRecords[workerThreadData->currentRunRecordIndex].Consume(addMe);
    workerThreadData->currentRunRecordIndex = workerThreadData->currentRunRecordIndex + 1;
    return 1;
}

void CreateRun(struct WorkerThreadData *workerThreadData) {
    ApplyQuickSortOnCurrentRun(workerThreadData);
    WriteCurrentRunOnTheFile(workerThreadData);
    ClearCurrentRunPages(workerThreadData);
}

// Write sort logic here.
void ApplyQuickSortOnCurrentRun(struct WorkerThreadData *workerThreadData) {
    QuickSort(workerThreadData->currentRunRecords, 0, workerThreadData->currentRunRecordIndex - 1, workerThreadData->sortOrder);
}

void WriteCurrentRunOnTheFile(struct WorkerThreadData *workerThreadData) {
    Page* pageBuffer = new Page();
    for (int i=0; i < workerThreadData->currentRunRecordIndex; i++) {
        if(pageBuffer->Append(&workerThreadData->currentRunRecords[i]) == 0) {
            workerThreadData->bigQFile.AddPage(pageBuffer, workerThreadData->currentPageIndex);
            workerThreadData->currentPageIndex += 1;
            pageBuffer->EmptyItOut();
            pageBuffer->Append(&workerThreadData->currentRunRecords[i]);
        }
    }
    workerThreadData->bigQFile.AddPage(pageBuffer, workerThreadData->currentPageIndex);
    delete pageBuffer;
}

void ClearCurrentRunPages(struct WorkerThreadData *workerThreadData) {
    workerThreadData->numberOfRuns++;
    workerThreadData->currentRunRecordIndex = 0;
}

void QuickSort(Record* recordsArray, int low, int high, OrderMaker* sortOrder) {
    if (low < high) {
        // Choosing random pivot.
        // int randomPivotIndex = rand() % (high - low + 1) + low;
        // SwapRecords(recordsArray, high, randomPivotIndex);

        int pivotPosition = Partition(recordsArray, low, high, sortOrder);
        QuickSort(recordsArray, low, pivotPosition-1, sortOrder);
        QuickSort(recordsArray, pivotPosition+1, high, sortOrder);
    }
}

int Partition(Record* recordsArray, int low, int high, OrderMaker* sortOrder) {
    /* ComparisonEngine comp;
    int i = low;
    int j = high - 1;

    while (i < j) {
        while (i < high && comp.Compare(&recordsArray[i], &recordsArray[high], sortOrder) < 1) i++;
        while (j > low && comp.Compare(&recordsArray[j], &recordsArray[high], sortOrder) > 0) j--;
        if (i < j) {
            SwapRecords(recordsArray, i, j);
        }
    }
    SwapRecords(recordsArray, j+1, high);
    return j+1; */

    int i = (low - 1);  // Index of smaller element

    for (int j = low; j <= high- 1; j++)
    {
        // If current element is smaller than the pivot
        ComparisonEngine comp;
        if (comp.Compare(&recordsArray[j], &recordsArray[high], sortOrder) < 1)
        {
            i++;    // increment index of smaller element
            SwapRecords(recordsArray, i, j);
        }
    }
    SwapRecords(recordsArray, i+1, high);
    return (i + 1);
}

void SwapRecords(Record* recordsArray, int index1, int index2) {
    Record* temp = new Record();
    temp->Consume(&recordsArray[index1]);
    recordsArray[index1].Consume(&recordsArray[index2]);
    recordsArray[index2].Consume(temp);
    delete temp;
}

int comparator() {

}

void MergeRuns(struct WorkerThreadData *workerThreadData) {
    ComparisonEngine cmp;

    CustomCompare* abc = new CustomCompare();

    std::priority_queue<PriorityQueueItem, std::vector<PriorityQueueItem>, CustomCompare> pq;

    PriorityQueueItem* priorityQueueItem= new (std::nothrow) PriorityQueueItem[workerThreadData->numberOfRuns];
    for (int i=0; i < workerThreadData->numberOfRuns; i++) {
        priorityQueueItem[i].currentPageNumber = i * workerThreadData->runLength;
        priorityQueueItem[i].maxPageNumberOfCurrentRun =
                std::min((off_t)priorityQueueItem[i].currentPageNumber + workerThreadData->runLength - 1,  workerThreadData->bigQFile.GetLength() - 2);
        priorityQueueItem[i].page =  new Page();
        workerThreadData->bigQFile.GetPage(priorityQueueItem[i].page, priorityQueueItem[i].currentPageNumber);
        priorityQueueItem[i].head = new Record();
        priorityQueueItem[i].page->GetFirst(priorityQueueItem[i].head);
        priorityQueueItem[i].sortOrder = workerThreadData->sortOrder;
        cerr << priorityQueueItem[i].currentPageNumber << " : " << priorityQueueItem[i].maxPageNumberOfCurrentRun;
        pq.push(priorityQueueItem[i]);
    }

    while (!pq.empty()) {
        PriorityQueueItem item = pq.top();
        pq.pop();
        workerThreadData->outputPipe->Insert(item.head);
        if(item.page->GetFirst(item.head) == 0) {
            if (item.currentPageNumber + 1 <= item.maxPageNumberOfCurrentRun) {
                item.currentPageNumber++;
                workerThreadData->bigQFile.GetPage(item.page, item.currentPageNumber);
                item.page->GetFirst(item.head);
                pq.push(item);
            }
        } else {
            pq.push(item);
        }
    }
}