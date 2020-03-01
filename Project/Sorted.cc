#include "Sorted.h"
#include "GenericDBFile.h"


Sorted :: Sorted(SortInfo* sortInfo) {
    file = new File();

    this->currentlyInWriteMode = false;
    this->sortInfo = sortInfo;

    isFileOpen = false;

    this->currentlyBeingReadPageNumber = 0;
    readBufferPage = new Page();
    pipeBufferSize = 100;

    queryOrderMaker = new OrderMaker();
    useSameQueryOrderMaker = false;
}

Sorted :: ~Sorted() {
}

int Sorted::Create (const char *f_path) {
    int createStatus = file->Open(0, const_cast<char *>(f_path));
    isFileOpen = createStatus;
    return createStatus;
}

int Sorted::Open (const char *f_path) {
    fileName = f_path;
    file->Open(1, const_cast<char *>(f_path));
    int status = file->Open(1, const_cast<char *>(f_path));
    isFileOpen = status;
    this->MoveFirst();
    return status;
}

void Sorted::Add (Record &rec) {
    CreateBigQIfRequired();

    inputPipe->Insert(&rec);
}

void Sorted::Load (Schema &f_schema, const char *loadpath) {
    CreateBigQIfRequired();

    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    while (temp.SuckNextRecord (&f_schema, tableFile)) {
        inputPipe->Insert(&temp);
    }
}

void Sorted::MoveFirst() {
    if (!isFileOpen) {
        cerr << "BAD: File is not open!\n";
        exit(1);
    }
    useSameQueryOrderMaker = false;
    readBufferPage->EmptyItOut();
    this->currentlyBeingReadPageNumber = 0;
    if (file->GetLength() - 2 >= this->currentlyBeingReadPageNumber) {
        file->GetPage(readBufferPage, this->currentlyBeingReadPageNumber);
    }
}

int Sorted::GetNext (Record &fetchme) {
    MergeAndCreateNewSortedFileIfRequired();
    return readRecordFromBuffer(fetchme);
}

int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    MergeAndCreateNewSortedFileIfRequired();
    if (!useSameQueryOrderMaker) {
        cnf.GetCommonSortOrder(*this->sortInfo->myOrder, *queryOrderMaker);
    }
    return GetNextForSortedFile(fetchme, cnf, literal);
}

int Sorted::Close () {
    MergeAndCreateNewSortedFileIfRequired();
    return 0;
}

void Sorted::CreateBigQIfRequired() {
    if(!currentlyInWriteMode) {
        CreateBigQ();
        currentlyInWriteMode = true;
    }
}

void Sorted::CreateBigQ() {
    inputPipe = new Pipe(pipeBufferSize);
    outputPipe = new Pipe(pipeBufferSize);
    BigQ(*inputPipe, *outputPipe, *this->sortInfo->myOrder, this->sortInfo->runLength);
}

void Sorted::MergeAndCreateNewSortedFileIfRequired() {
    if(currentlyInWriteMode) {
        MergeAndCreateNewSortedFile();
        delete inputPipe;
        delete outputPipe;
    }
    currentlyInWriteMode = false;
}

void Sorted::MergeAndCreateNewSortedFile() {
    // Shut down input pipe of BigQ so that BigQ gives sorted records in output pipe.
    inputPipe->ShutDown();
    file->Close();

    // Creating temp file.
    string tempFileName = fileName + ".temp";

    // Reading currentFile as heap file.
    Heap* currentFile = new Heap();
    currentFile->Open(const_cast<char*>(fileName.c_str()));
    currentFile->MoveFirst();

    Record currentFileRecord;
    bool currentFileNotFullyConsumed = currentFile->GetNext(currentFileRecord);

    // Writing sorted records by using heap file.
    Heap* newFile = new Heap();
    newFile->Create(const_cast<char*>(tempFileName.c_str()));

    // Output pipe variables.
    Record outputPipeRecord;
    bool outputPipeNotFullyConsumed = outputPipe->Remove(&outputPipeRecord);

    ComparisonEngine cmp;
    // If either of the stream is fully consumed - Break;
    while(outputPipeNotFullyConsumed && currentFileNotFullyConsumed) {
        // If currentFileRecord is less
        if (cmp.Compare(&currentFileRecord, &outputPipeRecord, this->sortInfo->myOrder) < 1) {
            newFile->Add(currentFileRecord);
            currentFileNotFullyConsumed = currentFile->GetNext(currentFileRecord);
        }
        // If BigQ output pipe's record is less.
        else {
            newFile->Add(outputPipeRecord);
            outputPipeNotFullyConsumed = outputPipe->Remove(&outputPipeRecord);
        }
    }
    // If output pipe is not fully consumed.
    while(outputPipeNotFullyConsumed) {
        newFile->Add(outputPipeRecord);
        outputPipeNotFullyConsumed = outputPipe->Remove(&outputPipeRecord);
    }
    // If current file records are not fully consumed.
    while(currentFileNotFullyConsumed) {
        newFile->Add(currentFileRecord);
        currentFileNotFullyConsumed = currentFile->GetNext(currentFileRecord);
    }

    // Close both files
    newFile->Close();
    currentFile->Close();

    // remove the old file.
    remove(const_cast<char*>(fileName.c_str()));

    // rename the file name.
    rename(const_cast<char*>(tempFileName.c_str()), const_cast<char*>(fileName.c_str()));
    file->Open(1, const_cast<char*>(fileName.c_str()));
}

int Sorted::readRecordFromBuffer(Record &rec) {
    if (!readBufferPage->GetFirst(&rec)) {
        ++currentlyBeingReadPageNumber;
        if (file->GetLength() - 2 < currentlyBeingReadPageNumber) {
            return 0;
        }
        file->GetPage(readBufferPage, currentlyBeingReadPageNumber);
        this->readRecordFromBuffer(rec);
    }
    return 1;
}

int Sorted::GetNextForSortedFile(Record &fetchme, CNF &cnf, Record &literal) {
    if (queryOrderMaker->isEmpty()) {
        return SequentialSearchWithCNF(fetchme, cnf, literal);
    }

    if (useSameQueryOrderMaker) {
        return SequentialSearchWithQueryAndCNF(fetchme, cnf, literal);
    }

    useSameQueryOrderMaker = true;
    Record firstQueryMatchedRecord;
    int comparisonValue = SequentialSearchOnPageWithQuery(readBufferPage, &literal, &firstQueryMatchedRecord);
    if(comparisonValue == 0) {
        return SequentialSearchWithQueryAndCNF(fetchme, cnf, literal, firstQueryMatchedRecord);
    } else if (comparisonValue == -1) {
        return 0;
    }

    // Applying Binary Search to get a page.
    // where the first record of the page is less than the literal
    // and the next page is greater than or equal to the literal.
    off_t searchedPageNumber = BinarySearch(currentlyBeingReadPageNumber + 1, file->GetLength() - 2, literal);
    currentlyBeingReadPageNumber = searchedPageNumber;
    file->GetPage(readBufferPage, searchedPageNumber);

    // Try to match query on 2 pages, we are sure we will get result.
    // Because first record of (searchedPageNumber + 1)'s page will contain record which is greater than or equal to the query.
    comparisonValue = SequentialSearchOnPageWithQuery(readBufferPage, &literal, &firstQueryMatchedRecord);
    if(comparisonValue == 0) {
        return SequentialSearchWithQueryAndCNF(fetchme, cnf, literal, firstQueryMatchedRecord);
    } else if (comparisonValue == -1) {
        return 0;
    }

    file->GetPage(readBufferPage, ++currentlyBeingReadPageNumber);
    comparisonValue = SequentialSearchOnPageWithQuery(readBufferPage, &literal, &firstQueryMatchedRecord);
    if(comparisonValue == 0) {
        return SequentialSearchWithQueryAndCNF(fetchme, cnf, literal, firstQueryMatchedRecord);
    } else if (comparisonValue == -1) {
        return 0;
    }

    return 0;
}

int Sorted::SequentialSearchOnPageWithQuery(Page *page, Record* literal, Record* searchedRecord) {
    while(page->GetFirst(searchedRecord)) {
        int comparisonValue = cng.Compare(literal, this->queryOrderMaker, searchedRecord, this->sortInfo->myOrder);
        if (comparisonValue == 0) {
            return 0;
        }
        if (comparisonValue < 0) {
            return -1;
        }
    }
    return 1;
}

int Sorted::SequentialSearchWithCNF(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while(true) {
        if (this->readRecordFromBuffer(fetchme) == 0) return 0;
        else if (comp.Compare(&fetchme, &literal, &cnf)) break;
    }
    return 1;
}

int Sorted::SequentialSearchWithQueryAndCNF(Record &fetchme, CNF &cnf, Record &literal) {
    Record nextRecord;
    readRecordFromBuffer(nextRecord);
    while(cng.Compare(&literal, queryOrderMaker, &nextRecord, this->sortInfo->myOrder) == 0) {
        if (cng.Compare(&nextRecord, &literal, &cnf)) {
            fetchme.Consume(&nextRecord);
            return 1;
        }
        readRecordFromBuffer(nextRecord);
    }
    return 0;
}

int Sorted::SequentialSearchWithQueryAndCNF(Record &fetchme, CNF &cnf, Record &literal, Record &firstQueryMatchedRecord) {
    while(cng.Compare(&literal, queryOrderMaker, &firstQueryMatchedRecord, this->sortInfo->myOrder) == 0) {
        if (cng.Compare(&firstQueryMatchedRecord, &literal, &cnf)) {
            fetchme.Consume(&firstQueryMatchedRecord);
            return 1;
        }
        readRecordFromBuffer(firstQueryMatchedRecord);
    }
    return 0;
}

off_t Sorted::BinarySearch(off_t low, off_t high, Record &literal) {
    Page bufferPage;
    Record temp;

    // Base condition or Break condition
    if(low == high) {
        file->GetPage(&bufferPage, low);
        bufferPage.GetFirst(&temp);
        if(cng.Compare(&literal, this->queryOrderMaker, &temp, this->sortInfo->myOrder) > 0) {
            return low;
        }
        return std::max(low-1, currentlyBeingReadPageNumber + 1);
    }

    // Get first record of the middle page.
    off_t mid = (low+high)/2;
    file->GetPage(&bufferPage, mid);
    bufferPage.GetFirst(&temp);
    if(cng.Compare(&literal, this->queryOrderMaker, &temp, this->sortInfo->myOrder) > 0) {
        return BinarySearch(mid+1, high, literal);
    }
    return BinarySearch(low, mid-1, literal);
}