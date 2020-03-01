#include "Heap.h"

Heap ::Heap() {
    currentlyBeingWritenPageNumber = 0;
    currentlyBeingReadPageNumber = 0;

    writePerformedAfterOpen = false;

    writeBufferPage = new Page();
    readBufferPage = new Page();
}

Heap ::~Heap() {
    delete writeBufferPage;
    delete readBufferPage;
}

int Heap::Create (const char *f_path) {
    int createStatus = file.Open(0, const_cast<char *>(f_path));
    isFileOpen = createStatus == 1;
    return createStatus;
}

int Heap::Open (const char *f_path) {
    file.Open(1, const_cast<char *>(f_path));
    int status = file.Open(1, const_cast<char *>(f_path));
    if (status == 1) {
        writePerformedAfterOpen = false;
        currentlyBeingReadPageNumber = 0;

        // If there are no pages inside the file.
        if (file.GetLength() - 2 < 0) {
            currentlyBeingWritenPageNumber = 0;
            writeBufferPage = new Page();
            readBufferPage = new Page();
        } else {
            currentlyBeingWritenPageNumber = file.GetLength() - 2;
            file.GetPage(writeBufferPage, currentlyBeingWritenPageNumber);
            file.GetPage(readBufferPage, currentlyBeingReadPageNumber);
        }
        isFileOpen = true;
    }
    return status;
}

void Heap::Add (Record &rec) {
    this->addRecordToBuffer(rec);
}

void Heap::Load (Schema &f_schema, const char *loadpath) {
    if (!isFileOpen) {
        std::cerr << "BAD: File is not open!\n";
        exit(1);
    }

    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
        this->addRecordToBuffer(temp);
    }
}

void Heap::MoveFirst () {
    if (!isFileOpen) {
        std::cerr << "BAD: File is not open!\n";
        exit(1);
    }
    // Get the page 0 and put it inside readBufferPage.
    readBufferPage->EmptyItOut();
    if (file.GetLength() - 2 >= 0) {
        file.GetPage(readBufferPage, 0);
    }
}

int Heap::GetNext (Record &fetchme) {
    return this->readRecordFromBuffer(fetchme);
}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while(true) {
        if (this->readRecordFromBuffer(fetchme) == 0) return 0;
        else if (comp.Compare(&fetchme, &literal, &cnf)) break;
    }
    return 1;
}

int Heap::Close () {
    if (!isFileOpen) {
        return 0;
    }
    if(writePerformedAfterOpen) {
        file.AddPage(writeBufferPage, currentlyBeingWritenPageNumber);
    }
    file.Close();
    isFileOpen = false;
    return 1;
}

// Method to handle add record to the write buffer page.
// If the current write page is full, It will save that page to the file,
// and create new write buffer page with the current record.
void Heap::addRecordToBuffer(Record &rec) {
    Record* newRecord;
    if(currentlyBeingReadPageNumber == currentlyBeingWritenPageNumber) {
        newRecord = new Record();
        newRecord->Copy(&rec);
    }
    if(writeBufferPage->Append((&rec)) == 0) {
        file.AddPage(writeBufferPage, currentlyBeingWritenPageNumber++);
        writeBufferPage->EmptyItOut();
        writeBufferPage = new Page();
        writeBufferPage->Append(&rec);
    }

    if(currentlyBeingReadPageNumber == currentlyBeingWritenPageNumber) {
        readBufferPage->Append(newRecord);
    }
    writePerformedAfterOpen = true;
}

// Method to get next record form the read buffer page.
// If there are not records present in the read buffer page,
// It will fill read buffer page with the next page from the file.
// If all the page are already read from the file, it will return 0.
int Heap::readRecordFromBuffer(Record &rec) {
    if (readBufferPage->GetFirst(&rec) == 0) {
        ++currentlyBeingReadPageNumber;
        if (file.GetLength() - 2 < currentlyBeingReadPageNumber) {
            return 0;
        }
        file.GetPage(readBufferPage, currentlyBeingReadPageNumber);
        this->readRecordFromBuffer(rec);
    }
    return 1;
}