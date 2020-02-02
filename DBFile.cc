#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

DBFile::DBFile () {
    currentlyBeingWritenPageNumber = 0;
    currentlyBeingReadPageNumber = 0;

    writePerformedAfterOpen = false;
    isFileOpen = false;

    writeBufferPage = new Page();
    readBufferPage = new Page();
}

DBFile::~DBFile () {
    delete writeBufferPage;
    delete readBufferPage;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    file.Open(0, const_cast<char *>(f_path));
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
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

int DBFile::Open (const char *f_path) {
    file.Open(1, const_cast<char *>(f_path));
    currentlyBeingWritenPageNumber = file.GetLength() - 2;
    currentlyBeingReadPageNumber = 0;

    writePerformedAfterOpen = false;

    if(file.GetLength() - 2 < 0) {
        currentlyBeingWritenPageNumber = 0;
        currentlyBeingReadPageNumber = 0;
        writeBufferPage = new Page();
        readBufferPage = new Page();
    } else {
        file.GetPage(writeBufferPage, currentlyBeingWritenPageNumber);
        file.GetPage(readBufferPage, currentlyBeingReadPageNumber);
    }

    isFileOpen = true;
    return 1;
}

void DBFile::MoveFirst () {
    if (!isFileOpen) {
        std::cerr << "BAD: File is not open!\n";
        exit(1);
    }
    file.GetPage(readBufferPage, 0);
}

int DBFile::Close () {
    if (!isFileOpen) {
        return 0;
    }
    if(writePerformedAfterOpen) {
        file.AddPage(writeBufferPage, currentlyBeingWritenPageNumber);
    }
    file.Close();
    return 1;
}

void DBFile::Add (Record &rec) {
    this->addRecordToBuffer(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return this->readRecordFromBuffer(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while(true) {
        if (this->readRecordFromBuffer(fetchme) == 0) return 0;
        else if (comp.Compare(&fetchme, &literal, &cnf)) break;
    }
    return 1;
}

void DBFile::addRecordToBuffer(Record &rec) {
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

int DBFile::readRecordFromBuffer(Record &rec) {
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
