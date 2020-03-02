#include "Heap.h"

Heap ::Heap() {
    currentlyBeingWritenPageNumber = 0;
    writeBufferPage.EmptyItOut();
}

Heap ::~Heap() {
}

/* ****************************************** ALL OVERRIDDEN METHODS *********************************************** */

void Heap :: SwitchToWriteMode() {
    if (isInWriteMode) return;

    writeBufferPage.EmptyItOut();
    currentlyBeingWritenPageNumber = GetLengthInPages() <= 0 ? 0 : GetLengthInPages() - 1;
    GetPageFromDataFile(writeBufferPage, currentlyBeingWritenPageNumber);
    isInWriteMode = true;
}

void Heap :: SwitchToReadMode() {
    if (!isInWriteMode) return;
    AddPageToDataFile(writeBufferPage, currentlyBeingWritenPageNumber);
    isInWriteMode = false;
}

void Heap :: AddToDBFile(Record &addme) {
    if(writeBufferPage.Append(&addme)) return;

    AddPageToDataFile(writeBufferPage, currentlyBeingWritenPageNumber++);
    writeBufferPage.Append(&addme);
}

int Heap :: GetNextFromDBFile(Record &fetchme) {
    return GetRecordFromReadBufferPage(fetchme);
}

int Heap :: GetNextFromDBFile(Record &fetchme, CNF &cnf, Record &literal) {
    while(GetRecordFromReadBufferPage(fetchme))
        if (comparisonEngine.Compare(&fetchme, &literal, &cnf)) return 1;

    return 0;
}
