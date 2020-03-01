#include "DBFile.h"

DBFile::DBFile () {
}

DBFile::~DBFile () {
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    ofstream fOut;
    fOut.open(GetMataDataFilePath(f_path));
    fOut << f_type << "\n";

    switch (f_type) {
        case heap: {
            myInternalVar = new Heap();
            break;
        }
        case sorted: {
            SortInfo* sortInfo = (SortInfo*) startup;
            fOut << sortInfo->runLength << "\n";
            fOut << sortInfo->myOrder->ToString() << "\n";

            myInternalVar = new Sorted(sortInfo);
            break;
        }
        default: {
            cerr << "Not yet implemented";
            exit(1);
        }
    }

    fOut.close();
    return myInternalVar->Create(f_path);
}

int DBFile::Open (const char *f_path) {
    ifstream fIn;
    string readLine;

    fIn.open(GetMataDataFilePath(f_path));
    getline(fIn, readLine);

    switch (stoi(readLine)) {
        case heap: {
            myInternalVar = new Heap();
            break;
        }
        case sorted: {
            SortInfo* sortInfo = new SortInfo();
            sortInfo->myOrder = new OrderMaker();

            getline(fIn, readLine);
            sortInfo->runLength = stoi(readLine);

            getline(fIn, readLine);
            sortInfo->myOrder->FromString(readLine);

            myInternalVar = new Sorted(sortInfo);
            break;
        }
        default:
            cerr << "Not yet implemented";
            exit(1);
    }

    fIn.close();
    return myInternalVar->Open(f_path);
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

void DBFile::Add (Record &rec) {
    myInternalVar->Add(rec);
}

void DBFile::MoveFirst () {
    myInternalVar->MoveFirst();
}

int DBFile::GetNext (Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}

int DBFile::Close () {
    return myInternalVar->Close();
}

string DBFile::GetMataDataFilePath(const char *fpath) {
    string metadata_path = fpath;
    metadata_path += ".metadata";
    return metadata_path;
}
