#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "File.h"

class GenericDBFile {
public:
    GenericDBFile();
    ~GenericDBFile();

    virtual int Create (const char *fpath) = 0;
    virtual int Open (const char *fpath) = 0;

    virtual void Add (Record &addme) = 0;
    virtual void Load (Schema &myschema, const char *loadpath) = 0;

    virtual void MoveFirst () = 0;
    virtual int GetNext (Record &fetchme) = 0;
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;

    virtual int Close () = 0;
};
#endif