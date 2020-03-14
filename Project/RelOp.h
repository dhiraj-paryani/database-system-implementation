#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
    protected:
        pthread_t thread;
        int runLength = 16;

	public:
        // blocks the caller until the particular relational operator
        // has run to completion
        void WaitUntilDone ();

        // tell us how much internal memory the operation can use
        void Use_n_Pages (int n);
};

struct SelectFileData {
    DBFile* dbFile;
    Pipe* outputPipe;
    CNF* cnf;
    Record* literal;
};
void *SelectFileFunction(void *threadData);
class SelectFile : public RelationalOp {
	public:
	    void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
};

struct SelectPipeData {
    Pipe* inputPipe;
    Pipe* outputPipe;
    CNF* cnf;
    Record* literal;
};
void *Select(void *threadData);
class SelectPipe : public RelationalOp {
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
};

struct ProjectData {
    Pipe* inputPipe;
    Pipe* outputPipe;
    int* keepMe;
    int numAttsInput;
    int numAttsOutput;
};
void *ProjectFunction(void *threadData);
class Project : public RelationalOp { 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
};

struct JoinData {
    Pipe* leftInputPipe;
    Pipe* rightInputPipe;
    Pipe* outputPipe;
    CNF* cnf;
    Record* literal;
    int runLength;
};
void *JoinFunction(void *threadData);
class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
};

struct DuplicateRemovalData {
    Pipe* inputPipe;
    Pipe* outputPipe;
    Schema* schema;
    int runLength;
};
void *DuplicateRemovalFunction (void *threadData);
class DuplicateRemoval : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
};

struct SumData {
    Pipe* inputPipe;
    Pipe* outputPipe;
    Function* computeMe;
};
void *SumFunction (void *threadData);
class Sum : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
};

struct GroupByData {
    Pipe* inputPipe;
    Pipe* outputPipe;
    OrderMaker* groupAtts;
    Function* computeMe;
    int runLength;
};
void *GroupByFunction (void *threadData);
void AddGroupByRecordToPipe(Pipe* outputPipe, Record* tableRecord, double sum, OrderMaker* order);
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
};

struct WriteOutData {
    Pipe* inputPipe;
    FILE* outputFile;
    Schema* schema;
};
void *WriteOutFunction (void *threadData);
class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
};
#endif
