#include "RelOp.h"

void RelationalOp :: WaitUntilDone () {
    pthread_join(thread, nullptr);
}

void RelationalOp :: Use_n_Pages (int n) {
    this->runLength = n;
}

void SelectPipe :: Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectPipeData* my_data = new SelectPipeData();

    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->cnf = &selOp;
    my_data->literal = &literal;

    pthread_create(&thread, nullptr, Select, (void *) my_data);
}

void *Select(void *threadData) {
    SelectPipeData* my_data = (SelectPipeData*) threadData;

    ComparisonEngine comparisonEngine;
    Record temp;
    while (my_data->inputPipe->Remove(&temp)) {
        if (comparisonEngine.Compare(&temp, my_data->literal, my_data->cnf)) {
            my_data->outputPipe->Insert(&temp);
        }
    }
    my_data->outputPipe->ShutDown();
}

void SelectFile :: Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectFileData* my_data = new SelectFileData();

    my_data->dbFile = &inFile;
    my_data->outputPipe = &outPipe;
    my_data->cnf = &selOp;
    my_data->literal = &literal;

    pthread_create(&thread, nullptr, SelectFileFunction, (void *) my_data);
}

void *SelectFileFunction(void *threadData) {
    SelectFileData* my_data = (SelectFileData*) threadData;
    my_data->dbFile->MoveFirst();

    Record temp;
    while(my_data->dbFile->GetNext(temp, *my_data->cnf, *my_data->literal)) {
        my_data->outputPipe->Insert(&temp);
    }

    my_data->outputPipe->ShutDown();
}

void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    ProjectData* my_data = new ProjectData();

    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->keepMe = keepMe;
    my_data->numAttsInput = numAttsInput;
    my_data->numAttsOutput = numAttsOutput;

    pthread_create(&thread, nullptr, ProjectFunction, (void *) my_data);
}

void *ProjectFunction(void *threadData) {
    ProjectData* my_data = (ProjectData*) threadData;

    Record temp;
    while(my_data->inputPipe->Remove(&temp)) {
        temp.Project(my_data->keepMe, my_data->numAttsOutput, my_data->numAttsInput);
        my_data->outputPipe->Insert(&temp);
    }

    my_data->outputPipe->ShutDown();
}

void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    DuplicateRemovalData* my_data = new DuplicateRemovalData();
    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->schema = &mySchema;
    my_data->runLength = this->runLength;

    pthread_create(&thread, nullptr, DuplicateRemovalFunction, (void *) my_data);
}

void *DuplicateRemovalFunction (void *threadData) {
    DuplicateRemovalData* my_data = (DuplicateRemovalData*) threadData;
    OrderMaker orderMaker(my_data->schema);

    Pipe intermediatePipe(100);

    BigQ(*my_data->inputPipe, intermediatePipe, orderMaker, my_data->runLength);

    int i=0;
    Record rec[2];
    ComparisonEngine comparisonEngine;

    if(intermediatePipe.Remove(&rec[(i++)%2])) {
        while (intermediatePipe.Remove(&rec[i%2])) {
            if (comparisonEngine.Compare(&rec[(i+1)%2], &rec[i%2], &orderMaker) != 0) {
                my_data->outputPipe->Insert(&rec[(i+1)%2]);
            }
            i++;
        }
        my_data->outputPipe->Insert(&rec[(i+1)%2]);
    }
    my_data->outputPipe->ShutDown();
}

void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    WriteOutData* my_data = new WriteOutData();
    my_data->inputPipe = &inPipe;
    my_data->outputFile = outFile;
    my_data->schema = &mySchema;

    pthread_create(&thread, nullptr, WriteOutFunction, (void *) my_data);
}

void *WriteOutFunction (void *threadData) {
    WriteOutData* my_data = (WriteOutData*) threadData;

    int n = my_data->schema->GetNumAtts();
    Attribute *atts = my_data->schema->GetAtts();

    Record temp;
    int count = 0;
    while(my_data->inputPipe->Remove(&temp)) {
        ++count;
        for (int i = 0; i < n; i++) {

            int pointer = ((int *) temp.bits)[i + 1];

            if (atts[i].myType == Int) {
                int *myInt = (int *) &(temp.bits[pointer]);
                fprintf(my_data->outputFile, "%d|", *myInt);

                // then is a double
            } else if (atts[i].myType == Double) {
                double *myDouble = (double *) &(temp.bits[pointer]);
                fprintf(my_data->outputFile, "%f|", *myDouble);

                // then is a character string
            } else if (atts[i].myType == String) {
                char *myString = (char *) &(temp.bits[pointer]);
                fprintf(my_data->outputFile, "%c|", *myString);
            }
        }
        fprintf(my_data->outputFile, "\n");
    }
}

void Sum :: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    SumData* my_data = new SumData();
    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->computeMe = &computeMe;

    pthread_create(&thread, nullptr, SumFunction, (void *) my_data);
}

void *SumFunction (void *threadData) {
    SumData* my_data = (SumData*) threadData;

    double sum = 0;
    int intVal = 0; double doubleVal = 0;

    Record temp;
    while (my_data->inputPipe->Remove(&temp)) {
        intVal = 0; doubleVal = 0;
        my_data->computeMe->Apply(temp, intVal, doubleVal);
        sum += (intVal + doubleVal);
    }

    Attribute DA = {"sum", Double};
    Schema out_sch ("out_sch", 1, &DA);

    temp.ComposeRecord(&out_sch, (std::to_string(sum) + "|").c_str());

    my_data->outputPipe->Insert(&temp);

    my_data->outputPipe->ShutDown();
}

void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    JoinData* my_data = new JoinData();
    my_data->leftInputPipe = &inPipeL;
    my_data->rightInputPipe = &inPipeR;
    my_data->outputPipe = &outPipe;
    my_data->cnf = &selOp;
    my_data->literal = &literal;
    my_data->runLength = runLength;

    pthread_create(&thread, nullptr, JoinFunction, (void *) my_data);
}

void *JoinFunction (void *threadData) {
    JoinData* my_data = (JoinData*) threadData;
    OrderMaker leftOrderMaker, rightOrderMaker;

    my_data->cnf->GetSortOrders(leftOrderMaker, rightOrderMaker);

    if (leftOrderMaker.isEmpty() || rightOrderMaker.isEmpty()) {
        cerr << "Order Maker is empty\n";
        exit(1);
        // block-nested loops join.
    }

    Pipe leftOutputPipe(100), rightOutputPipe(100);
    BigQ(*my_data->leftInputPipe, leftOutputPipe, leftOrderMaker, my_data->runLength);
    BigQ(*my_data->rightInputPipe, rightOutputPipe, rightOrderMaker, my_data->runLength);

    Record leftOutputPipeRecord, rightOutputPipeRecord;
    bool leftOutputPipeNotFullyConsumed = leftOutputPipe.Remove(&leftOutputPipeRecord);
    bool rightOutputPipeNotFullyConsumed = rightOutputPipe.Remove(&rightOutputPipeRecord);

    ComparisonEngine comparisonEngine;
    while(leftOutputPipeNotFullyConsumed && rightOutputPipeNotFullyConsumed) {
        int comparisonValue = comparisonEngine.Compare(&leftOutputPipeRecord, &leftOrderMaker,
                &rightOutputPipeRecord, &rightOrderMaker);

        if (comparisonValue == 0) {
            Record mergedRecord;
            mergedRecord.MergeRecords(&leftOutputPipeRecord, &rightOutputPipeRecord);
            my_data->outputPipe->Insert(&mergedRecord);
            // leftOutputPipeNotFullyConsumed = leftOutputPipe.Remove(&leftOutputPipeRecord);
            rightOutputPipeNotFullyConsumed = rightOutputPipe.Remove(&rightOutputPipeRecord);
        } else if (comparisonValue < 0) {
            leftOutputPipeNotFullyConsumed = leftOutputPipe.Remove(&leftOutputPipeRecord);
        } else {
            rightOutputPipeNotFullyConsumed = rightOutputPipe.Remove(&rightOutputPipeRecord);
        }
    }
    while(leftOutputPipeNotFullyConsumed) {
        leftOutputPipeNotFullyConsumed = leftOutputPipe.Remove(&leftOutputPipeRecord);
    }
    while(rightOutputPipeNotFullyConsumed) {
        rightOutputPipeNotFullyConsumed = rightOutputPipe.Remove(&rightOutputPipeRecord);
    }

    my_data->outputPipe->ShutDown();
}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    GroupByData* my_data = new GroupByData();
    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->groupAtts = &groupAtts;
    my_data->computeMe = &computeMe;
    my_data->runLength = runLength;

    pthread_create(&thread, nullptr, GroupByFunction, (void *) my_data);
}

void *GroupByFunction (void *threadData) {
    GroupByData* my_data = (GroupByData*) threadData;

    Schema groupAttributesSchema (*my_data->groupAtts);


    Pipe bigQOutputPipe(100);
    BigQ(*my_data->inputPipe, bigQOutputPipe, *my_data->groupAtts, my_data->runLength);

    double sum = 0;
    int intVal = 0; double doubleVal = 0;

    int i = 0;
    Record recs[2];

    ComparisonEngine comparisonEngine;
    int count = 0;
    if(bigQOutputPipe.Remove(&recs[i%2])) {
        count++;
        my_data->computeMe->Apply(recs[i%2], intVal, doubleVal);
        sum += (intVal + doubleVal);
        i++;
        while (bigQOutputPipe.Remove(&recs[i%2])) {
            count++;
            if (comparisonEngine.Compare(&recs[(i+1)%2], &recs[i%2], my_data->groupAtts) != 0) {
                AddGroupByRecordToPipe(my_data->outputPipe, &recs[(i+1)%2], sum, my_data->groupAtts);
                intVal=0; doubleVal=0;
                sum = 0;
            }
            my_data->computeMe->Apply(recs[i%2], intVal, doubleVal);
            sum += (intVal + doubleVal);
            intVal=0; doubleVal=0;
            i++;
        }
        AddGroupByRecordToPipe(my_data->outputPipe, &recs[(i+1)%2], sum, my_data->groupAtts);
    }
    cerr << "Total Records: " << count << "\n";
    my_data->outputPipe->ShutDown();
}

void AddGroupByRecordToPipe(Pipe* outputPipe, Record* tableRecord, double sum, OrderMaker* order) {
    Attribute DA = {"sum", Double};
    Schema out_sch ("out_sch", 1, &DA);
    Record sumRecord;
    sumRecord.ComposeRecord(&out_sch, (std::to_string(sum) + "|").c_str());

    Schema groupAttributesSchema (*order);
    tableRecord->Project(order->GetAtts(), order->GetNumAtts());

    Record groupRecord;
    groupRecord.MergeRecords(&sumRecord, tableRecord);

    outputPipe->Insert(&groupRecord);
}