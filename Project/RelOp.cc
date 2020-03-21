#include "RelOp.h"

RelationalOp::RelationalOp() {
    this->runLength = 16;
}

void RelationalOp::WaitUntilDone() {
    pthread_join(thread, nullptr);
}

void RelationalOp::Use_n_Pages(int n) {
    this->runLength = n;
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectPipeData *my_data = new SelectPipeData();

    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->cnf = &selOp;
    my_data->literal = &literal;

    pthread_create(&thread, nullptr, SelectPipeFunction, (void *) my_data);
}

void *SelectPipeFunction(void *threadData) {
    SelectPipeData *my_data = (SelectPipeData *) threadData;

    ComparisonEngine comparisonEngine;
    Record temp;
    while (my_data->inputPipe->Remove(&temp)) {
        if (comparisonEngine.Compare(&temp, my_data->literal, my_data->cnf)) {
            my_data->outputPipe->Insert(&temp);
        }
    }
    my_data->outputPipe->ShutDown();
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectFileData *my_data = new SelectFileData();

    my_data->dbFile = &inFile;
    my_data->outputPipe = &outPipe;
    my_data->cnf = &selOp;
    my_data->literal = &literal;

    pthread_create(&thread, nullptr, SelectFileFunction, (void *) my_data);
}

void *SelectFileFunction(void *threadData) {
    SelectFileData *my_data = (SelectFileData *) threadData;

    Record temp;
    while (my_data->dbFile->GetNext(temp, *my_data->cnf, *my_data->literal)) {
        my_data->outputPipe->Insert(&temp);
    }

    my_data->outputPipe->ShutDown();
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    ProjectData *my_data = new ProjectData();

    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->keepMe = keepMe;
    my_data->numAttsInput = numAttsInput;
    my_data->numAttsOutput = numAttsOutput;

    pthread_create(&thread, nullptr, ProjectFunction, (void *) my_data);
}

void *ProjectFunction(void *threadData) {
    ProjectData *my_data = (ProjectData *) threadData;

    Record temp;
    while (my_data->inputPipe->Remove(&temp)) {
        temp.Project(my_data->keepMe, my_data->numAttsOutput, my_data->numAttsInput);
        my_data->outputPipe->Insert(&temp);
    }

    my_data->outputPipe->ShutDown();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    DuplicateRemovalData *my_data = new DuplicateRemovalData();
    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->schema = &mySchema;
    my_data->runLength = this->runLength;

    pthread_create(&thread, nullptr, DuplicateRemovalFunction, (void *) my_data);
}

void *DuplicateRemovalFunction(void *threadData) {
    DuplicateRemovalData *my_data = (DuplicateRemovalData *) threadData;
    OrderMaker orderMaker(my_data->schema);

    Pipe intermediatePipe(PIPE_BUFFER_SIZE);

    BigQ(*my_data->inputPipe, intermediatePipe, orderMaker, my_data->runLength);

    int i = 0;
    Record rec[2];
    ComparisonEngine comparisonEngine;

    if (intermediatePipe.Remove(&rec[(i++) % 2])) {
        while (intermediatePipe.Remove(&rec[i % 2])) {
            if (comparisonEngine.Compare(&rec[(i + 1) % 2], &rec[i % 2], &orderMaker) != 0) {
                my_data->outputPipe->Insert(&rec[(i + 1) % 2]);
            }
            i++;
        }
        my_data->outputPipe->Insert(&rec[(i + 1) % 2]);
    }
    my_data->outputPipe->ShutDown();
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    WriteOutData *my_data = new WriteOutData();
    my_data->inputPipe = &inPipe;
    my_data->outputFile = outFile;
    my_data->schema = &mySchema;

    pthread_create(&thread, nullptr, WriteOutFunction, (void *) my_data);
}

void *WriteOutFunction(void *threadData) {
    WriteOutData *my_data = (WriteOutData *) threadData;

    Record temp;
    while (my_data->inputPipe->Remove(&temp)) {
        fprintf(my_data->outputFile, "%s\n", temp.ToString(my_data->schema).c_str());
    }
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    SumData *my_data = new SumData();
    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->computeMe = &computeMe;

    pthread_create(&thread, nullptr, SumFunction, (void *) my_data);
}

void *SumFunction(void *threadData) {
    SumData *my_data = (SumData *) threadData;

    double sum = 0;
    int intVal = 0;
    double doubleVal = 0;

    Record temp;
    while (my_data->inputPipe->Remove(&temp)) {
        intVal = 0;
        doubleVal = 0;
        my_data->computeMe->Apply(temp, intVal, doubleVal);
        sum += (intVal + doubleVal);
    }

    temp.ComposeRecord(&sumSchema, (std::to_string(sum) + "|").c_str());
    my_data->outputPipe->Insert(&temp);

    my_data->outputPipe->ShutDown();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    JoinData *my_data = new JoinData();
    my_data->leftInputPipe = &inPipeL;
    my_data->rightInputPipe = &inPipeR;
    my_data->outputPipe = &outPipe;
    my_data->cnf = &selOp;
    my_data->literal = &literal;
    my_data->runLength = runLength;

    pthread_create(&thread, nullptr, JoinFunction, (void *) my_data);
}

void *JoinFunction(void *threadData) {
    JoinData *my_data = (JoinData *) threadData;
    OrderMaker leftOrderMaker, rightOrderMaker;

    my_data->cnf->GetSortOrders(leftOrderMaker, rightOrderMaker);

    if (leftOrderMaker.isEmpty() || rightOrderMaker.isEmpty()) {
        NestedBlockJoin(my_data->leftInputPipe, my_data->rightInputPipe, my_data->outputPipe, my_data->runLength);
    } else {
        Pipe leftBigQOutputPipe(PIPE_BUFFER_SIZE), rightBigQOutputPipe(PIPE_BUFFER_SIZE);
        BigQ(*my_data->leftInputPipe, leftBigQOutputPipe, leftOrderMaker, my_data->runLength);
        BigQ(*my_data->rightInputPipe, rightBigQOutputPipe, rightOrderMaker, my_data->runLength);
        JoinUsingSortMerge(&leftBigQOutputPipe, &rightBigQOutputPipe, my_data->outputPipe, &leftOrderMaker,
                           &rightOrderMaker);
    }
    my_data->outputPipe->ShutDown();

}

void NestedBlockJoin(Pipe *leftInputPipe, Pipe *rightInputPipe, Pipe *outputPipe, int runLength) {
    // Create temporary heap DBFiles for left and right input pipe.
    HeapDBFile leftDBFile;
    HeapDBFile rightDBFile;

    char *leftDBFileName = "tempLeftDBfile.bin";
    char *rightDBFileName = "tempRightDBfile.bin";

    leftDBFile.Create(leftDBFileName);
    rightDBFile.Create(rightDBFileName);

    // Add all left input pipe's records into DBfile.
    Record temp;
    while (leftInputPipe->Remove(&temp)) {
        leftDBFile.Add(temp);
    }
    // Add all right input pipe's records into DBfile.
    while (rightInputPipe->Remove(&temp)) {
        rightDBFile.Add(temp);
    }

    // Nested join to merge all records from left DBFile and right DBFile.
    Record leftDBFileRecord, rightDBFileRecord, mergedRecord;
    // Create buffer page array to store current runs' records
    Page *recordsBlock = new(std::nothrow) Page[runLength];
    if (recordsBlock == NULL) {
        cerr << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    leftDBFile.MoveFirst();
    bool leftDBFileNotFullyConsumed = leftDBFile.GetNext(leftDBFileRecord);

    while (leftDBFileNotFullyConsumed) {
        int blockPageIndex = 0;

        while (leftDBFileNotFullyConsumed) {
            if (!recordsBlock[blockPageIndex].Append(&leftDBFileRecord)) {
                if (blockPageIndex + 1 < runLength) {
                    blockPageIndex++;
                    recordsBlock[blockPageIndex].Append(&leftDBFileRecord);
                } else {
                    break;
                }
            }
            leftDBFileNotFullyConsumed = leftDBFile.GetNext(leftDBFileRecord);
        }
        vector<Record *> leftBlockRecords;
        LoadVectorFromBlock(&leftBlockRecords, recordsBlock, blockPageIndex);

        rightDBFile.MoveFirst();
        bool rightDBFileNotFullyConsumed = rightDBFile.GetNext(rightDBFileRecord);

        while (rightDBFileNotFullyConsumed) {
            blockPageIndex = 0;
            while (rightDBFileNotFullyConsumed) {
                if (!recordsBlock[blockPageIndex].Append(&rightDBFileRecord)) {
                    if (blockPageIndex + 1 < runLength) {
                        blockPageIndex++;
                        recordsBlock[blockPageIndex].Append(&rightDBFileRecord);
                    } else {
                        break;
                    }
                }
                rightDBFileNotFullyConsumed = rightDBFile.GetNext(rightDBFileRecord);
            }
            vector<Record *> rightBlockRecords;
            LoadVectorFromBlock(&rightBlockRecords, recordsBlock, blockPageIndex);
            // Join left block and right block of pages.
            JoinTableBlocks(&leftBlockRecords, &rightBlockRecords, outputPipe);
        }
    }


    // Delete both temporary files.
    remove(leftDBFileName);
    remove(rightDBFileName);
}

void LoadVectorFromBlock(vector<Record *> *loadMe, Page *block, int blockLength) {
    Record *temp = new Record();
    for (int i = 0; i <= blockLength; i++) {
        while (block[i].GetFirst(temp)) {
            loadMe->push_back(temp);
            temp = new Record();
        }
    }
}

void JoinTableBlocks(vector<Record *> *leftBlockRecords, vector<Record *> *rightBlockRecords, Pipe *outputPipe) {
    Record mergedRecord;
    for (Record *leftBlockRecord : *leftBlockRecords) {
        for (Record *rightBlockRecord : *rightBlockRecords) {
            mergedRecord.MergeRecords(leftBlockRecord, rightBlockRecord);
            outputPipe->Insert(&mergedRecord);
        }
    }
}

void JoinUsingSortMerge(Pipe *leftInputPipe, Pipe *rightInputPipe, Pipe *outputPipe,
                        OrderMaker *leftOrderMaker, OrderMaker *rightOrderMaker) {

    Record leftOutputPipeRecord, rightOutputPipeRecord;
    bool leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord);
    bool rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord);

    ComparisonEngine comparisonEngine;
    // While there is data in both the input pipes.
    while (leftOutputPipeNotFullyConsumed && rightOutputPipeNotFullyConsumed) {

        // Compare the left and right record.
        int comparisonValue = comparisonEngine.Compare(&leftOutputPipeRecord, leftOrderMaker,
                                                       &rightOutputPipeRecord, rightOrderMaker);

        if (comparisonValue == 0) {
            Record mergedRecord;
            vector<Record *> leftPipeRecords, rightPipeRecords;

            Record *tempLeft = new Record();
            tempLeft->Consume(&leftOutputPipeRecord);
            leftPipeRecords.push_back(tempLeft);

            int index = 0;
            while (true) {
                leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord);
                if (leftOutputPipeNotFullyConsumed &&
                    comparisonEngine.Compare(leftPipeRecords[index], &leftOutputPipeRecord, leftOrderMaker) == 0) {
                    tempLeft = new Record();
                    tempLeft->Consume(&leftOutputPipeRecord);
                    leftPipeRecords.push_back(tempLeft);
                } else {
                    break;
                }
            }

            Record *tempRight = new Record();
            tempRight->Consume(&rightOutputPipeRecord);
            rightPipeRecords.push_back(tempRight);


            index = 0;
            while (true) {
                rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord);
                if (rightOutputPipeNotFullyConsumed &&
                    comparisonEngine.Compare(rightPipeRecords[index++], &rightOutputPipeRecord, rightOrderMaker) == 0) {
                    tempRight = new Record();
                    tempRight->Consume(&rightOutputPipeRecord);
                    rightPipeRecords.push_back(tempRight);
                } else {
                    break;
                }
            }

            for (Record *leftPipeRecord : leftPipeRecords) {
                for (Record *rightPipeRecord : rightPipeRecords) {
                    mergedRecord.MergeRecords(leftPipeRecord, rightPipeRecord);
                    outputPipe->Insert(&mergedRecord);
                }
            }
        } else if (comparisonValue < 0) {
            leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord);
        } else {
            rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord);
        }
    }
    while (leftOutputPipeNotFullyConsumed) {
        leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord);
    }
    while (rightOutputPipeNotFullyConsumed) {
        rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord);
    }
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    GroupByData *my_data = new GroupByData();
    my_data->inputPipe = &inPipe;
    my_data->outputPipe = &outPipe;
    my_data->groupAtts = &groupAtts;
    my_data->computeMe = &computeMe;
    my_data->runLength = runLength;

    pthread_create(&thread, nullptr, GroupByFunction, (void *) my_data);
}

void *GroupByFunction(void *threadData) {
    GroupByData *my_data = (GroupByData *) threadData;

    Pipe bigQOutputPipe(PIPE_BUFFER_SIZE);
    BigQ(*my_data->inputPipe, bigQOutputPipe, *my_data->groupAtts, my_data->runLength);

    double sum = 0;
    int intVal = 0;
    double doubleVal = 0;

    int i = 0;
    Record recs[2];

    ComparisonEngine comparisonEngine;
    if (bigQOutputPipe.Remove(&recs[i % 2])) {
        my_data->computeMe->Apply(recs[i % 2], intVal, doubleVal);
        sum += (intVal + doubleVal);
        i++;
        while (bigQOutputPipe.Remove(&recs[i % 2])) {
            if (comparisonEngine.Compare(&recs[(i + 1) % 2], &recs[i % 2], my_data->groupAtts) != 0) {
                AddGroupByRecordToPipe(my_data->outputPipe, &recs[(i + 1) % 2], sum, my_data->groupAtts);
                intVal = 0;
                doubleVal = 0;
                sum = 0;
            }
            my_data->computeMe->Apply(recs[i % 2], intVal, doubleVal);
            sum += (intVal + doubleVal);
            intVal = 0;
            doubleVal = 0;
            i++;
        }
        AddGroupByRecordToPipe(my_data->outputPipe, &recs[(i + 1) % 2], sum, my_data->groupAtts);
    }
    my_data->outputPipe->ShutDown();
}

void AddGroupByRecordToPipe(Pipe *outputPipe, Record *tableRecord, double sum, OrderMaker *order) {
    Record sumRecord;
    sumRecord.ComposeRecord(&sumSchema, (std::to_string(sum) + "|").c_str());

    Schema groupAttributesSchema(*order);
    tableRecord->Project(order->GetAtts(), order->GetNumAtts());

    Record groupRecord;
    groupRecord.MergeRecords(&sumRecord, tableRecord);

    outputPipe->Insert(&groupRecord);
}