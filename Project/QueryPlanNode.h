#ifndef QUERY_PLAN__NODE_H
#define QUERY_PLAN__NODE_H

#include "Schema.h"
#include "Function.h"
#include "Comparison.h"

class RelOpNode {
public:
    RelOpNode *child1 = nullptr;
    int inputPipeId1 = -1;

    RelOpNode *child2 = nullptr;
    int inputPipeId2 = -1;

    Schema *outputSchema = nullptr;
    int outputPipeId = -1;

    RelOpNode(RelOpNode *child1, RelOpNode *child2);

    virtual void Print();
};


class DuplicateRemovalNode : public RelOpNode {
public:
    Schema *inputSchema = nullptr;

    DuplicateRemovalNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class GroupByNode : public RelOpNode {
public:
    OrderMaker *groupAtts = nullptr;
    Function *computeMe = nullptr;
    int distinctFunc = 0;

    GroupByNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class JoinNode : public RelOpNode {
public:
    CNF *selOp = nullptr;
    Record *literal = nullptr;

    JoinNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class SelectFileNode : public RelOpNode {
public:
    CNF *selOp = nullptr;
    Record *literal = nullptr;

    SelectFileNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class SelectPipeNode : public RelOpNode {
public:
    CNF *selOp = nullptr;
    Record *literal = nullptr;

    SelectPipeNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};

class SumNode : public RelOpNode {
public:
    Function *computeMe = nullptr;
    int distinctFunc = 0;

    SumNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};

class ProjectNode : public RelOpNode {
public:
    int *keepMe = nullptr;
    int numAttsInput;
    int numAttsOutput;

    ProjectNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};

#endif