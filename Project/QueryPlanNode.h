#ifndef QUERY_PLAN__NODE_H
#define QUERY_PLAN__NODE_H

#include "Schema.h"
#include "Function.h"
#include "Comparison.h"

class RelOpNode {
public:
    RelOpNode *child1;
    int inputPipeId1 = -1;

    RelOpNode *child2;
    int inputPipeId2 = -1;

    Schema *outputSchema;
    int outputPipeId = -1;

    RelOpNode(RelOpNode *child1, RelOpNode *child2);

    virtual void Print();
};


class DuplicateRemovalNode : public RelOpNode {
public:
    Schema *inputSchema;

    DuplicateRemovalNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class GroupByNode : public RelOpNode {
public:
    OrderMaker *groupAtts;
    Function *computeMe;
    int distinctFunc;

    GroupByNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class JoinNode : public RelOpNode {
public:
    CNF *selOp;
    Record *literal;

    JoinNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class SelectFileNode : public RelOpNode {
public:
    CNF *selOp;
    Record *literal;

    SelectFileNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};


class SelectPipeNode : public RelOpNode {
public:
    CNF *selOp;
    Record *literal;

    SelectPipeNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};

class SumNode : public RelOpNode {
public:
    Function *computeMe;
    int distinctFunc;

    SumNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};

class ProjectNode : public RelOpNode {
public:
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;

    ProjectNode(RelOpNode *child1, RelOpNode *child2) : RelOpNode(child1, child2) {};

    void Print();
};

#endif