#ifndef QUERY_PLAN_H
#define QUERY_PLAN_H

#include <vector>
#include <float.h>

#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "Function.h"
#include "QueryPlanNode.h"
#include "Comparison.h"
#include "RelOp.h"

void heapPermutation(int a[], int size, int n, vector<int *> *permutations);

struct Query {
    FuncOperator *finalFunction;
    TableList *tables;
    AndList *andList;
    NameList *groupingAtts;
    NameList *attsToSelect;
    int distinctAtts;
    int distinctFunc;
};


class QueryPlan {
private:
    char *catalogPath;
    Query *query;
    Statistics *statistics;

    unordered_map<string, RelOpNode *> groupNameToRelOpNode;
    unordered_map<string, string> relNameToGroupNameMap;

    int nextAvailablePipeId = 0;
    unordered_set<int> freePipeIds;

    void LoadAllTables();

    void SplitAndList(unordered_map<string, AndList *> *tableSelectionAndList, vector<AndList *> *joins);
        void ApplySelection(unordered_map<string, AndList *> *tableSelectionAndList);
        void RearrangeJoins(vector<AndList *> *joins, vector<AndList *> *joins_arranged);
        void ApplyJoins(vector<AndList *> *joins);

    void ApplyGroupBy();

    void ApplySum();

    void ApplyDuplicateRemoval();

    void ApplyProject();

    static void PrintQueryPlanPostOrder(RelOpNode *node);

    int GetFreePipeId();
    void FreePipeId(int pipeId);
    string GetResultantGroupName();

public:
    QueryPlan(char *catalog_path, Statistics *statistics, Query *query);
    ~QueryPlan();

    void Print();
private:
    void MakeQueryPlan();
};

#endif