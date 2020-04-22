#ifndef QUERY_PLAN_NODE_H
#define QUERY_PLAN_NODE_H

#include "QueryPlanNode.h"

void RelOpNode :: Print() {
    if (inputPipeId1 != -1) {
        cout << "Input Pipe " << inputPipeId1 << "\n";
    }

    if (inputPipeId2 != -1) {
        cout << "Input Pipe " << inputPipeId2 << "\n";
    }

    if (outputPipeId != -1) {
        cout << "Output Pipe " << outputPipeId << "\n";
    }

    if (outputSchema) {
        cout << "Output Schema:" << "\n";
        outputSchema->Print();
    }
}

void DuplicateRemovalNode :: Print() {
    cout << " *********** " << "\n";
    cout << "DISTINCT operation" << "\n";
    RelOpNode::Print();
}

void GroupByNode :: Print() {
    cout << " *********** " << "\n";
    cout << "GROUP BY operation" << "\n";
    RelOpNode::Print();
    cout << "\n";

    if(groupAtts) {
        cout << "GROUPING ON" << "\n";
        groupAtts->Print();
        cout << "\n";
    }

    if(computeMe) {
        cout << "FUNCTION" << "\n";
        computeMe->Print(child1->outputSchema);
        cout << "\n";
        cout << "Distinct Function: " << distinctFunc << "\n";
        cout << "\n";
    }
}

void JoinNode :: Print() {
    cout << " *********** " << "\n";
    cout << "JOIN operation" << "\n";
    RelOpNode::Print();
    cout << "\n";

    cout << "CNF: " << "\n";
    if (selOp) {
        selOp->Print();
        cout << "\n";
    }

}

void SelectFileNode :: Print() {
    cout << " *********** " << "\n";
    cout << "SELECT FILE operation" << "\n";
    RelOpNode::Print();
    cout << "\n";

    if (selOp) {
        cout << "SELECTION CNF :" << "\n";
        selOp->Print();
        cout << "\n";
    }
}

void SelectPipeNode :: Print() {
    cout << " *********** " << "\n";
    cout << "SELECT PIPE operation" << "\n";
    RelOpNode::Print();
    cout << "\n";

    if (selOp) {
        cout << "SELECTION CNF :" << "\n";
        selOp->Print();
        cout << "\n";
    }
}

void SumNode :: Print() {
    cout << " *********** " << "\n";
    cout << "SUM operation" << "\n";
    RelOpNode::Print();
    cout << "\n";

    if(computeMe) {
        cout << "FUNCTION" << "\n";
        computeMe->Print(child1->outputSchema);
        cout << "\n";
        cout << "Distinct Function: " << distinctFunc << "\n";
        cout << "\n";
    }
}

void ProjectNode :: Print() {
    cout << " *********** " << "\n";
    cout << "PROJECT operation" << "\n";
    RelOpNode::Print();
    cout << "\n";
}

#endif


