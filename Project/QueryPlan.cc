#include "QueryPlan.h"

QueryPlan::QueryPlan(char *catalog_path, Statistics *statistics, Query *query) {
    this->catalogPath = catalog_path;
    this->statistics = statistics;
    this->query = query;

    MakeQueryPlan();
}

QueryPlan::~QueryPlan() {
}

void QueryPlan::MakeQueryPlan() {
    unordered_map<string, AndList *> tableSelectionAndList;
    vector<AndList *> joins;
    vector<AndList *> joins_arranged;

    // Load all the tables using SelectFile RelOp.
    LoadAllTables();

    // Split the AndList into selection and joins.
    SplitAndList(&tableSelectionAndList, &joins);

    // Apply selection on tables using SelectPipe RelOp.
    ApplySelection(&tableSelectionAndList);

    // Rearrange joins, so that number of intermediate tuples generated will be minimum.
    RearrangeJoins(&joins, &joins_arranged);
    // Apply joins on tables using Join RelOp.
    ApplyJoins(&joins_arranged);

    // Apply group by if it is in the query using GroupBy RelOp.
    ApplyGroupBy();

    // Apply Function if it is in the query using Sum RelOp.
    ApplySum();

    // Apply Project using Project RelOp.
    ApplyProject();

    // Apply Duplicate removal using DuplicateRemoval RelOp if distinct is present.
    ApplyDuplicateRemoval();
}

void QueryPlan::LoadAllTables() {
    TableList *tableList = query->tables;

    while (tableList) {
        // Create SelectFileNode RelOp.
        SelectFileNode *selectFileNode = new SelectFileNode(NULL, NULL);

        // Create Schema with aliased attributes.
        Schema *schema = new Schema(catalogPath, tableList->tableName);
        schema->AliasAttributes(tableList->aliasAs);

        selectFileNode->outputSchema = schema;
        selectFileNode->outputPipeId = nextAvailablePipeId++;

        relNameToGroupNameMap[tableList->aliasAs] = tableList->aliasAs;
        groupNameToRelOpNode[tableList->aliasAs] = selectFileNode;

        tableList = tableList->next;
    }
}

void QueryPlan::ApplySelection(unordered_map<string, AndList *> *tableSelectionAndList) {
    for (auto const &item : *tableSelectionAndList) {
        string relName = item.first;
        string groupName = relNameToGroupNameMap[relName];
        SelectFileNode *inputRelOpNode = dynamic_cast<SelectFileNode *>(groupNameToRelOpNode[groupName]);

        // Create CNF
        AndList *andList = item.second;
        CNF *cnf = new CNF();
        Record *literal = new Record();
        cnf->GrowFromParseTree(andList, inputRelOpNode->outputSchema, *literal); // constructs CNF predicate

        if (!inputRelOpNode->selOp) {
            inputRelOpNode->selOp = cnf;
            inputRelOpNode->literal = literal;
        } else {
            // Create SelectPipe
            SelectPipeNode *selectPipeNode = new SelectPipeNode(inputRelOpNode, NULL);

            selectPipeNode->outputSchema = inputRelOpNode->outputSchema;
            selectPipeNode->outputPipeId = nextAvailablePipeId++;

            selectPipeNode->selOp = cnf;
            selectPipeNode->literal = literal;

            groupNameToRelOpNode[groupName] = selectPipeNode;
        }

        // Updating Statistics
        char *applyRelNames[] = {const_cast<char *>(relName.c_str())};
        statistics->Apply(andList, applyRelNames, 1);
    }
}

void QueryPlan::ApplyJoins(vector<AndList *> *joins) {
    for (AndList *andList : *joins) {
        OrList *orList = andList->left;

        string leftOperandName = string(orList->left->left->value);
        string tableName1 = leftOperandName.substr(0, leftOperandName.find('.'));
        string groupName1 = relNameToGroupNameMap[tableName1];
        RelOpNode *inputRelOp1 = groupNameToRelOpNode[groupName1];

        string rightOperandName = string(orList->left->right->value);
        string tableName2 = rightOperandName.substr(0, rightOperandName.find('.'));
        string groupName2 = relNameToGroupNameMap[tableName2];
        RelOpNode *inputRelOp2 = groupNameToRelOpNode[groupName2];

        // constructs CNF predicate
        CNF *cnf = new CNF();
        Record *literal = new Record();
        cnf->GrowFromParseTree(andList, inputRelOp1->outputSchema, inputRelOp2->outputSchema, *literal);

        JoinNode *joinNode = new JoinNode(inputRelOp1, inputRelOp2);

        Schema *outputSchema = new Schema(inputRelOp1->outputSchema, inputRelOp2->outputSchema);
        joinNode->outputSchema = outputSchema;
        joinNode->outputPipeId = nextAvailablePipeId++;

        joinNode->selOp = cnf;
        joinNode->literal = literal;

        string newGroupName;
        newGroupName.append(groupName1).append("&").append(groupName2);
        relNameToGroupNameMap[tableName1] = newGroupName;
        relNameToGroupNameMap[tableName2] = newGroupName;

        groupNameToRelOpNode.erase(groupName1);
        groupNameToRelOpNode.erase(groupName2);
        groupNameToRelOpNode[newGroupName] = joinNode;
    }
}

void QueryPlan::ApplyGroupBy() {
    NameList *nameList = query->groupingAtts;
    if (!nameList)
        return;

    // Get Resultant RelOp Node.
    string finalGroupName = GetResultantGroupName();
    RelOpNode *inputRelOpNode = groupNameToRelOpNode[finalGroupName];

    Schema *groupByInputSchema = inputRelOpNode->outputSchema;

    // Build Compute function.
    Function *function = new Function();
    function->GrowFromParseTree(query->finalFunction, *groupByInputSchema);

    // Build OrderMaker
    OrderMaker *orderMaker = new OrderMaker(groupByInputSchema, nameList);

    // Build Schema and keepMe From nameList
    int *keepMe;
    Schema *groupedAttsSchema = new Schema(groupByInputSchema, nameList, keepMe);

    Schema *outputSchema = new Schema(&sumSchema, groupedAttsSchema);

    // Create Group by Node.
    GroupByNode *groupByNode = new GroupByNode(inputRelOpNode, NULL);

    groupByNode->outputSchema = outputSchema;
    groupByNode->outputPipeId = nextAvailablePipeId++;

    groupByNode->groupAtts = orderMaker;
    groupByNode->computeMe = function;
    groupByNode->distinctFunc = query->distinctFunc;

    groupNameToRelOpNode[finalGroupName] = groupByNode;
}

void QueryPlan::ApplySum() {
    if (query->groupingAtts || !query->finalFunction) {
        return;
    }

    // Get Resultant RelOp Node.
    string finalGroupName = GetResultantGroupName();
    RelOpNode *inputRelOpNode = groupNameToRelOpNode[finalGroupName];

    // Build Compute function.
    Function *function = new Function();
    function->GrowFromParseTree(query->finalFunction, *inputRelOpNode->outputSchema);

    SumNode *sumNode = new SumNode(inputRelOpNode, NULL);
    sumNode->outputSchema = &sumSchema;
    sumNode->outputPipeId = nextAvailablePipeId++;

    sumNode->computeMe = function;
    sumNode->distinctFunc = query->distinctFunc;

    groupNameToRelOpNode[finalGroupName] = sumNode;
}

void QueryPlan::ApplyProject() {
    NameList *attsToSelect = query->attsToSelect;

    if (query->finalFunction) {
        NameList *sumAtt = new NameList();
        sumAtt->name = SUM_ATT_NAME;
        sumAtt->next = attsToSelect;
        attsToSelect = sumAtt;
    }

    if (!attsToSelect)
        return;

    // Get Resultant RelOp Node.
    string finalGroupName = GetResultantGroupName();
    RelOpNode *inputRelOpNode = groupNameToRelOpNode[finalGroupName];

    Schema *inputSchema = inputRelOpNode->outputSchema;

    int *keepMe;
    Schema *outputSchema = new Schema(inputSchema, attsToSelect, keepMe);

    if (inputSchema->GetNumAtts() == outputSchema->GetNumAtts()) {
        return;
    }

    // Create Project RelOp Node
    ProjectNode *projectNode = new ProjectNode(inputRelOpNode, NULL);

    projectNode->outputSchema = outputSchema;
    projectNode->outputPipeId = nextAvailablePipeId++;

    projectNode->keepMe = keepMe;
    projectNode->numAttsInput = inputSchema->GetNumAtts();
    projectNode->numAttsOutput = outputSchema->GetNumAtts();

    groupNameToRelOpNode[finalGroupName] = projectNode;
}

void QueryPlan::ApplyDuplicateRemoval() {
    if (!query->distinctAtts)
        return;

    // Get Resultant RelOp Node.
    string finalGroupName = GetResultantGroupName();
    RelOpNode *inputRelOpNode = groupNameToRelOpNode[finalGroupName];

    // Create Distinct RelOp Node.
    DuplicateRemovalNode *duplicateRemovalNode = new DuplicateRemovalNode(inputRelOpNode, NULL);

    duplicateRemovalNode->outputPipeId = nextAvailablePipeId++;
    duplicateRemovalNode->outputSchema = inputRelOpNode->outputSchema;

    duplicateRemovalNode->inputSchema = inputRelOpNode->outputSchema;

    groupNameToRelOpNode[finalGroupName] = duplicateRemovalNode;
}

void QueryPlan::SplitAndList(unordered_map<string, AndList *> *tableSelectionAndList, vector<AndList *> *joins) {

    AndList *andList = query->andList;
    while (andList) {
        unordered_map<string, AndList *> currentTableSelectionAndList;

        OrList *orList = andList->left;

        while (orList) {

            Operand *leftOperand = orList->left->left;
            Operand *rightOperand = orList->left->right;

            // Duplicate OrList
            OrList *newOrList = new OrList();
            newOrList->left = orList->left;
            if (leftOperand->code == NAME && rightOperand->code == NAME) {
                AndList *newAndList = new AndList();

                // Add to new or list to and list.
                newAndList->left = newOrList;

                // Push newly created and list to joins vector.
                joins->push_back(newAndList);
            } else if (leftOperand->code == NAME || rightOperand->code == NAME) {
                Operand *nameOperand = leftOperand->code == NAME ? leftOperand : rightOperand;
                string name = string(nameOperand->value);
                string relationName = name.substr(0, name.find('.'));

                if (currentTableSelectionAndList.find(relationName) == currentTableSelectionAndList.end()) {
                    AndList *newAndList = new AndList();
                    newAndList->left = newOrList;
                    currentTableSelectionAndList[relationName] = newAndList;
                } else {
                    OrList *currentOrList = currentTableSelectionAndList[relationName]->left;

                    while (currentOrList->rightOr) {
                        currentOrList = currentOrList->rightOr;
                    }
                    currentOrList->rightOr = newOrList;
                }
            }
            orList = orList->rightOr;
        }

        // Iterate and merge and lists
        for (auto const &item : currentTableSelectionAndList) {
            if (tableSelectionAndList->find(item.first) == tableSelectionAndList->end()) {
                (*tableSelectionAndList)[item.first] = item.second;
            } else {
                AndList *currentAndList = tableSelectionAndList->at(item.first);

                while (currentAndList->rightAnd) {
                    currentAndList = currentAndList->rightAnd;
                }

                currentAndList->rightAnd = item.second;
            }
        }

        andList = andList->rightAnd;
    }
}

void QueryPlan::RearrangeJoins(vector<AndList *> *joins, vector<AndList *> *joins_arranged) {
    int n = joins->size();
    if (n < 1) {
        return;
    }
    int initialPermutation[n];
    for (int i = 0; i < n; i++) {
        initialPermutation[i] = i;
    }

    vector<int *> permutations;

    HeapPermutation(initialPermutation, joins->size(), joins->size(), &permutations);

    int minI = -1;
    double minIntermediateTuples = DBL_MAX;
    for (int i = 0; i < permutations.size(); i++) {
        double permutationIntermediateTuples = 0.0;
        Statistics dummy(*statistics);

        int relNamesIndex = 0;
        char **relNames = new char *[2 * n];
        unordered_set<string> relNamesSet;
        for (int j = 0; j < n; j++) {
            AndList *currentAndList = joins->at(permutations[i][j]);
            string attNameWithRelName1 = string(currentAndList->left->left->left->value);
            string attNameWithRelName2 = string(currentAndList->left->left->right->value);

            string relName1 = attNameWithRelName1.substr(0, attNameWithRelName1.find('.'));
            string relName2 = attNameWithRelName2.substr(0, attNameWithRelName2.find('.'));

            if (relNamesSet.find(relName1) == relNamesSet.end()) {
                relNamesSet.insert(string(relName1));
                char *newRel = new char[relName1.length() + 1];
                strcpy(newRel, relName1.c_str());
                relNames[relNamesIndex++] = newRel;
            }

            if (relNamesSet.find(relName2) == relNamesSet.end()) {
                relNamesSet.insert(relName2);
                char *newRel = new char[relName2.length() + 1];
                strcpy(newRel, relName2.c_str());
                relNames[relNamesIndex++] = newRel;
            }

            double intermediate = dummy.Estimate(currentAndList, relNames, relNamesIndex);
            permutationIntermediateTuples += intermediate;
            dummy.Apply(currentAndList, relNames, relNamesIndex);

        }
        if (permutationIntermediateTuples < minIntermediateTuples) {
            minIntermediateTuples = permutationIntermediateTuples;
            minI = i;
        }
    }

    for (int i = 0; i < n; i++) {
        joins_arranged->push_back(joins->at(permutations[minI][i]));
    }
}

void HeapPermutation(int *a, int size, int n, vector<int *> *permutations) {
    // if size becomes 1 then prints the obtained
    // permutation
    if (size == 1) {
        // Add new Permutation in the permutations vector.
        int *newPermutation = new int[n];
        for (int i = 0; i < n; i++) {
            newPermutation[i] = a[i];
        }
        permutations->push_back(newPermutation);
        return;
    }

    for (int i = 0; i < size; i++) {
        HeapPermutation(a, size - 1, n, permutations);

        // if size is odd, swap first and last
        // element
        if (size % 2 == 1)
            swap(a[0], a[size - 1]);

            // If size is even, swap ith and last
            // element
        else
            swap(a[i], a[size - 1]);
    }
}

string QueryPlan::GetResultantGroupName() {
    if (groupNameToRelOpNode.size() != 1) {
        cerr << "Query is not correct. Group by can be performed on only one group\n";
        exit(1);
    }
    return groupNameToRelOpNode.begin()->first;
}

void QueryPlan::Print() {
    cout << "PRINTING TREE POST ORDER: " << "\n";
    PrintQueryPlanPostOrder(groupNameToRelOpNode[GetResultantGroupName()]);
}

void QueryPlan::PrintQueryPlanPostOrder(RelOpNode *node) {
    if (node == nullptr)
        return;

    PrintQueryPlanPostOrder(node->child1);
    PrintQueryPlanPostOrder(node->child2);
    node->Print();
}