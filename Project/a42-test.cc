
#include <iostream>
#include "ParseTreeLatest.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

int main () {
    yyparse();

    struct FuncOperator *finalFunctionMain = finalFunction;
    struct TableList *tablesMain = tables;
    struct AndList *booleanMain = boolean;
    struct NameList *groupingAttsMain = groupingAtts;
    struct NameList *attsToSelectMain = attsToSelect;
    int distinctAtts = distinctAtts;
    int distinctFunc = distinctFunc;

    while(tablesMain) {
        cerr << string(tablesMain->tableName) << "\n";
        tablesMain = tablesMain->next;
    }
}


