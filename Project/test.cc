#include "Database.h"

#include <iostream>

using namespace std;

int main() {
    Database database;

    database.TurnOn();

    cout << "Database Turned Up Successfully\n";

    int input = 0;
    while(input != 2) {
        cout << " select option: \n";
        cout << " \t 1. Execute a query \n";
        cout << " \t 2. Exit \n";
        cin >> input;
        if (input == 1) {
            cout << "Enter Query: (when done press ctrl-D):\n";

            database.ExecuteQuery();
        }
    }

    database.ShutDown();

    cout << "Database Shut Down Successfully\n";
}