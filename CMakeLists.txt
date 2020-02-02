cmake_minimum_required(VERSION 3.15)
project(database_system_implementation)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(database_system_implementation
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        DBFile.cc
        DBFile.h
        Defs.h
        File.cc
        File.h
        lex.yy.c
        main.cc
        ParseTree.h
        Record.cc
        Record.h
        Schema.cc
        Schema.h
        TwoWayList.cc
        TwoWayList.h
        test.h
        test.cc
        y.tab.o)
