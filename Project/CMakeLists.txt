cmake_minimum_required(VERSION 3.15)
project(Project)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(Project
        BigQ.cc
        BigQ.h
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        Database.cc
        Database.h
        DBFile.cc
        DBFile.h
        Defs.h
        File.cc
        File.h
        Function.cc
        Function.h
        GenericDBFile.cc
        GenericDBFile.h
        HeapDBFile.cc
        HeapDBFile.h
        lex.yy.c
        ParseFunc.h
        ParseTree.h
        PathConfig.cc
        PathConfig.h
        Pipe.cc
        Pipe.h
        QueryPlan.cc
        QueryPlan.h
        QueryRunner.cc
        QueryRunner.h
        Record.cc
        Record.h
        RelOp.cc
        RelOp.h
        RelOpPlanNode.cc
        RelOpPlanNode.h
        Schema.cc
        Schema.h
        SortedDBFile.cc
        SortedDBFile.h
        Statistics.cc
        Statistics.h
        test.cc
        TwoWayList.cc
        TwoWayList.h
        y.tab.c
        y.tab.h)
