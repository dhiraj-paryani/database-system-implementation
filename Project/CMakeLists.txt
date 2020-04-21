cmake_minimum_required(VERSION 3.15)
project(Project)

set(CMAKE_CXX_STANDARD 11)

include_directories(.)

add_executable(Project
        BigQ.cc
        BigQ.h
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        DBFile.cc
        DBFile.h
        Defs.h
        File.cc
        File.h
        ParseTree.h
        Pipe.cc
        Pipe.h
        Record.cc
        Record.h
        Schema.cc
        Schema.h
        TwoWayList.cc
        TwoWayList.h
        GenericDBFile.cc
        GenericDBFile.h
        HeapDBFile.cc
        HeapDBFile.h
        SortedDBFile.cc
        SortedDBFile.h
        Statistics.cc
        Statistics.h
        RelOp.cc
        RelOp.h
        Function.cc
        Function.h
        a3-test.cc
        a3-test.h
        Lexer.l
        Lexerfunc.l)
