cmake_minimum_required(VERSION 3.15)
project(Project)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(Project
        gtests/BigQGTests.cc
        gtests/DBFileGTests.cc
        a1-test.cc
        a1-test.h
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
        RelOp.cc
        RelOp.h
        Function.cc
        Function.h
        test.cc
        test.h)
