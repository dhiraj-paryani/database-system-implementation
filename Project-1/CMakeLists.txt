cmake_minimum_required(VERSION 3.15)
project(dbi)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(gtest)

add_executable(dbi
        gtest/gtest-all.cc
        gtest/gtest.h
        gtest/gtest_main.cc
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
        test.cc
        test.h
        TwoWayList.cc
        TwoWayList.h
        y.tab.c
        y.tab.h)
