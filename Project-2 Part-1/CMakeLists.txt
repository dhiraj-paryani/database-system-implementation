cmake_minimum_required(VERSION 3.15)
project(Project_2_Part_1)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(gtest)

add_executable(Project_2_Part_1
        gtest/gtest-all.cc
        gtest/gtest.h
        gtest/gtest_main.cc
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        DBFile.cc
        DBFile.h
        DBFileGTests.cc
        Defs.h
        File.cc
        File.h
        main.cc
        ParseTree.h
        Record.cc
        Record.h
        Schema.cc
        Schema.h
        test.cc
        test.h
        TwoWayList.cc
        TwoWayList.h)
