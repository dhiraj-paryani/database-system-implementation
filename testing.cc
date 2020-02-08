#include "gtest/gtest.h"
#include "../Record.h"
#include "../DBFile.h"
#include "../test.h"
#include "../Constants.h"

const char *dbfile_dir = DBFILE_PATH;       // dir where binary heap files should be stored
const char *tpch_dir = TPCH_PATH;           // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = CATALOG_PATH;    // full path of the catalog file
DBFile dbfile;
relation *rel;
char tbl_path[2100] = "/Users/vaibhav/Documents/UF CISE/DBI/P1/table/lineitem.tbl";
//const char *table_path = DBFILE_PATH + ""

void setup() {
    setup(catalog_path, dbfile_dir, tpch_dir);
    relation *rel_ptr[] = {n, r, c, p, ps, o, li};
    rel = rel_ptr[0];
}


TEST(DBFile, CreateTestSuccess) {
    cout << " DBFile will be created at " << rel->path() << endl;
    int creation = dbfile.Create(rel->path(), heap, NULL);
    EXPECT_EQ(1, creation);
}

TEST(DBFile, CreateTestFailureOnFiletypeSorted) {
    cout << " DBFile will be created at " << rel->path() << endl;
    int creation = dbfile.Create(rel->path(), sorted, NULL);
    EXPECT_EQ(0, creation);
}

TEST(DBFile, CreateTestFailureOnFiletypeTree) {
    cout << " DBFile will be created at " << rel->path() << endl;
    int creation = dbfile.Create(rel->path(), tree, NULL);
    EXPECT_EQ(0, creation);
}

TEST(DBFile, CloseTestSuccess) {
    char tbl_path[2100] = "/Users/vaibhav/Documents/UF CISE/DBI/P1/table/nation.tbl"; // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    //dbfile.Load(*(rel->schema()), tbl_path);
    //EXPECT_EQ(0, dbfile.Close());
    EXPECT_GE(dbfile.Close(),0);
}

TEST(DBFile, CloseTestFailure) {
    //char tbl_path[2100] = "/Users/vaibhav/Documents/UF CISE/DBI/P1/table/lineitem.tbl"; // construct path of the tpch flat text file
    //sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    //dbfile.Load(*(rel->schema()), tbl_path);
    EXPECT_EQ(0, dbfile.Close());
}

TEST(DBFile, OpenTestSuccess) {
    const char *path = "temp/nation.bin";
    sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    //dbfile.Load(*(rel->schema()), tbl_path);
    EXPECT_EQ(1, dbfile.Open(path));
}

TEST(DBFile, OpenTestFailure) {
    const char *path = "temp/lineitem12.bin";
    sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    //dbfile.Load(*(rel->schema()), tbl_path);
    EXPECT_EQ(0, dbfile.Open(path));
}

TEST(DBFile, LoadTestFailure)
{
    char tbl_path[2100] = "/Users/arpi/Desktop/git/DBI-master/table/supplierx.tbl";
    EXPECT_EQ(-1, dbfile.Load(*(rel->schema()), tbl_path));
}

TEST(DBFile, LoadTestSuccess)
{

    EXPECT_EQ(0, dbfile.Load(*(rel->schema()), tbl_path));
}

int main(int argc, char *argv[]) {

    setup();
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}