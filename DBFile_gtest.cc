#include "gtest/gtest.h"
#include <iostream>
#include "DBFile.h"
#include "test.h"
#include <ctime>
#include <fstream>

char *dbfile_dir = "";
char *tpch_dir ="tpch-dbgen/";
char *catalog_path = "catalog";

using namespace std;

relation *rel;

class DBFileTest : public ::testing::Test
{};

TEST_F  (DBFileTest,  producerReadTest) {
  fstream f1, f2;
  char c1, c2;
  int counter1 = 0, counter2 = 0;
  rel = new relation (region, new Schema (catalog_path, region), dbfile_dir);
  CNF cnf;
	Record literal;
	ofstream outfile;
	outfile.open("query.txt");
  outfile << "";
  outfile.close();
	rel->get_cnf_test (cnf, literal,"(r_name = 'EUROPE')");
	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();
	Record temp;
	int counter = 0;
	clock_t begin = clock();
	while (dbfile.GetNext (temp, cnf, literal) == 1) {
		counter += 1;
		temp.Print (rel->schema());
		if (counter % 10000 == 0) {
			cout << counter << "\n";
		}
	}
	clock_t end = clock();
	double total_time = double(end-begin)/CLOCKS_PER_SEC;
	double scan_time = total_time ;
	cout << " selected " << counter << " recs \n";
	cout << " query time : " << scan_time << " secs\n";
	dbfile.Close ();
  f1.open("query.txt",ios::in);
  f2.open("q1_2.txt",ios::in);
  int flag = 1;
  while(1){
    c1=f1.get();
    c2=f2.get();
    if(c1!=c2){
        flag=0;
        break;
    }
    if((c1==EOF)||(c2==EOF))
        break;
  }
  f1.close();
  f2.close();
  EXPECT_EQ(flag, 1);
}

// TEST (DBFileTest, load_negative) {
//   DBFile dbfile = DBFile();
//   dbfile.Create("/cise/homes/aymittal/P1/region.bin", heap, NULL);
//   dbfile.Open("/cise/homes/aymittal/P1/region.bin");
//   EXPECT_DEATH(dbfile.Load(*(new Schema ("/cise/homes/aymittal/P1/catalog", "nation")),  "/cise/homes/aymittal/P1/tsble_files/nation.tbl"),"(.*?)");
//   dbfile.Close();
// }
//
// TEST (DBFileTest, load_positive) {
//   DBFile dbfile = DBFile();
//   dbfile.Create("/cise/homes/aymittal/P1/partsupp.bin", heap, NULL);
//   dbfile.Open("/cise/homes/aymittal/P1/partsupp.bin");
//   EXPECT_NO_THROW(dbfile.Load(*(new Schema ("/cise/homes/aymittal/P1/catalog", "partsupp")),  "/cise/homes/aymittal/tpch-dbgen/partsupp.tbl"));
//   dbfile.Close();
// }
