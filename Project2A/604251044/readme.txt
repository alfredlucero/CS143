Alfred Lucero
alfred.lucero@ucla.edu
ID: 604251044

Nicandro Vergara
nicandro@ucla.edu
ID: 804346386

PART 2A:
We modeled our load function after the select function. We created a new
table file if it doesn't already exist through open's 'w' mode, and we 
read in tuples from the load file through the use of fstream's getline and
the given SQLEngine's parseLoadLine. We handled errors by modeling the RC
checks after select's fprintf format. We did not know at first that ifstream
takes in a cstring in its constructor so we had to use the c_str function
to convert load file to the proper type. We tested it on bruinbase with
loading our own dummy files and recreating the movie table to test that the
SELECT commands in the bruinbase overview tutorial were not affected by our
load function.

PART 2B:
We followed the insertion algorithms for leaf node/nonleaf node overflow and
we worked off of the set-up given to us in BTreeNode.cc. We didn't get to
fully test it out, so the insertAndSplit functions for both types of nodes 
and the sorting the nodes have improper values. We were able to manually
test in SQLEngine.cc's run function and fix it eventually.

PART 2C:
We implemented the recursive insertion functions that build off of the
BTreeNode set-up in BTreeIndex to actually shape up the B+ Tree. We 
modified the load function to insert into the tree and we created our
own print function to test it out. Two levels appear to be working.

PART 2D:
We implemented the select function to use the index depending on the query
conditions, especially for ranges/equalities. When using invalid queries, 
it would not read all the pages anymores and ranges use less disk IOs. 
Using COUNT(*), for example, reads all pages in B+Tree and counts number 
of tuples. We experience bugs related to the xlarge data table with certain
queries that we were unable to fix. We figure that the select,
locate, or insertion functions may be points of interest to check for subtle
errors. Most of the queries work with performance gain, but larger data sets
may lead to improper results.
