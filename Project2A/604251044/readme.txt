Alfred Lucero
ID: 604251044

Nicandro Vergara
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
