/**
* Copyright (C) 2008 by The Regents of the University of California
* Redistribution of this file is permitted under the terms of the GNU
* Public License (GPL).
*
* @author Junghoo "John" Cho <cho AT cs.ucla.edu>
* @date 3/24/2008
*/

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeNode.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
	/*
	// Debugging


	// RecordId trid, trid2, trid3;
	// trid.pid = 0;
	// trid.sid = 0;
	// trid2.pid = 0;
	// trid2.sid = 1;
	// trid3.pid = 0;
	// trid3.sid = 2;

	BTNonLeafNode test1 = BTNonLeafNode();
	BTNonLeafNode test2 = BTNonLeafNode();
	// test.insert(272, trid); //272,"Baby Take a Bow"
	// //test.print();

	// test.insert(216, trid2);
	// test.insert(298, trid3);
	// test.print();


	PageId tpid1 = 0;
	PageId tpid2 = 1;
	RC rc;

	for (int i = 0; i < 162; i += 2)
	{
		rc = test1.insert(i + 100, tpid1);
		if (rc < 0)
		{
			int key;
			// test.setNextNodePtr(trid2.pid);
			// test2.setNextNodePtr();

			test1.insertAndSplit(179, tpid1, test2, key);

			cout << rc << endl;
			cout << key << endl;
		}
	}

	test1.print();
	cout << endl;
	test2.print();
	*/

	// Original run()
	
	fprintf(stdout, "Bruinbase> ");
	// set the command line input and start parsing user input
	sqlin = commandline;
	sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
	// SqlParser.y by bison (bison is GNU equivalent of yacc)
	return 0;
	
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
	RecordFile rf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning
	BTreeIndex bTree; // B+Tree to hold index
	IndexCursor cursor; // Cursor to traverse the B+Tree

	RC     rc;
	int    key;
	string value;
	int    count;
	int    diff;

	// open the table file
	if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
		fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
		return rc;
	}

	SelCond tempCond; // stores selection conditions when checking cond vector
	bool hasIndex = false; // to check for closing the tree file later if we have B+tree index
	bool hasKeyCond = false; // to check for key conditions
	bool hasValueCond = false; // to check for value conditions

	int equalValue = -1; // keeps track of equality conditions for keys
 	int minKey = -1; 
	int maxKey = -1;
	bool geCond = false; // true -> key >= minKey
	bool leCond = false; // true -> key <= maxKey
	bool wrongValue = false;
	string valueCheck = "";

	int condPos = 0;
	int numCond = cond.size();
	while (condPos < numCond)
	{
		tempCond = cond[condPos];
		int tempValue = atoi(tempCond.value); 
		// EQ, NE, LT, GT, LE, GE
		// Check conditions on keys other than NE
		if (tempCond.attr == 1 && tempCond.comp != SelCond::NE)
		{
			hasKeyCond = true; // found a valid key condition
			if (tempCond.comp == SelCond::EQ) // key = curKey, break out for select condition
			{
				equalValue = tempValue;
				break;
			}
			else if (tempCond.comp == SelCond::LT) // key < maxKey
			{
				if (maxKey == -1 || tempValue <= maxKey)
				{
					maxKey = tempValue;
					leCond = false;
				}
			}
			else if (tempCond.comp == SelCond::GT) // key > minKey
			{
				if (minKey == -1 || tempValue >= minKey)
				{
					minKey = tempValue;
					geCond = false;
				}

			}
			else if (tempCond.comp == SelCond::LE) // key <= maxKey
			{
				if (maxKey == -1 || tempValue < maxKey)
				{
					maxKey = tempValue;
					leCond = true;
				}
			}
			else if (tempCond.comp == SelCond::GE)
			{
				if (minKey == -1 || tempValue > minKey)
				{
					minKey = tempValue;
					geCond = true;
				}
			}
		} // check value conditions
		else if (tempCond.attr == 2) 
		{
			hasValueCond = true;

			// check matching value
			if (tempCond.comp == SelCond::EQ)
			{
				if (valueCheck == "" || strcmp(value.c_str(), tempCond.value) == 0)
					valueCheck = tempValue;
				else
					wrongValue = true;
			}
		}

		condPos++;
	}

	// Check for early select aborts
	if ((maxKey != -1 && minKey != -1 && maxKey < minKey) || wrongValue)
		goto abort_select;

	/*
	if (!geCond && !leCond && maxKey != -1 && minKey != -1 && maxKey == minKey)
		goto abort_select;
	*/
	// Use normal select if no index tree or when using count(*) without conditions
	if ((rc = bTree.open(table + ".idx", 'r') != 0) || (attr != 4 && !hasKeyCond))
	{
		// scan the table file from the beginning
		rid.pid = rid.sid = 0;
		count = 0;
		while (rid < rf.endRid()) {
			// read the tuple
			if ((rc = rf.read(rid, key, value)) < 0) {
				fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
				goto exit_select;
			}

			// check the conditions on the tuple
			for (unsigned i = 0; i < cond.size(); i++) {
				// compute the difference between the tuple value and the condition value
				switch (cond[i].attr) {
				case 1:
					diff = key - atoi(cond[i].value);
					break;
				case 2:
					diff = strcmp(value.c_str(), cond[i].value);
					break;
				}

				// skip the tuple if any condition is not met
				switch (cond[i].comp) {
				case SelCond::EQ:
					if (diff != 0) goto next_tuple;
					break;
				case SelCond::NE:
					if (diff == 0) goto next_tuple;
					break;
				case SelCond::GT:
					if (diff <= 0) goto next_tuple;
					break;
				case SelCond::LT:
					if (diff >= 0) goto next_tuple;
					break;
				case SelCond::GE:
					if (diff < 0) goto next_tuple;
					break;
				case SelCond::LE:
					if (diff > 0) goto next_tuple;
					break;
				}
			}

			// the condition is met for the tuple. 
			// increase matching tuple counter
			count++;

			// print the tuple 
			switch (attr) {
			case 1:  // SELECT key
				fprintf(stdout, "%d\n", key);
				break;
			case 2:  // SELECT value
				fprintf(stdout, "%s\n", value.c_str());
				break;
			case 3:  // SELECT *
				fprintf(stdout, "%d '%s'\n", key, value.c_str());
				break;
			}

			// move to the next tuple
		next_tuple:
			++rid;
		}
	}
	else // use the index file
	{
		rid.pid = rid.sid = 0;
		count = 0;
		hasIndex = true;

		// Set cursor position
		if (equalValue != -1) // key is equal to certain value
			bTree.locate(equalValue, cursor);
		else if (minKey != -1 && geCond)
			bTree.locate(minKey, cursor);
		else if (minKey != -1 && !geCond)
			bTree.locate(minKey + 1, cursor);
		else
			bTree.locate(0, cursor);

		// Traverse through tree
		while (bTree.readForward(cursor, key, rid) == 0)
		{
			if (!hasValueCond && attr == 4)
			{
				// Key equality failure
				if (equalValue != -1 && key != equalValue)
					goto abort_select;

				// check GT or GE failures
				if (minKey != -1)
				{
					if (geCond && key < minKey)
						goto abort_select;
					else if (!geCond && key <= minKey)
						goto abort_select;
				}

				// check LE or LT failures
				if (maxKey != -1)
				{
					if (leCond && key > maxKey)
						goto abort_select;
					else if (!leCond && key >= maxKey)
						goto abort_select;
				}

				// checking for failures as we traverse saves from extraneous reads
				count++;
				continue;
			}

			// read the tuple
			if ((rc = rf.read(rid, key, value)) < 0) {
				fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
				goto exit_select;
			}

			// check the conditions on the tuple
			for (unsigned i = 0; i < cond.size(); i++) {
				// compute the difference between the tuple value and the condition value
				switch (cond[i].attr) {
				case 1:
					diff = key - atoi(cond[i].value);
					break;
				case 2:
					diff = strcmp(value.c_str(), cond[i].value);
					break;
				}

				// skip the tuple if any condition is not met
				// after LE,LT,EQ fails, abort select since index tree is sorted
				switch (cond[i].comp) {
				case SelCond::EQ:
					if (diff != 0)
					{
						if (cond[i].attr == 1)
							goto abort_select;
						goto continue_loop;
					}
					break;
				case SelCond::NE:
					if (diff == 0) goto continue_loop;
					break;
				case SelCond::GT:
					if (diff <= 0) goto continue_loop;
					break;
				case SelCond::LT:
					if (diff >= 0)
					{
						if (cond[i].attr == 1)
							goto abort_select;
						goto continue_loop;
					}
					break;
				case SelCond::GE:
					if (diff < 0) goto continue_loop;
					break;
				case SelCond::LE:
					if (diff > 0)
					{
						if (cond[i].attr == 1)
							goto abort_select;
						goto continue_loop;
					}
					break;
				}
			}

			// the condition is met for the tuple. 
			// increase matching tuple counter
			count++;

			// print the tuple 
			switch (attr) {
			case 1:  // SELECT key
				fprintf(stdout, "%d\n", key);
				break;
			case 2:  // SELECT value
				fprintf(stdout, "%s\n", value.c_str());
				break;
			case 3:  // SELECT *
				fprintf(stdout, "%d '%s'\n", key, value.c_str());
				break;
			}

			// move to the next tuple
		continue_loop:
			cout << "";

		}
	}

	// if we fail in finding some tuples based on the select conditions, break out of select early
abort_select:

	// print matching tuple count if "select count(*)"
	if (attr == 4) {
		fprintf(stdout, "%d\n", count);
	}
	rc = 0;

	// close the table file and return
exit_select:
	if (hasIndex)
		bTree.close();
	rf.close();
	return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
	RecordFile rf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning
	BTreeIndex bTree; // B+ tree to hold index

	RC     rc;
	int    key;
	string value;
	string line;
	int linecount = 1;

	// create the table file if it doesn't exist
	if ((rc = rf.open(table + ".tbl", 'w')) < 0) {
		fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
		return rc;
	}

	// open the load file and parse line by line
	// insert the tuples into the table file
	ifstream infile(loadfile.c_str());
	if (index)
	{
		// Open index file
		if ((rc = bTree.open(table + ".idx", 'w')) < 0)
		{
			return rc;
		}

		while (getline(infile, line))
		{
			if ((rc = parseLoadLine(line, key, value)) < 0)
			{
				fprintf(stderr, "Error: table %s could not parse line %d \n", table.c_str(), linecount);
				return rc;
			}
			if ((rc = rf.append(key, value, rid)) < 0)
			{
				fprintf(stderr, "Error: table %s could not append line %d \n", table.c_str(), linecount);
				return rc;
			}

			// Insert key-rid pair into bTree to index
			if ((rc = bTree.insert(key, rid)) < 0)
			{
				return rc;
			}

			linecount++;
		}
		//bTree.print();
		// Close index tree file
		bTree.close();
	}
	else
	{
		while (getline(infile, line))
		{
			if ((rc = parseLoadLine(line, key, value)) < 0)
			{
				fprintf(stderr, "Error: table %s could not parse line %d \n", table.c_str(), linecount);
				return rc;
			}
			if ((rc = rf.append(key, value, rid)) < 0)
			{
				fprintf(stderr, "Error: table %s could not append line %d \n", table.c_str(), linecount);
				return rc;
			}

			linecount++;
		}
	}

	infile.close();
	rf.close();
	return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
	const char *s;
	char        c;
	string::size_type loc;

	// ignore beginning white spaces
	c = *(s = line.c_str());
	while (c == ' ' || c == '\t') { c = *++s; }

	// get the integer key value
	key = atoi(s);

	// look for comma
	s = strchr(s, ',');
	if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

	// ignore white spaces
	do { c = *++s; } while (c == ' ' || c == '\t');

	// if there is nothing left, set the value to empty string
	if (c == 0) {
		value.erase();
		return 0;
	}

	// is the value field delimited by ' or "?
	if (c == '\'' || c == '"') {
		s++;
	}
	else {
		c = '\n';
	}

	// get the value string
	value.assign(s);
	loc = value.find(c, 0);
	if (loc != string::npos) { value.erase(loc); }

	return 0;
}
