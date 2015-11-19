/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
	// Set default values: rootPid 1 is the smallest page that a root can start
	// Upon adding a root node, minimum valid treeHeight is 1
    rootPid = -1;
	treeHeight = 0;
	memset(buffer, 0, sizeof(buffer));
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC rc;

	// Open the PageFile
	if ((rc = pf.open(indexname, mode)) < 0)
	{
		fprintf(stderr, "Error: failed in opening index file in read/write mode");
		return rc;
	}

	// If file is open for the first time, initialize everything again
	if (pf.endPid() == 0)
	{
		rootPid = -1;
		treeHeight = 0;

		if ((rc = pf.write(0, buffer)) < 0)
		{
			fprintf(stderr, "Error: failed to write to index file");
			return rc;
		}

		return rc;
	}

	// Read in metadata saved in page 0
	if ((rc = pf.read(0, buffer)) < 0)
	{
		fprintf(stderr, "Error: failed to load page 0 metadata");
		return rc;
	}

	// Load pid and height into index variables
	// Pid cannot be 0 (stored for metadata) or negative, height must be positive
	int loadPid;
	int loadHeight;
	memcpy(&loadPid, buffer, sizeof(int));
	memcpy(&loadHeight, buffer + 4, sizeof(int));
	if (loadPid > 0 && loadHeight >= 0)
	{
		rootPid = loadPid;
		treeHeight = loadHeight;
	}

	return rc;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	RC rc;

	// Save information related to rootPid and treeHeight in Page 0
	memcpy(buffer, &rootPid, sizeof(int));
	memcpy(buffer, &treeHeight, sizeof(int));
	if ((rc = pf.write(0, buffer)) < 0)
	{
		fprintf(stderr, "Error: failed in writing root/height metadata to disk");
		return rc;
	}
	
	// If it successfully wrote Page 0 metadata into disk
	// Close the index file
	if ((rc = pf.close()) < 0)
	{
		fprintf(stderr, "Error: failed to close the index file");
	}

	return rc;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	RC rc;
	BTNonLeafNode nonLeafNode;
	BTLeafNode leafNode;
	
	// Traverse down B+ tree by following the child pointers given the searchKey
	// Keep going down until the leaf node area based on the searchKey
	int curHeight = 1;
	PageId nextChild = rootPid;
	while (curHeight != treeHeight)
	{
		// Read in nonleaf node data
		if ((rc = nonLeafNode.read(nextChild, pf)) < 0)
		{
			fprintf(stderr, "Error: failed to read in nonleaf node data");
			return rc;
		}

		// Traverse down to the next level/child based on searchKey
		if ((rc = nonLeafNode.locateChildPtr(searchKey, nextChild)) < 0)
		{
			fprintf(stderr, "Error: failed to locate next child");
			return rc;
		}

		curHeight++;
	}

	// We have now reached a leaf node that may have the searchKey, so attempt to locate it
	// First, read in the leaf node data
	if ((rc = leafNode.read(nextChild, pf)) < 0)
	{
		fprintf(stderr, "Error: failed to read in leaf node data");
		return rc;
	}

	// Locate and extract the eid of the searchKey if it exists in the leaf node
	int eid;
	if ((rc = leafNode.locate(searchKey, eid)) < 0)
	{
		fprintf(stderr, "Error: failed to locate the searchKey in the leaf node");

		// Set the IndexCursor to proper fields
		// (index entry immediately after the largest index key that is smaller than searchKey and current pid)
		cursor.eid = eid;
		cursor.pid = nextChild;

		return rc;
	}

	// Set the IndexCursor to the proper fields (extracted eid and current pid)
	cursor.eid = eid;
	cursor.pid = nextChild;

	return rc;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	RC rc;
	BTLeafNode leafNode;

	// Read in data from the leaf node
	if ((rc = leafNode.read(cursor.pid, pf)) < 0)
	{
		fprintf(stderr, "Error: failed to read in leaf node data");
		return rc;
	}

	// Read in key-rid pair from eid value
	if ((rc = leafNode.readEntry(cursor.eid, key, rid)) < 0)
	{
		fprintf(stderr, "Error: failed to read in key-rid pair from eid");
		return rc;
	}

	// Cursor's page id is out of range
	if (cursor.pid <= 0)
		return RC_INVALID_CURSOR;
	
	// Move forward the cursor to the next entry
	if (cursor.eid + 1 < leafNode.getKeyCount())
	{
		cursor.eid++;
	}
	else // If it goes beyond the maximum eid entry, reset the eid to 0 and advance to the next page
	{
		cursor.eid = 0;
		cursor.pid = leafNode.getNextNodePtr();
	}

	return rc;
}


PageId BTreeIndex::getRoot()
{
	return rootPid;
}

int BTreeIndex::getHeight()
{
	return treeHeight;
}
