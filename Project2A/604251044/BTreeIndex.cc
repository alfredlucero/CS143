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
#include <iostream>

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
		//fprintf(stderr, "Error: failed in opening index file in read/write mode");
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
	memcpy(buffer + 4, &treeHeight, sizeof(int));
	if ((rc = pf.write(0, buffer)) < 0)
	{
		//fprintf(stderr, "Error: failed in writing root/height metadata to disk");
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
	RC rc;
	
	// Initialize a new tree from the root if it doesn't exist yet
	if (treeHeight == 0)
	{
		// Insert key-rid pair into leaf node
		BTLeafNode rootTree;
		if ((rc = rootTree.insert(key, rid)) < 0)
		{
			fprintf(stderr, "Error: failed to insert into root leaf node");
			return rc;
		}
		
		// Set root to 1 if the file was just created; otherwise, set it to last pid available
		// pid = 0 is used to store metadata about the treeHeight and rootPid
		if (pf.endPid() == 0)
			rootPid = 1;
		else
			rootPid = pf.endPid();

		treeHeight++;

		// Write the tree into the pid of the PageFile
		if ((rc = rootTree.write(rootPid, pf)) < 0)
		{
			fprintf(stderr, "Error: failed to write new tree");
		}

		return rc;
	}

	// If the tree exists already, traverse it to find where to insert the key-rid pair
	// Begin after the metadata page 0 -> pidStart = 1; heightStart = 1 from the root
	int inKey;
	PageId inPid;
	if ((rc = insertPair(key, rid, rootPid, 1, inKey, inPid)) < 0)
	{
		fprintf(stderr, "Error: failed to insert pair into leaf node");
	}

	return rc;
}

RC BTreeIndex::insertPair(int key, const RecordId& rid, PageId curPid, int curHeight, int& inKey, PageId& inPid)
{
	RC rc;
	inKey = -1;
	inPid = -1;

	// Recursively traverse through the B+ tree until you reach the max height
	// At the max height we insert at the leaf node level
	if (curHeight == treeHeight)
	{
		// Read page contents into the leaf node
		BTLeafNode curLeaf;
		if ((rc = curLeaf.read(curPid, pf)) < 0)
		{
			fprintf(stderr, "Error: failed to read contents into leaf node (rec)");
			return rc;
		}

		// Attempt to insert into leaf node and write into the page file
		if ((rc = curLeaf.insert(key, rid)) == 0)
		{
			if ((rc = curLeaf.write(curPid, pf)) < 0)
			{
				fprintf(stderr, "Error: failed to write into leaf node (rec)");
				return rc;
			}

			return rc;
		}

		// Leaf overflow/insertion into leaf node unsuccessful, so try insertAndSplit algorithm
		BTLeafNode splitLeaf;
		int splitKey;
		if ((rc = curLeaf.insertAndSplit(key, rid, splitLeaf, splitKey)) < 0)
		{
			fprintf(stderr, "Error: failed to insert and split the leaf node (rec)");
			return rc;
		}

		// The splitKey (median key) must propagate up to the parent
		// Set sibling pointers accordingly for splitLeaf and curLeaf
		int endPid = pf.endPid();
		inKey = splitKey;
		inPid = endPid;
		splitLeaf.setNextNodePtr(curLeaf.getNextNodePtr());
		curLeaf.setNextNodePtr(endPid);

		// Write the splitLeaf's contents into the last pid area
		if ((rc = splitLeaf.write(endPid, pf)) < 0)
		{
			fprintf(stderr, "Error: failed to write split leaf's contents (rec)");
			return rc;
		}

		// Re-write the curLeaf's modified contents into its pid
		if ((rc = curLeaf.write(curPid, pf)) < 0)
		{
			fprintf(stderr, "Error: failed to re-write current leaf's contents (rec)");
			return rc;
		}

		// Splitting a root requires a new non-leaf node 
		// The splitLeaf/sibling's first value propagates up to the root
		if (treeHeight == 1)
		{
			// Initialize the root with the splitKey (median key) pushed up
			// It has two pid references to the split leaf nodes
			BTNonLeafNode root;
			root.initializeRoot(curPid, splitKey, endPid);
			
			// Write the root into the rootPid page
			rootPid = pf.endPid();
			if ((rc = root.write(rootPid, pf)) < 0)
			{
				fprintf(stderr, "Error: failed to write root contents (rec)");
				return rc;
			}
			
			treeHeight++;
		}
	}
	else
	{
		// Let's continue to traverse down the B+ tree properly while we haven't reached leaf level
		// Read nonLeaf node's contents into the page
		BTNonLeafNode nonLeaf;
		if ((rc = nonLeaf.read(curPid, pf)) < 0)
		{
			fprintf(stderr, "Error: failed to read nonleaf node's contents (rec)");
			return rc;
		}

		// Find the child node for the key
		PageId childPid;
		nonLeaf.locateChildPtr(key, childPid);

		// Keep going through the tree to insert at node's closer to leaf level
		inKey = -1;
		inPid = -1;
		rc = insertPair(key, rid, childPid, curHeight + 1, inKey, inPid);

		// If the node was split, propagate the median key to the parent
		if (!(inKey == -1 && inPid == -1))
		{
			// Insert median key into nonleaf node parent
			if ((rc = nonLeaf.insert(inKey, inPid)) == 0)
			{
				if ((rc = nonLeaf.write(curPid, pf)) < 0)
				{
					fprintf(stderr, "Error: failed to write nonleaf node's contents after insert (rec)");
					return rc;
				}

				return rc;
			}

			// Failed to insert into nonleaf node parent due to overflow
			// Insert and split the nonleaf node to push median key to next parent
			BTNonLeafNode splitNonLeaf;
			int splitKey;
			if ((rc = nonLeaf.insertAndSplit(inKey, inPid, splitNonLeaf, splitKey)) < 0)
			{
				fprintf(stderr, "Error: failed to split nonleaf node (rec)");
				return rc;
			}

			int endPid = pf.endPid();
			inKey = splitKey;
			inPid = endPid;

			// Re-write modified nonLeaf node's contents
			if ((rc = nonLeaf.write(curPid, pf)) < 0)
			{
				fprintf(stderr, "Error: failed to write nonleaf node's contents after split (rec)");
				return rc;
			}

			// Write splitNonLeaf node's contents 
			if ((rc = splitNonLeaf.write(endPid, pf)) < 0)
			{
				fprintf(stderr, "Error: failed to write split nonleaf node's contents (rec)");
				return rc;
			}

			// Splitting a root requires a new non-leaf node 
			// The splitNonLeaf/sibling's first value propagates up to the root
			if (treeHeight == 1)
			{
				// Initialize the root with the splitKey (median key) pushed up
				// It has two pid references to the split modes
				BTNonLeafNode root;
				root.initializeRoot(curPid, splitKey, endPid);

				// Write the root into the rootPid page
				rootPid = pf.endPid();
				if ((rc = root.write(rootPid, pf)) < 0)
				{
					fprintf(stderr, "Error: failed to write root contents (rec)");
					return rc;
				}

				treeHeight++;
			}
		}	
	}

	return rc;
}
/**
 * Run the standard B+Tree key search algorithm and identify the
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

void BTreeIndex::print()
{
	if (treeHeight == 1)
	{
		BTLeafNode leafRoot;
		leafRoot.read(rootPid, pf);
		leafRoot.print();
	}
	else if (treeHeight > 1)
	{
		BTNonLeafNode root;
		root.read(rootPid, pf);
		root.print();

		PageId zero, remaining;
		memcpy(&zero, root.buffer, sizeof(PageId));
		cout << zero << endl; // first four bytes of nonleaf root is pid0 less than median key pushed up from leaf overflow
		BTLeafNode leafZero, leaf;
		leafZero.read(zero, pf);
		leafZero.print();
	
		// Loop through remaining leaf nodes
		for(int i = 4; i < root.getKeyCount()*8; i += 8)
		{
			// pid1 (greater than or equal to median key pushed up) begins at position 8 (pid0,key1,pid1,...) each entity is 4 bytes
			memcpy(&remaining, root.buffer + i + 4, sizeof(PageId));
			cout << remaining << endl;
			leaf.read(remaining, pf);
			leaf.print();
		}
		
		// Loop through leaf nodes and print pid and next pid
		cout << "---------------" << endl;
		cout << "leaf 0 pid = " << zero << ", getNextNodePtr = " << leafZero.getNextNodePtr() << endl;
		int leafNum = 1;
		for(int i = 4; i < root.getKeyCount()*8; i += 8)
		{
			BTLeafNode tleaf;
			PageId tpid;
			memcpy(&tpid, root.buffer + i + 4, sizeof(PageId));
			cout << tpid << endl;
			tleaf.read(tpid, pf);;
			
			cout << "leaf " << leafNum << " pid = " << tpid << ", getNextNodePtr = " << tleaf.getNextNodePtr() << endl;
			leafNum++;
		}
		
	}	
}
