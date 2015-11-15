#include "BTreeNode.h"
#include <iostream>
#include <cstring>
#include <cstdlib>

using namespace std;

/**
* Constructor: initialize empty leaf node
*/
BTLeafNode::BTLeafNode()
{
	m_numKeys = 0;
	memset(buffer, 0, sizeof(buffer));
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	RC rc;

	// Use PageFile read to get the node's contents from the page pid in the Pagefile pf to main memory
	rc = pf.read(pid, buffer);
	
	return rc;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC rc;

	// Use PageFile write to put the node's contents to the page pid in the Pagefile pf to disk
	rc = pf.write(pid, buffer);

	return rc;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	int keyCount = 0;
	int kvPairSize = sizeof(int) + sizeof(RecordId);
	char * tempBuffer = buffer;

	// Each key-RecordId pair is 12 bytes
	// Max number of keys: 80; Max key-value pair byte usage: 80*12=960
	// Largest possible index of pair to be inserted: 960-12=948
	// Loop through the buffer to check the number of keys
	for (int n = 0; n <= 948; n += kvPairSize)
	{
		// Check if the nth key is valid and not null (== 0), meaning it was not empty
		int nKey;
		memcpy(&nKey, tempBuffer, sizeof(int));
		if (nKey == 0)
			break;

		// Increment key count for every valid key and iterate to next key-value pair
		keyCount++;
		tempBuffer += kvPairSize;
	}

	return keyCount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	RC rc;
	PageId nextNode = getNextNodePtr();
	int kvPairSize = sizeof(int) + sizeof(RecordId);
	int maxKVPairs = 80;

	// If adding one more key-rid pair overflows the node's max capacity (80 pairs)
	// return that the node is full
	if (getKeyCount() + 1 > maxKVPairs)
	{
		rc = RC_NODE_FULL;
	}
	else
	{
		// Check where to insert the key-rid pair into the node
		char * tempBuffer = buffer;
		
		int idx;
		for (idx = 0; idx < 948; idx += kvPairSize)
		{
			int nKey;
			memcpy(&nKey, tempBuffer, sizeof(int));
			
			// Check if the key is less than one of the node's keys or if the node's key is empty
			// the index will be where to insert the key-value pair after shifting everything over 
			if (nKey == 0 || key <= nKey)
				break;

			tempBuffer += kvPairSize;
		}

		// After finding the index location (idx) to insert the key-rid pair
		// let's use another buffer to shift everything over
		char * insertBuffer = (char *)malloc(PageFile::PAGE_SIZE);
		memset(insertBuffer, 0, sizeof(insertBuffer));
		
		// Make sure the insertBuffer has all the values up until the index insert location
		memcpy(insertBuffer, buffer, idx);

		// Insert key-value pair into proper index location
		memcpy(insertBuffer + idx, &key, sizeof(int));
		memcpy(insertBuffer + idx + sizeof(int), &rid, sizeof(RecordId));

		// Copy the rest of the values after the index insert location to shift everything over
		// Also put in the nextNode in the last four bytes of the insertBuffer
		memcpy(insertBuffer + idx + kvPairSize, buffer + idx, getKeyCount() * kvPairSize - idx);
		memcpy(insertBuffer + PageFile::PAGE_SIZE - sizeof(PageId), &nextNode, sizeof(PageId));

		// Update the old buffer to the insertBuffer with properly inserted key-rid pair
		// Deallocate the buffer that aided the insertion
		memcpy(buffer, insertBuffer, PageFile::PAGE_SIZE);
		free(insertBuffer);

		m_numKeys++;
		rc = 0;
	}

	return rc;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	RC rc;
	PageId nextNode = getNextNodePtr();
	int kvPairSize = sizeof(int) + sizeof(RecordId);
	int maxKVPairs = 80;

	// Node must be full before performing leaf overflow split algorithm
	if (!(getKeyCount() + 1 > maxKVPairs))
	{
		rc = RC_INVALID_FILE_FORMAT;
	} // The new sibling to split key-rid pairs with must be empty first before proceeding
	else if (sibling.getKeyCount() != 0)
	{
		rc = RC_INVALID_ATTRIBUTE;
	}
	else
	{
		// Clear sibling before adding the other half of the keys into it
		memset(sibling.buffer, 0, sizeof(sibling.buffer));

		// Find the number of half keys and the position to split the the node in two
		int halfKeys = ((int)((getKeyCount() + 1) / 2));
		int halfPos = halfKeys * kvPairSize;

		// Copy second half of original node's pairs into sibling's node
		// 1024 - 4 (nextnodeptr) - 60 (filler between nextnodeptr and max total pair index: 960) - halfPos (up to 480)
		// Set the number of keys and its next node pointer properly
		memcpy(sibling.buffer, buffer + halfPos, PageFile::PAGE_SIZE - sizeof(PageId) - 60 - halfPos);
		sibling.m_numKeys = getKeyCount() - halfKeys;
		sibling.setNextNodePtr(getNextNodePtr());

		// Clear second half of the original node's buffer up to the pid
		memset(buffer + halfPos, 0, PageFile::PAGE_SIZE - sizeof(PageId) - halfPos);
		m_numKeys = halfKeys;

		// Check whether to insert the key-rid pair into the first half or the second half in the sibling
		int firstSiblingKey;
		memcpy(&firstSiblingKey, sibling.buffer, sizeof(int));

		// If the key to insert is less than the first key in the second half
		// insert into the first half
		if (key < firstSiblingKey)
			insert(key, rid);
		else // otherwise, insert into the sibling/second half
			sibling.insert(key, rid);

		// Copy the first key in the sibling node after the split into siblingKey
		memcpy(&siblingKey, sibling.buffer, sizeof(int));

		/// Can also get rid from sibling.buffer+sizeof(int)

		rc = 0;
	}

	return rc;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	RC rc;
	int kvPairSize = sizeof(int) + sizeof(RecordId);
	char * tempBuffer = buffer;

	int n;
	for (n = 0; n < getKeyCount()*kvPairSize; n += kvPairSize)
	{
		int nKey;
		memcpy(&nKey, tempBuffer, sizeof(int));
		if (nKey == searchKey)
		{
			// if searchKey exists, then set the eid to the index entry of the searchKey
			eid = n / kvPairSize;
			rc = 0;

			break;
		} 
		else if (nKey > searchKey)
		{
			// if index key is greater than the search key, set eid to the largest index key smaller than the searchKey
			eid = (n - 1) / kvPairSize;
			rc = RC_NO_SUCH_RECORD;
			break;
		}
		
		tempBuffer += kvPairSize;
	}

	// All of the keys were less than the search key so set eid to the last index entry of the buffer
	if (n == getKeyCount())
	{
		eid = (n - 1) / kvPairSize;
		rc = RC_NO_SUCH_RECORD;
	}

	return rc;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	RC rc;

	// If entry number is out of range, return error
	if (eid < 0 || eid >= getKeyCount())
	{
		rc = RC_NO_SUCH_RECORD;
	}
	else
	{
		// Size of key-value pair is sizeof(RecordId) = 8 bytes + sizeof(key) = 4 bytes, so 12 bytes
		int kvPairSize = sizeof(int) + sizeof(RecordId);
		
		// Compute the offset of the kvPair from the beginning of the buffer
		int offset = eid * kvPairSize;

		char * tempBuffer = buffer;

		// Copy key-RecordId pair data into function parameters
		memcpy(&key, tempBuffer + offset, sizeof(int));
		memcpy(&rid, tempBuffer + offset + sizeof(int), sizeof(RecordId));

		rc = 0;
	}

	return rc;
}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
	// Assume no next sibling node first
	PageId pid = -1;
	char * tempBuffer = buffer;

	// Copy the pid from the last four bytes of the buffer that holds the next sibling node if any
	memcpy(&pid, tempBuffer + PageFile::PAGE_SIZE - sizeof(PageId), sizeof(PageId));

	return pid;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	RC rc;

	// pid is invalid and out of range
	if (pid < 0)
		rc = RC_INVALID_PID;

	// Copy pid of the next sibling into last 4 bytes of the leaf node
	char * tempBuffer = buffer;
	memcpy(tempBuffer + PageFile::PAGE_SIZE - sizeof(PageId), &pid, sizeof(PageId));

	rc = 0;
	return rc;
}


/**
* Constructor: initialize empty non-leaf node
*/
BTNonLeafNode::BTNonLeafNode()
{
	m_numKeys = 0;
	memset(buffer, 0, sizeof(buffer));
}


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	RC rc;

	// Use PageFile read to get the node's contents from the page pid in the Pagefile pf to main memory
	rc = pf.read(pid, buffer);

	return rc;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC rc; 

	// Use PageFile write to put the node's contents to the page pid in the Pagefile pf to disk
	rc = pf.write(pid, buffer);

	return rc;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ 
	int keyCount = 0;
	int kvPairSize = sizeof(int) + sizeof(PageId);
	char * tempBuffer = buffer + 4;

	// Each key-PageId pair is 8 bytes
	// Max number of keys: 80; Max key-value pair byte usage: 80*8 = 640
	// First four bytes are for pid of non-leaf node
	// Largest possible index of pair to be inserted: 644-8 = 636
	// Loop through the buffer to check the number of keys
	for (int n = 4; n <= 636; n += kvPairSize)
	{
		// Check if the nth key is valid and not null (== 0), meaning it was not empty
		int nKey;
		memcpy(&nKey, tempBuffer, sizeof(int));
		if (nKey == 0)
			break;

		// Increment key count for every valid key and iterate to next key-value pair
		keyCount++;
		tempBuffer += kvPairSize;
	}

	return keyCount;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	RC rc;
	int kvPairSize = sizeof(int) + sizeof(PageId);
	int maxKVPairs = 80;

	// If adding one more key-pid pair overflows the node's max capacity (80 pairs)
	// return that the node is full
	if (getKeyCount() + 1 > maxKVPairs)
	{
		rc = RC_NODE_FULL;
	}
	else
	{
		// Check where to insert the key-pid pair into the node
		char * tempBuffer = buffer + 4;

		int idx;
		for (idx = 4; idx < 636; idx += kvPairSize)
		{
			int nKey;
			memcpy(&nKey, tempBuffer, sizeof(int));

			// Check if the key is less than one of the node's keys or if the node's key is empty
			// the index will be where to insert the key-value pair after shifting everything over 
			if (nKey == 0 || key <= nKey)
				break;

			tempBuffer += kvPairSize;
		}

		// After finding the index location (idx) to insert the key-pid pair
		// let's use another buffer to shift everything over
		char * insertBuffer = (char *)malloc(PageFile::PAGE_SIZE);
		memset(insertBuffer, 0, sizeof(insertBuffer));

		// Make sure the insertBuffer has all the values up until the index insert location
		memcpy(insertBuffer, buffer, idx);

		// Insert key-value pair into proper index location
		memcpy(insertBuffer + idx, &key, sizeof(int));
		memcpy(insertBuffer + idx + sizeof(int), &pid, sizeof(PageId));

		// Copy the rest of the values after the index insert location to shift everything over
		memcpy(insertBuffer + idx + kvPairSize, buffer + idx, getKeyCount() * kvPairSize - idx + 4);

		// Update the old buffer to the insertBuffer with properly inserted key-pid pair
		// Deallocate the buffer that aided the insertion
		memcpy(buffer, insertBuffer, PageFile::PAGE_SIZE);
		free(insertBuffer);

		m_numKeys++;
		rc = 0;
	}

	return rc;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	RC rc;
	int kvPairSize = sizeof(int) + sizeof(PageId);
	int maxKVPairs = 80;

	// Node must be full before performing non-leaf overflow split algorithm
	if (!(getKeyCount() + 1 > maxKVPairs))
	{
		rc = RC_INVALID_FILE_FORMAT;
	} // The new sibling to split key-pid pairs with must be empty first before proceeding
	else if (sibling.getKeyCount() != 0)
	{
		rc = RC_INVALID_ATTRIBUTE;
	}
	else
	{
		// Clear sibling before adding the other half of the keys into it
		memset(sibling.buffer, 0, sizeof(sibling.buffer));

		// Find the number of half keys and the position to split the the node in two
		int halfKeys = ((int)((getKeyCount() + 1) / 2));
		int halfPos = halfKeys * kvPairSize + 4;

		// Retrieve the last key of the first half and the first key of the second half
		// use these two keys and the given key to insert to determine the middle key to push up
		int lastFHKey = 0;
		int firstSHKey = 0;

		memcpy(&lastFHKey, buffer + halfPos - 8, sizeof(int));
		memcpy(&firstSHKey, buffer + halfPos, sizeof(int));

		// First second half key is the middle key
		if (key > firstSHKey)
		{
			// Shift everything to the right by copying all the keys to the right of the 
			// halfPos except for the first key-value pair
			// we will insert the sibling pid into the first four bytes and the second half key-pid pairs after
			// Since we have 80 keys and 4 byte offset for initial pid, that fills up 644 Bytes maximum.
			// We must do 1024 - 380(filler) - halfPos (up to 320) for number of bytes to copy - 8 for first key-pid pair in second half 
			memcpy(sibling.buffer + 4, buffer + halfPos + 8, PageFile::PAGE_SIZE - 380 - halfPos - 8);
			sibling.m_numKeys = getKeyCount() - halfKeys - 1;

			// Place value of first second half key into midKey
			memcpy(&midKey, buffer + halfPos, sizeof(int));

			// Make sure you copy sibling pid into first four bytes
			memcpy(sibling.buffer, buffer + halfPos + 4, sizeof(PageId));
			
			// Zero out the second half of the original buffer
			memset(buffer + halfPos, 0, PageFile::PAGE_SIZE - halfPos);
			m_numKeys = halfKeys;

			// Insert the key-pid pair somewhere into the second half/sibling's node
			sibling.insert(key, pid);

		} // Last key of the first half is the middle key 
		else if (key < lastFHKey) 
		{
			// Move everything on the right of the halfPos into the sibling buffer
			// First four bytes are for first pid
			// Number of bytes to copy: 1024 - 380(filler) - halfPos(up to 320)
			memcpy(sibling.buffer + 4, buffer + halfPos, PageFile::PAGE_SIZE - 380 - halfPos);
			sibling.m_numKeys = getKeyCount() - halfKeys;

			// Place value of the last key of the first half into midKey
			memcpy(&midKey, buffer + halfPos - 8, sizeof(int));
			
			// Make sure you copy sibling pid into first four bytes
			memcpy(sibling.buffer, buffer + halfPos - 4, sizeof(PageId));

			// Zero out the second half of the original buffer
			memset(buffer + halfPos, 0, PageFile::PAGE_SIZE - halfPos);
			m_numKeys = halfKeys - 1;

			// Insert the key-pid pair somewhere into the first half/original node
			insert(key, pid);

		} // Key to be inserted is the middle key
		else
		{
			// Move everything on the right side of the halfPos into the sibling buffer
			// First four bytes are for first pid
			// Number of bytes to copy: 1024 - 380(filler) - halfPos(up to 320)
			memcpy(sibling.buffer + 4, buffer + halfPos, PageFile::PAGE_SIZE - 380 - halfPos);
			sibling.m_numKeys = getKeyCount() - halfKeys;

			// Place value of key to insert into midKey
			midKey = key;

			// Make sure you copy sibling pid into first four bytes
			memcpy(sibling.buffer, &pid, sizeof(PageId));

			// Zero out the second half of the original buffer
			memset(buffer + halfPos, 0, PageFile::PAGE_SIZE - halfPos);
			m_numKeys = halfKeys;

		}

		rc = 0;
	}

	return rc;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	RC rc;
	int kvPairSize = sizeof(int) + sizeof(PageId);
	char * tempBuffer = buffer + 4;

	int n;
	for (n = 4; n < getKeyCount() * kvPairSize + 4; n += kvPairSize)
	{
		int nKey;
		memcpy(&nKey, tempBuffer, sizeof(int));
		if (n == 8 && nKey > searchKey)
		{
			// searchKey is less than the first key so return first pid in first four bytes
			memcpy(&pid, buffer, sizeof(PageId));
			
			rc = 0;
			return rc;
		}
		else if (nKey > searchKey)
		{
			// return the pid of the key just before the one that is greater than searchKey
			memcpy(&pid, tempBuffer - 4, sizeof(PageId));
			
			rc = 0;
			return rc;
		}

		tempBuffer += kvPairSize;
	}
	
	// searchKey is greater than all the keys in the buffer so return last possible pid
	memcpy(&pid, tempBuffer - 4, sizeof(PageId));

	rc = 0;
	return rc;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
	RC rc;
	
	// Make sure buffer is clean
	memset(buffer, 0, sizeof(buffer));

	// Initialize first pid to insert in first four bytes
	memcpy(buffer, &pid1, sizeof(PageId));

	// Insert first key-pid pair into buffer
	rc = insert(key, pid2);

	return rc;
}
