/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <queue>
#include <stack>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	// Generate index file name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	indexFileName = idxStr.str(); // index name is the name of the index file
	outIndexName = indexFileName;

	// Set the variables
	scanExecuting = false;
	this->bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	this->headerPageNum = 1;

	// Set attribute type
	switch(attributeType){
		case INTEGER:
			this->leafOccupancy = INTARRAYLEAFSIZE;
			this->nodeOccupancy = INTARRAYNONLEAFSIZE;
			break;
		case DOUBLE:
			this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
			this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
			break;
		case STRING:
			this->leafOccupancy = STRINGARRAYLEAFSIZE;
			this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
			break;
	}

	// Check if file exist
	if(File::exists(indexFileName)){
		// If file exist, open the file
		this->file = new BlobFile(indexFileName, false);
		Page * firstPage;
		
		// Read the metadata
		bufMgr->readPage(file, headerPageNum, firstPage);
		IndexMetaInfo* metadata = (IndexMetaInfo*)firstPage;

		// Check if the values in the metadata match with the given constructor parameters
		std::string cmpRelationName(metadata->relationName);
		if(!relationName.compare(cmpRelationName) || !(metadata->attrType != attrType) || !(metadata->attrByteOffset != attrByteOffset)){
			throw BadIndexInfoException("Metadata does not match constructor parameters");
			// TODO: better implementation, more info
		}

		this->rootPageNum = metadata->rootPageNo;
	}
	else{
		// File does not exist, create a new file
		this->file = new BlobFile(indexFileName, true);

		// Allocate page for metadata, first page
		Page* metadataPage;
		bufMgr->allocPage(file, headerPageNum, metadataPage);

		// Allocate page for root, second page
		Page* rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// Initialize the root node, level is set to 0
		switch(attributeType){
			case INTEGER:{
				NonLeafNodeInt* root = (NonLeafNodeInt*)rootPage;
				root->level = 0;
				break;
			}
			case DOUBLE:{
				NonLeafNodeDouble* root = (NonLeafNodeDouble*)rootPage;
				root->level = 0;
				break;
			}
			case STRING:{
				NonLeafNodeString* root = (NonLeafNodeString*)rootPage;
				root->level = 0;
				break;
			}
		}	

		// Create metadata for the index file
		IndexMetaInfo* metadata = (IndexMetaInfo*)metadataPage;
		strcpy(metadata->relationName, relationName.c_str());	
		metadata->attrByteOffset = attrByteOffset;
		metadata->attrType = attrType;
		metadata->rootPageNo = rootPageNum;

		// Write metadata and root to file
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);

		// Insert every tuple into the b+tree
		{
			FileScan* fsInsert = new FileScan(relationName, bufMgr);

			RecordId currRid;
			int ha = 0;
			try{
				while(1){
					// Get the recordId and key
					fsInsert->scanNext(currRid);
					std::string recordStr = fsInsert->getRecord();
					const char *record = recordStr.c_str();

					// Insert the recordId and key based on it's type
					switch(attributeType){
						case INTEGER:{
							int key = *((int *)(record + attrByteOffset));
							this->insertEntry((void*)&key, currRid);
							break;
						}
						case DOUBLE:{
							break;
						}
						case STRING:{
							break;
						}
					}	
					ha++;
				}
			}
			catch (EndOfFileException e){
				std::cout << "All records are inserted" << std::endl;
			}
			delete fsInsert;
		}
		// End of insert
		// printTree();
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// If it is still scanning, end the scan
	if(scanExecuting){
		endScan();
	}
	
	// Flush all dirty pages and close file
	bufMgr->flushFile(file);
	delete file;
	File::remove(indexFileName);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// Get the root node
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);	

	switch(attributeType){
		case INTEGER:{
			// Initialize the variables
			int keyInt = *(int*)key;
			RIDKeyPair<int>* keyPair = new RIDKeyPair<int>(rid, keyInt);
			NonLeafNodeInt* rootInt = (NonLeafNodeInt*)rootPage;
			Page* leafPage;

			if(rootInt->level == 0){
				// If there is only one leaf node, then check if the leaf node exist
				if(rootInt->pageNoArray[0] == 0){
					// If the leaf node does not exist yet(first insert), create the first leaf node
					bufMgr->allocPage(file, rootInt->pageNoArray[0], leafPage);	
					LeafNodeInt* leafInt = (LeafNodeInt*)leafPage;

					// Insert entry
					leafInt->keyArray[0] = keyPair->key;
					leafInt->ridArray[0] = keyPair->rid;
					leafInt->numKeys = 1;
					leafInt->rightSibPageNo = 0;

					// Write page
					bufMgr->unPinPage(file, rootInt->pageNoArray[0], true);
					bufMgr->unPinPage(file, rootPageNum, true);
				}
				else{
					// If the first leaf node exist 
					bufMgr->readPage(file, rootInt->pageNoArray[0], leafPage);

					// Get the leaf node
					LeafNodeInt* leafNode = (LeafNodeInt*)leafPage;

					// If the leaf node is full, split the root
					if(leafNode->numKeys >= leafOccupancy){
						// Remove the last item to be inserted later
						RIDKeyPair<int>* endKeyPair = new RIDKeyPair<int>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
						leafNode->numKeys--;

						// Insert the keyPair
						insertIntLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

						// Compare the extra key Pair
						RIDKeyPair<int>* currKeyPair = new RIDKeyPair<int>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
						swapIntKeyPair(endKeyPair, currKeyPair);
						leafNode->keyArray[leafOccupancy-1] = currKeyPair->key;
						leafNode->ridArray[leafOccupancy-1] = currKeyPair->rid;
						// The array is now sorted

						// Split the node
						Page* newLeafPage;
						bufMgr->allocPage(file, rootInt->pageNoArray[1], newLeafPage);	
						LeafNodeInt* newLeafNode = (LeafNodeInt*)newLeafPage;

						// The position (k to end) in the array that is moved to another node
						int k = (leafOccupancy+1)/2;

						// Set the root node
						rootInt->level++;
						rootInt->keyArray[0] = leafNode->keyArray[k];
						rootInt->numKeys++;

						// Set the left leaf node
						leafNode->numKeys = k;
						leafNode->rightSibPageNo = rootInt->pageNoArray[1];

						// Set the right leaf node
						newLeafNode->rightSibPageNo = 0;
						newLeafNode->numKeys = 0;

						// Copy half of the array to the new array
						int j = 0;
						for(j = k; j < leafOccupancy; j++){
							newLeafNode->keyArray[j-k] = leafNode->keyArray[j];
							newLeafNode->ridArray[j-k] = leafNode->ridArray[j];
							newLeafNode->numKeys++;
						}

						newLeafNode->keyArray[j-k] = endKeyPair->key;
						newLeafNode->ridArray[j-k] = endKeyPair->rid;
						newLeafNode->numKeys++;

						// Write the changes
						bufMgr->unPinPage(file, rootInt->pageNoArray[0], true);
						bufMgr->unPinPage(file, rootInt->pageNoArray[1], true);
						bufMgr->unPinPage(file, rootPageNum, true);
					}
					else{
						// Insert the key
						insertIntLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

						// Write the changes
						bufMgr->unPinPage(file, rootInt->pageNoArray[0], true);
						bufMgr->unPinPage(file, rootPageNum, false);
					}
				}
			}
			else{
				// Search the tree
				int currLevel = rootInt->level;

				// Traverse the non-leaf level
				PageId currPageId = rootPageNum;
				PageId rightPageId; // For setting the right sibling of the leaf node
				Page* currPage;
				PageId prevPageId = currPageId;
				
				// Stack to store pageId for reverse traversal
				std::stack<PageId> pageStack;

				while(currLevel > 0){
					bufMgr->readPage(file, currPageId, currPage);
					pageStack.push(currPageId);
					NonLeafNodeInt* currInt = (NonLeafNodeInt*)currPage;

					int i = 0;
					for(i = 0; i < currInt->numKeys; i++){
						if(!(keyInt >= currInt->keyArray[i])){
							break;
						}
					}

					prevPageId = currPageId;
					currPageId = currInt->pageNoArray[i];

					// For setting leafNode right sibling later
					if(!(i >= nodeOccupancy)){
						rightPageId = currInt->pageNoArray[i+1];
					}
					else{
						rightPageId = 0;
					}

					bufMgr->unPinPage(file, prevPageId, false);
					currLevel --;
				}

				// Reach the leaf level
				bufMgr->readPage(file, currPageId, currPage);
				LeafNodeInt* currLeafInt = (LeafNodeInt*)currPage;

				// Check if the leaf node is full
				if(currLeafInt->numKeys >= leafOccupancy){
					int lastKey;
					RecordId lastRecord;
					PageId leafIntNewId;

					// If the leaf node is full, split the node

					// Remove the last item
					lastKey = currLeafInt->keyArray[leafOccupancy-1];
					lastRecord = currLeafInt->ridArray[leafOccupancy-1];
					currLeafInt->numKeys--;

					insertIntLeafArray(currLeafInt->keyArray, currLeafInt->ridArray, currLeafInt->numKeys, keyPair);

					// Compare the extra key
					if(lastKey < currLeafInt->keyArray[leafOccupancy-1]){
						int temp = currLeafInt->keyArray[leafOccupancy-1];
						currLeafInt->keyArray[leafOccupancy-1] = lastKey;
						lastKey = temp;

						RecordId tempR = currLeafInt->ridArray[leafOccupancy-1];
						currLeafInt->ridArray[leafOccupancy-1] = lastRecord;
						lastRecord = tempR;
					}
					// The array is now sorted

					// Split the node
					Page* leafPageNew;
					bufMgr->allocPage(file, leafIntNewId, leafPageNew);	
					LeafNodeInt* leafIntNew = (LeafNodeInt*)leafPageNew;

					// The position (k to end) in the array that is moved to another node
					int k = (leafOccupancy+1)/2;
					int keyForParent = currLeafInt->keyArray[k];
					int pageIdForParent = leafIntNewId;

					// // Set the left leaf node
					currLeafInt->numKeys = k;
					currLeafInt->rightSibPageNo = leafIntNewId;

					// Set the right leaf node
					leafIntNew->rightSibPageNo = rightPageId;
					leafIntNew->numKeys = 0;

					int j = 0;
					for(j = k; j < leafOccupancy; j++){
						leafIntNew->keyArray[j-k] = currLeafInt->keyArray[j];
						leafIntNew->ridArray[j-k] = currLeafInt->ridArray[j];
						leafIntNew->numKeys++;
					}

					leafIntNew->keyArray[j-k] = lastKey;
					leafIntNew->ridArray[j-k] = lastRecord;
					leafIntNew->numKeys++;

					// Set the empty slots to 0, for debugging purposes
					for(int l = k; l < leafOccupancy; l++){
						currLeafInt->keyArray[l] = 0;
					}

					// // Write the changes
					bufMgr->unPinPage(file, currPageId, true);
					bufMgr->unPinPage(file, leafIntNewId, true);
					bufMgr->unPinPage(file, rootPageNum, true);

					// Reverse traversal up the tree
					while(pageStack.size() > 0){
						// Get the parent node pageId
						PageId parentPageId = pageStack.top();
						pageStack.pop();

						Page* parentPage;
						bufMgr->readPage(file, parentPageId, parentPage);

						NonLeafNodeInt* parentInt = (NonLeafNodeInt*)parentPage;

						// Check if the parent node is full
						if(parentInt->numKeys >= nodeOccupancy){
							// If the current node is full, split the node
							// Sort the key array of the current node, the extra key or the largest key is stored in an int variable
							PageId lastPageId;
								
							// Remove the last item
							lastKey = parentInt->keyArray[nodeOccupancy-1];
							lastPageId = parentInt->pageNoArray[nodeOccupancy];
							parentInt->numKeys--;

							// Find the spot to insert the key
							int l = 0;
							for(l = 0; l < parentInt->numKeys; l++){
								if(!(keyForParent >= parentInt->keyArray[l])){
									break;
								}
							}

							// Shift the keys that are larger than the current key to the right
							for(j = parentInt->numKeys; j > l; j--){
								parentInt->keyArray[j] = parentInt->keyArray[j-1];
								parentInt->pageNoArray[j+1] = parentInt->pageNoArray[j];
							}

							// Insert the current key and pageNo
							parentInt->keyArray[l] = keyForParent;
							parentInt->pageNoArray[l+1] = pageIdForParent;
							parentInt->numKeys++;

							// Compare the extra key
							if(lastKey < parentInt->keyArray[nodeOccupancy-1]){
								int temp = parentInt->keyArray[nodeOccupancy-1];
								parentInt->keyArray[nodeOccupancy-1] = lastKey;
								lastKey = temp;

								PageId tempR = parentInt->pageNoArray[nodeOccupancy];
								parentInt->pageNoArray[nodeOccupancy] = lastPageId;
								lastPageId = tempR;
							}
							// The array is now sorted

							// Split the node
							Page* parentPageNew;
							PageId parentPageNewId;
							bufMgr->allocPage(file, parentPageNewId, parentPageNew);	
							NonLeafNodeInt* parentIntNew = (NonLeafNodeInt*)parentPageNew;

							// The position (k to end) in the array that is moved to another node
							k = (nodeOccupancy+1)/2;
							keyForParent = parentInt->keyArray[k];
							pageIdForParent = parentPageNewId;

							// Set the left node
							parentInt->numKeys = k;

							// Set the right node
							parentIntNew->numKeys = 0;

							for(j = k; j < nodeOccupancy-1; j++){
								parentIntNew->keyArray[j-k] = parentInt->keyArray[j+1];
								parentIntNew->pageNoArray[j-k] = parentInt->pageNoArray[j+1];
								parentIntNew->numKeys++;
							}

							parentIntNew->keyArray[j-k] = lastKey;
							parentIntNew->pageNoArray[j-k] = parentInt->pageNoArray[j+1];
							parentIntNew->pageNoArray[j-k] = lastPageId;
							parentIntNew->numKeys++;

							// Set the empty slots to 0, for debugging purposes
							for(int l = k; l < leafOccupancy; l++){
								parentInt->keyArray[l] = 0;
							}

							// If it is the root
							if(pageStack.size() == 0){
								Page* newRootPage;
								bufMgr->allocPage(file, rootPageNum, newRootPage);

								NonLeafNodeInt* newRootInt = (NonLeafNodeInt*)newRootPage;

								newRootInt->level = parentInt->level + 1;
								newRootInt->pageNoArray[0] = parentPageId;
								newRootInt->pageNoArray[1] = parentPageNewId;
								newRootInt->keyArray[0] = keyForParent;
								newRootInt->numKeys = 1;

								bufMgr->unPinPage(file, rootPageNum, true);

								// Read the metadata
								Page* metadataPage;
								bufMgr->readPage(file, headerPageNum, metadataPage);
								IndexMetaInfo* metadata = (IndexMetaInfo*)metadataPage;
								metadata->rootPageNo = rootPageNum;

								bufMgr->unPinPage(file, headerPageNum, true);
							}

							// Write the changes
							bufMgr->unPinPage(file, parentPageNewId, true);
							bufMgr->unPinPage(file, parentPageId, true);
						}
						else{
							// If the parent node is not full
							int l = 0;
							for(l = 0; l < parentInt->numKeys; l++){
								if(!(keyForParent >= parentInt->keyArray[l])){
									break;
								}
							}

							// Shift the keys that are larger than the current key to the right
							for(j = parentInt->numKeys; j > l; j--){
								parentInt->keyArray[j] = parentInt->keyArray[j-1];
								parentInt->pageNoArray[j+1] = parentInt->pageNoArray[j];
							}

							// Insert the current key and pageNo
							parentInt->keyArray[l] = keyForParent;
							parentInt->pageNoArray[l+1] = pageIdForParent;
							parentInt->numKeys++;

							bufMgr->unPinPage(file, parentPageId, true);
							return;
						}
					}
				}
				else{
					insertIntLeafArray(currLeafInt->keyArray, currLeafInt->ridArray, currLeafInt->numKeys, keyPair);

					// Write the changes
					bufMgr->unPinPage(file, currPageId, true);
					bufMgr->unPinPage(file, rootPageNum, false);
					return;
				}
			}
			break;
		}
		case DOUBLE:{
			NonLeafNodeDouble* rootDouble = (NonLeafNodeDouble*)rootPage;
			rootDouble->level = 0;
			break;
		}
		case STRING:{
			NonLeafNodeString* rootString = (NonLeafNodeString*)rootPage;
			rootString->level = 0;
			break;
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{	
	// If another scan is executing
	if(scanExecuting){
		endScan();
	}

	if(lowOpParm != GT && lowOpParm != GTE){
		throw BadOpcodesException();
	}

	lowOp = lowOpParm;

	if(highOpParm != LT && highOpParm != LTE){
		throw BadOpcodesException();
	}

	highOp = highOpParm;

	switch(attributeType){
		case INTEGER:{
			lowValInt = *(int*)lowValParm;
			highValInt = *(int*)highValParm;

			if(lowValInt > highValInt){
				throw BadScanrangeException();
			}

			// Scan for the low Value
			Page* rootPage;
			bufMgr->readPage(file, rootPageNum, rootPage);
			NonLeafNodeInt* rootInt = (NonLeafNodeInt*)rootPage;
			Page* currPage;
			int currLevel = rootInt->level;
			PageId currPageId = rootPageNum;
			PageId prevPageId = rootPageNum;

			bufMgr->unPinPage(file, rootPageNum, false);

			while(currLevel > 0){
				bufMgr->readPage(file, currPageId, currPage);
				NonLeafNodeInt* currInt = (NonLeafNodeInt*)currPage;

				// std::cout << std::endl << "numkeys: " << currInt->numKeys << std::endl << "[";
				// for(int i = 0; i < leafOccupancy-1; i++){
				// 	std::cout << currInt->keyArray[i] << ",";
				// }
				// std::cout << currInt->keyArray[leafOccupancy-1] << "]" << std::endl;

				int i = 0;

				for(i = 0; i < currInt->numKeys; i++){
					if(!(lowValInt >= currInt->keyArray[i])){
						break;
					}
				}

				prevPageId = currPageId;
				currPageId = currInt->pageNoArray[i];

				bufMgr->unPinPage(file, prevPageId, false);
				currLevel--;
			}

			// Reach the leaf level
			this->currentPageNum = currPageId;
			
			bufMgr->readPage(file, this->currentPageNum, this->currentPageData);
			LeafNodeInt* leafInt = (LeafNodeInt*)this->currentPageData;

			// std::cout << std::endl << "numkeys: " << leafInt->numKeys << std::endl << "[";
			// for(int i = 0; i < leafOccupancy-1; i++){
			// 	std::cout << leafInt->keyArray[i] << ",";
			// }
			// std::cout << leafInt->keyArray[leafOccupancy-1] << "]" << std::endl;

			this->scanExecuting = true;
			bool entryFound = false;

			// std::cout << "lowValInt: " << lowValInt << std::endl;
			// std::cout << "highValInt: " << highValInt << std::endl;

			// Search through the current leaf node
			int i = 0;
			for(i = 0; i < leafInt->numKeys; i++){
				if(lowOp == GT){
					// std::cout << "currSpot: " << leafInt->keyArray[i] << std::endl;
					if(leafInt->keyArray[i] > lowValInt){
						this->nextEntry = i;
						entryFound = true;
						break;
					}
				}
				else if(lowOp == GTE){
					if(leafInt->keyArray[i] >= lowValInt){
						this->nextEntry = i;
						entryFound = true;
						break;
					}
				}
			}

			if(!entryFound){
				std::cout << "No entry found" << std::endl;
			}
			// std::cout << "currSpot: " << leafInt->keyArray[nextEntry] << std::endl;

			// Search the right sibling if it is not found in this current leaf node
			if(!entryFound){
				PageId rightPageId = leafInt->rightSibPageNo;
				bufMgr->unPinPage(file, this->currentPageNum, false);
				this->currentPageNum = rightPageId;

				bufMgr->readPage(file, this->currentPageNum, this->currentPageData);
				leafInt = (LeafNodeInt*)this->currentPageData;

				for(i = 0; i < leafInt->numKeys; i++){
					if(lowOp == GT){
						if(leafInt->keyArray[i] > lowValInt){
							this->nextEntry = i;
							entryFound = true;
							break;
						}
					}
					else if(lowOp == GTE){
						if(leafInt->keyArray[i] >= lowValInt){
							this->nextEntry = i;
							entryFound = true;
							break;
						}
					}
				}
			}

			// If there is still no entry found
			if(!entryFound){
				endScan();
				throw NoSuchKeyFoundException();
			}

			// If the key found does not satisfy highOp
			if(highOp == LT){
				if(!(leafInt->keyArray[this->nextEntry] < highValInt)){
					endScan();
					throw NoSuchKeyFoundException();
				}
			}
			else if(highOp == LTE){
				if(!(leafInt->keyArray[this->nextEntry] <= highValInt)){
					endScan();
					throw NoSuchKeyFoundException();
				}
			}
			break;
		}
		case DOUBLE:{
			break;
		}
		case STRING:{
			break;
		}
	}	
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}

	// There is no right sibling
	if(this->nextEntry == -1){
		// endScan();
		throw IndexScanCompletedException();
	}

	switch(attributeType){
		case INTEGER:{
			LeafNodeInt* leafInt = (LeafNodeInt*)currentPageData;

			if(highOp == LT){
				if(!(leafInt->keyArray[this->nextEntry] < highValInt)){
					// endScan();
					throw IndexScanCompletedException();
				}
			}
			else if(highOp == LTE){
				if(!(leafInt->keyArray[this->nextEntry] <= highValInt)){
					// endScan();
					throw IndexScanCompletedException();
				}
			}

			// std::cout << leafInt->keyArray[this->nextEntry] << std::endl;
			outRid = leafInt->ridArray[this->nextEntry];

			if(this->nextEntry + 1 >= leafInt->numKeys){
				// std::cout << "Next" << std::endl;
				PageId rightPageId = leafInt->rightSibPageNo;

				// If there is no right sibling
				if(rightPageId == 0){
					this->nextEntry = -1;
				}
				else{
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageNum = rightPageId;
					bufMgr->readPage(file, currentPageNum, currentPageData);
					this->nextEntry = 0;
				}
			}
			else{
				this->nextEntry++;
			}			
			break;
		}
		case DOUBLE:{
			break;
		}
		case STRING:{
			break;
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{	
	// If there is no scan
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}

	scanExecuting = false;
	bufMgr->unPinPage(file, currentPageNum, false);

	currentPageNum = 0;
	currentPageData = NULL;
	nextEntry = -1;
}

// --------------------------------------------------------------------------------
/*
	Helper functions
*/
// --------------------------------------------------------------------------------

// Sort the arrays in the leaf
void BTreeIndex::insertIntLeafArray(void* array, void* ridArray, int& numItems, RIDKeyPair<int>* x){
	int* arr = (int*)array;
	RecordId* ridArr = (RecordId*)ridArray;

	int i = 0;
	for(i = 0; i < numItems; i++){
		if(arr[i] > x->key){
			break;
		}
	}

	for(int j = numItems; j > i; j--){
		arr[j] = arr[j-1];
		arr[j] = arr[j-1];
	}

	arr[i] = x->key;
	ridArr[i] = x->rid;

	numItems++;
}

// Swap x keyPair with ykeyPair if x < y
void BTreeIndex::swapIntKeyPair(RIDKeyPair<int>* x, RIDKeyPair<int>* y){
	if(x->key < y->key){
		int tempKey = x->key;
		RecordId tempRid = x->rid;

		x->set(y->rid, y->key);
		y->set(tempRid, tempKey);		
	}
}

// Print the whole tree
void BTreeIndex::printTree(void){
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);
	int nonLeafNum = 0;
	std::queue<PageId> pageQueue;

	switch(attributeType){
		case INTEGER:{
			NonLeafNodeInt* root = (NonLeafNodeInt*)rootPage;
			std::cout << "root: " << rootPageNum << std::endl;
			printNonLeafNode(rootPage);
			for(int i = 0; i < root->numKeys + 1; i++){
				pageQueue.push(root->pageNoArray[i]);
			}

			if(root->level > 1){
				nonLeafNum += root->numKeys + 1;
			}
			break;
		}
		case DOUBLE:{
			NonLeafNodeDouble* root = (NonLeafNodeDouble*)rootPage;
			std::cout << "root: " << rootPageNum << std::endl;
			printNonLeafNode(rootPage);
			for(int i = 0; i < root->numKeys + 1; i++){
				pageQueue.push(root->pageNoArray[i]);
			}

			if(root->level > 1){
				nonLeafNum += root->numKeys + 1;
			}
			break;
		}
		case STRING:{
			NonLeafNodeString* root = (NonLeafNodeString*)rootPage;
			std::cout << "root: " << rootPageNum << std::endl;
			printNonLeafNode(rootPage);
			for(int i = 0; i < root->numKeys + 1; i++){
				pageQueue.push(root->pageNoArray[i]);
			}

			if(root->level > 1){
				nonLeafNum += root->numKeys + 1;
			}
			break;
		}
	}
	bufMgr->unPinPage(file, rootPageNum, false);

	while(pageQueue.size() > 0){
		Page* currPage;
		PageId currPageId = pageQueue.front();
		pageQueue.pop();
		bufMgr->readPage(file, currPageId, currPage);
		
		switch(attributeType){
			case INTEGER:{
				if(nonLeafNum > 0){
					NonLeafNodeInt* node = (NonLeafNodeInt*)currPage;
					std::cout << "Non-leaf: " << currPageId << std::endl;
					printNonLeafNode(currPage);
					for(int i = 0; i < node->numKeys + 1; i++){
						pageQueue.push(node->pageNoArray[i]);
					}

					if(node->level > 1){
						nonLeafNum += node->numKeys + 1;
					}
				}
				else{
					std::cout << "Leaf: " << currPageId << std::endl;
					printLeafNode(currPage);
				}
				break;
			}
			case DOUBLE:{
				if(nonLeafNum > 0){
					NonLeafNodeDouble* node = (NonLeafNodeDouble*)currPage;
					std::cout << "Non-leaf: " << currPageId << std::endl;
					printNonLeafNode(currPage);
					for(int i = 0; i < node->numKeys + 1; i++){
						pageQueue.push(node->pageNoArray[i]);
					}

					if(node->level > 1){
						nonLeafNum += node->numKeys + 1;
					}
				}
				else{
					std::cout << "Leaf: " << currPageId << std::endl;
					printLeafNode(currPage);
				}
				break;
			}
			case STRING:{
				if(nonLeafNum > 0){
					NonLeafNodeString* node = (NonLeafNodeString*)currPage;
					std::cout << "Non-leaf: " << currPageId << std::endl;
					printNonLeafNode(currPage);
					for(int i = 0; i < node->numKeys + 1; i++){
						pageQueue.push(node->pageNoArray[i]);
					}

					if(node->level > 1){
						nonLeafNum += node->numKeys + 1;
					}
				}
				else{
					std::cout << "Leaf: " << currPageId << std::endl;
					printLeafNode(currPage);
				}
				break;
			}
		}

		bufMgr->unPinPage(file, currPageId, false);
		nonLeafNum--;
	}
}

// Print the non-leaf node, print both the keys and page no
void BTreeIndex::printNonLeafNode(Page* page){
	switch(attributeType){
		case INTEGER:{
			NonLeafNodeInt* node = (NonLeafNodeInt*)page;
			std::cout << "Key array: " << std::endl;
			printArray(node->keyArray, node->numKeys, 'i');
			std::cout << "PageNo array: " << std::endl;
			printArray(node->pageNoArray, node->numKeys+1, 'i');
			break;
		}
		case DOUBLE:{
			NonLeafNodeDouble* node = (NonLeafNodeDouble*)page;
			std::cout << "Key array: " << std::endl;
			printArray(node->keyArray, node->numKeys, 'd');
			std::cout << "PageNo array: " << std::endl;
			printArray(node->pageNoArray, node->numKeys+1, 'i');
			break;
		}
		case STRING:{
			NonLeafNodeString* node = (NonLeafNodeString*)page;
			std::cout << "Key array: " << std::endl;
			printArray(node->keyArray, node->numKeys, 's');
			std::cout << "PageNo array: " << std::endl;
			printArray(node->pageNoArray, node->numKeys+1, 'i');
			break;
		}
	}
}

// Print the leaf node, only print the keys
void BTreeIndex::printLeafNode(Page* page){
	switch(attributeType){
		case INTEGER:{
			LeafNodeInt* node = (LeafNodeInt*)page;
			printArray(node->keyArray, node->numKeys, 'i');
			break;
		}
		case DOUBLE:{
			LeafNodeDouble* node = (LeafNodeDouble*)page;
			printArray(node->keyArray, node->numKeys, 'd');
			break;
		}
		case STRING:{
			LeafNodeString* node = (LeafNodeString*)page;
			printArray(node->keyArray, node->numKeys, 's');
			break;
		}
	}
}

// Print out the given array based on types
void BTreeIndex::printArray(void* array, int numItems, char type){
	switch(type){
		case 'i':{
			int* arr = (int*)array;

			std::cout << "[";
			for(int i = 0; i < numItems-1; i++){
				std::cout << arr[i] << ",";
			}
			std::cout << arr[numItems-1] << "] " << numItems << " items" << std::endl;
			break;
		}
		case 'd':{
			double* arr = (double*)array;

			std::cout << "[";
			for(int i = 0; i < numItems-1; i++){
				std::cout << arr[i] << ",";
			}
			std::cout << arr[numItems-1] << "] " << numItems << " items" << std::endl;
			break;
		}
		case 'c':{
			char** arr = (char**)array;

			std::cout << "[";
			for(int i = 0; i < numItems-1; i++){
				std::cout << arr[i] << ",";
			}
			std::cout << arr[numItems-1] << "] " << numItems << " items" << std::endl;

			break;
		}
	}
}

}
