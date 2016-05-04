/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

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
	std::string indexName = idxStr.str(); // index name is the name of the index file
	outIndexName = indexName;

	if(File::exists(indexName)){
		// If file exist, open the file
		this->file = new BlobFile(indexName, false);
		Page * firstPage;
		
		// Read the metadata
		bufMgr->readPage(file, 1, firstPage);
		IndexMetaInfo* metadata = (IndexMetaInfo*)firstPage;

		// Check if the values in the metadata match with the given constructor parameters
		std::string cmpRelationName(metadata->relationName);
		if(!relationName.compare(cmpRelationName) || !(metadata->attrType != attrType) || !(metadata->attrByteOffset != attrByteOffset)){
			throw new BadIndexInfoException("Metadata does not match constructor parameters");
			// TODO: better implementation, more info
		}

		this->bufMgr = bufMgrIn;
		this->attrByteOffset = attrByteOffset;
		this->attributeType = attrType;
		this->headerPageNum = 1;
		this->rootPageNum = metadata->rootPageNo;

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
	}
	else{
		// File does not exist, create a new file
		this->file = new BlobFile(indexName, true);

		// Assign values to the private variables
		this->bufMgr = bufMgrIn;
		this->attrByteOffset = attrByteOffset;
		this->attributeType = attrType;

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

		// Allocate page for metadata, first page
		Page* metadataPage;
		bufMgr->allocPage(file, headerPageNum, metadataPage);

		// Allocate page for root, second page
		Page* rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// Initialize the root node, level is set to 0
		switch(attributeType){
			case INTEGER:{
				NonLeafNodeInt* rootInt = (NonLeafNodeInt*)rootPage;
				rootInt->level = 0;
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

		// Create metadata for the index file
		IndexMetaInfo* metadata = (IndexMetaInfo*)metadataPage;
		strcpy(metadata->relationName, relationName.c_str());	
		metadata->attrByteOffset = attrByteOffset;
		metadata->attrType = attrType;
		metadata->rootPageNo = rootPageNum;

		// Write metadata and root to file
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);
		// bufMgr->unPinPage(file, leafPageNum, true);

		// Insert every tuple into the b+tree
		{
			FileScan* fsInsert = new FileScan(relationName, bufMgr);

			RecordId currRid;
			int ha = 0;
			try{
				while(1){
					// if(ha >= leafOccupancy*3){
					// 	break;
					// }
					// Get the recordId and key
					fsInsert->scanNext(currRid);
					std::string recordStr = fsInsert->getRecord();
					const char *record = recordStr.c_str();

					// Insert the recordId and key based on it's type
					switch(attributeType){
						case INTEGER:{
							int key = *((int *)(record + attrByteOffset));
							// std::cout << key << std::endl;
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
		}
		// End of insert

		// Debugging purposes
		// Page* rootPagex;
		// bufMgr->readPage(file, rootPageNum, rootPagex);
		// NonLeafNodeInt* rootIntx = (NonLeafNodeInt*)rootPagex;

		// Page* leafPagex;
		// bufMgr->readPage(file, rootIntx->pageNoArray[12], leafPagex);
		// LeafNodeInt* leafIntx = (LeafNodeInt*)leafPagex;	

		// Page* leafPagexR;
		// bufMgr->readPage(file, rootIntx->pageNoArray[13], leafPagexR);
		// LeafNodeInt* leafIntxR = (LeafNodeInt*)leafPagexR;

		// std::cout << std::endl << "numkeys: " << rootIntx->numKeys << std::endl << "[";
		// for(int i = 0; i < leafOccupancy-1; i++){
		// 	std::cout << rootIntx->keyArray[i] << ",";
		// }
		// std::cout << rootIntx->keyArray[leafOccupancy-1] << "]" << std::endl;

		// std::cout << std::endl << "numkeys: " << rootIntx->numKeys << std::endl << "[";
		// for(int i = 0; i < leafOccupancy; i++){
		// 	std::cout << rootIntx->pageNoArray[i] << ",";
		// }
		// std::cout << rootIntx->pageNoArray[leafOccupancy] << "]" << std::endl;

		// std::cout << std::endl << "numkeys: " << leafIntx->numKeys << std::endl << "[";
		// for(int i = 0; i < leafOccupancy-1; i++){
		// 	std::cout << leafIntx->keyArray[i] << ",";
		// }
		// std::cout << leafIntx->keyArray[leafOccupancy-1] << "]" << std::endl;

		// std::cout << std::endl << "[";
		// for(int i = 0; i < leafOccupancy-1; i++){
		// 	std::cout << leafIntx->ridArray[i].slot_number << ",";
		// }
		// std::cout << leafIntx->ridArray[leafOccupancy-1].slot_number << "]" << std::endl;

		// std::cout << std::endl << "numkeys: " << leafIntxR->numKeys << std::endl << "[";
		// for(int i = 0; i < leafOccupancy-1; i++){
		// 	std::cout << leafIntxR->keyArray[i] << ",";
		// }
		// std::cout << leafIntxR->keyArray[leafOccupancy-1] << "]" << std::endl;

		// std::cout << std::endl << "[";
		// for(int i = 0; i < leafOccupancy-1; i++){
		// 	std::cout << leafIntxR->ridArray[i].slot_number << ",";
		// }
		// std::cout << leafIntxR->ridArray[leafOccupancy-1].slot_number << "]" << std::endl;

		// bufMgr->unPinPage(file, rootIntx->pageNoArray[1], false);
		// bufMgr->unPinPage(file, rootIntx->pageNoArray[0], false);
		// bufMgr->unPinPage(file, rootPageNum, false);
		// For debug usage for the moment
		// bufMgr->readPage(file, rootPageNum, rootPage);
		// switch(attributeType){
		// 	case INTEGER:{
		// 		NonLeafNodeInt* rootInt = (NonLeafNodeInt*)rootPage;
		// 		break;
		// 	}
		// 	case DOUBLE:{
		// 		NonLeafNodeDouble* rootDouble = (NonLeafNodeDouble*)rootPage;
		// 		break;
		// 	}
		// 	case STRING:{
		// 		NonLeafNodeString* rootString = (NonLeafNodeString*)rootPage;
		// 		break;
		// 	}
		// }
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
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
			int keyInt = *(int*)key;
			NonLeafNodeInt* rootInt = (NonLeafNodeInt*)rootPage;
			if(rootInt->level == 0){
				// If there is only one leaf node
				if(rootInt->pageNoArray[0] == 0){
					// If the leaf node does not exist yet(first insert), create the first leaf node
					Page* leafPage;
					bufMgr->allocPage(file, rootInt->pageNoArray[0], leafPage);	
					LeafNodeInt* leafInt = (LeafNodeInt*)leafPage;

					// Insert entry
					leafInt->keyArray[0] = *(int*)key;
					leafInt->ridArray[0] = rid;
					leafInt->numKeys = 1;
					leafInt->rightSibPageNo = 0;

					// Write page
					bufMgr->unPinPage(file, rootInt->pageNoArray[0], true);
					bufMgr->unPinPage(file, rootPageNum, true);
				}
				else{
					// If the first leaf node exist 
					Page* leafPage;
					bufMgr->readPage(file, rootInt->pageNoArray[0], leafPage);

					// Get the leaf node
					LeafNodeInt* leafInt = (LeafNodeInt*)leafPage;
					// std::cout << leafInt->numKeys << std::endl;

					if(leafInt->numKeys >= leafOccupancy){
						// If the leaf node is full, split the root
						// Sort the key array of the leaf node, the extra key or the largest key is stored in an int variable
						int i = 0;

						// Remove the last item
						int lastKey = leafInt->keyArray[leafOccupancy-1];
						RecordId lastRecord = leafInt->ridArray[leafOccupancy-1];
						leafInt->numKeys--;

						// Find the spot to insert the key
						for(i = 0; i < leafInt->numKeys; i++){
							if(!(keyInt >= leafInt->keyArray[i])){
								break;
							}
						}

						// Shift the keys that are larger than the current key to the right
						for(int j = leafInt->numKeys; j > i; j--){
							leafInt->keyArray[j] = leafInt->keyArray[j-1];
							leafInt->ridArray[j] = leafInt->ridArray[j-1];
						}

						// Insert the current key and record id
						leafInt->keyArray[i] = keyInt;
						leafInt->ridArray[i] = rid;
						leafInt->numKeys++;

						// Compare the extra key
						if(lastKey < leafInt->keyArray[leafOccupancy-1]){
							int temp = leafInt->keyArray[leafOccupancy-1];
							leafInt->keyArray[leafOccupancy-1] = lastKey;
							lastKey = temp;

							RecordId tempR = leafInt->ridArray[leafOccupancy-1];
							leafInt->ridArray[leafOccupancy-1] = lastRecord;
							lastRecord = tempR;
						}
						// The array is now sorted

						// Split the node
						Page* leafPageNew;
						bufMgr->allocPage(file, rootInt->pageNoArray[1], leafPageNew);	
						LeafNodeInt* leafIntNew = (LeafNodeInt*)leafPageNew;

						// The position (k to end) in the array that is moved to another node
						int k = (leafOccupancy+1)/2;

						// Set the root node
						rootInt->level++;
						rootInt->keyArray[0] = leafInt->keyArray[k];
						rootInt->numKeys++;

						// Set the left leaf node
						leafInt->numKeys = k;
						leafInt->rightSibPageNo = rootInt->pageNoArray[1];

						// Set the right leaf node
						leafIntNew->rightSibPageNo = 0;
						leafIntNew->numKeys = 0;

						int j = 0;
						for(j = k; j < leafOccupancy; j++){
							leafIntNew->keyArray[j-k] = leafInt->keyArray[j];
							leafIntNew->ridArray[j-k] = leafInt->ridArray[j];
							leafIntNew->numKeys++;
						}

						leafIntNew->keyArray[j-k] = lastKey;
						leafIntNew->ridArray[j-k] = lastRecord;
						leafIntNew->numKeys++;

						// Set the empty slots to 0, for debugging purposes
						for(int l = k; l < leafOccupancy; l++){
							leafInt->keyArray[l] = 0;
						}

						// Write the changes
						bufMgr->unPinPage(file, rootInt->pageNoArray[0], true);
						bufMgr->unPinPage(file, rootInt->pageNoArray[1], true);
						bufMgr->unPinPage(file, rootPageNum, true);
					}
					else{
						// Insert the key
						int i = 0;

						// Find the spot to insert the key
						for(i = 0; i < leafInt->numKeys; i++){
							if(!(keyInt >= leafInt->keyArray[i])){
								break;
							}
						}

						// Shift the keys that are larger than the current key to the right
						for(int j = leafInt->numKeys; j > i; j--){
							leafInt->keyArray[j] = leafInt->keyArray[j-1];
							leafInt->ridArray[j] = leafInt->ridArray[j-1];
						}

						// Insert the current key and record id
						leafInt->keyArray[i] = keyInt;
						leafInt->ridArray[i] = rid;
						leafInt->numKeys++;

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
					// Sort the key array of the leaf node, the extra key or the largest key is stored in an int variable
					int i = 0;

					// Remove the last item
					lastKey = currLeafInt->keyArray[leafOccupancy-1];
					lastRecord = currLeafInt->ridArray[leafOccupancy-1];
					currLeafInt->numKeys--;

					// Find the spot to insert the key
					for(i = 0; i < currLeafInt->numKeys; i++){
						if(!(keyInt >= currLeafInt->keyArray[i])){
							break;
						}
					}

					// Shift the keys that are larger than the current key to the right
					for(int j = currLeafInt->numKeys; j > i; j--){
						currLeafInt->keyArray[j] = currLeafInt->keyArray[j-1];
						currLeafInt->ridArray[j] = currLeafInt->ridArray[j-1];
					}

					// Insert the current key and record id
					currLeafInt->keyArray[i] = keyInt;
					currLeafInt->ridArray[i] = rid;
					currLeafInt->numKeys++;

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
					// Find the spot to insert the key
					int i = 0;
					for(i = 0; i < currLeafInt->numKeys; i++){
						if(!(keyInt >= currLeafInt->keyArray[i])){
							break;
						}
					}

					// Shift the keys that are larger than the current key to the right
					for(int j = currLeafInt->numKeys; j > i; j--){
						currLeafInt->keyArray[j] = currLeafInt->keyArray[j-1];
						currLeafInt->ridArray[j] = currLeafInt->ridArray[j-1];
					}

					// Insert the current key and record id
					currLeafInt->keyArray[i] = keyInt;
					currLeafInt->ridArray[i] = rid;
					currLeafInt->numKeys++;

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
	if(lowOpParm != GT && lowOpParm != GTE){
		throw new BadOpcodesException();
	}

	lowOp = lowOpParm;

	if(highOpParm != LT && highOpParm != LTE){
		throw new BadOpcodesException();
	}

	highOp = highOpParm;

	switch(attributeType){
		case INTEGER:{
			lowValInt = *(int*)lowValParm;
			highValInt = *(int*)highValParm;

			if(lowValInt > highValInt){
				throw new BadScanrangeException();
			}

			// Scan for the low Value
			Page* rootPage;
			bufMgr->readPage(file, rootPageNum, rootPage);
			NonLeafNodeInt* rootInt = (NonLeafNodeInt*)rootPage;
			Page* currPage;
			int currLevel = rootInt->level;
			PageId currPageId = rootPageNum;

			bufMgr->unPinPage(file, rootPageNum, false);

			while(currLevel > 0){
				bufMgr->readPage(file, currPageId, currPage);
				NonLeafNodeInt* currInt = (NonLeafNodeInt*)currPage;

				int i = 0;

				if(lowValInt <)

				for(i = 0; i < currInt->numKeys; i++){
					if(!(lowValInt >= currInt->keyArray[i])){
						break;
					}
				}

				currLevel--;
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

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
