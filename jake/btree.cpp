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
      if(relationName.compare(cmpRelationName) != 0 || metadata->attrType != attrType || metadata->attrByteOffset != attrByteOffset){
        std::ostringstream error;
        error << std::endl << "RelationName: " << relationName << std::endl <<
          "MetadataRelationName: " << cmpRelationName << std::endl <<
          "AttributeType: " << attrType << std::endl <<
          "MetadataAttributeType: " << metadata->attrType <<  std::endl <<
          "AttributeByteOffset: " << attrByteOffset << std::endl <<
          "MetadataAttributeByteOffset: " << metadata->attrByteOffset << std::endl;
        throw BadIndexInfoException(error.str());
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
                      std::cout << "init" << std::endl;
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
        int items = 0;
        try{
          while(1){
            // Get the recordId and key
            fsInsert->scanNext(currRid);
            std::string recordStr = fsInsert->getRecord();
            const char *record = recordStr.c_str();
            items++;
            // Insert the recordId and key based on it's type
            switch(attributeType){
              case INTEGER:{
                             int key = *((int *)(record + attrByteOffset));
                             this->insertEntry((void*)&key, currRid);
                             break;
                           }
              case DOUBLE:{
                            double key = *((double *)(record + attrByteOffset));
                            this->insertEntry((void*)&key, currRid);
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
          std::cout << "All records are inserted" << items << std::endl;
        }
        delete fsInsert;
      }
      // End of insert
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
                           swapIntRIDKeyPair(endKeyPair, currKeyPair);
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
                       Page* currPage;
                       PageId currPageId = rootPageNum;
                       PageId rightPageId; // For setting the right sibling of the leaf node
                       PageId prevPageId = currPageId;

                       // Stack to store pageId for reverse traversal
                       std::stack<PageId> pageStack;

                       // Traverse down the tree to search for insert location
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
                       LeafNodeInt* leafNode = (LeafNodeInt*)currPage;

                       // Check if the leaf node is full
                       if(leafNode->numKeys >= leafOccupancy){
                         // Remove the last item
                         RIDKeyPair<int>* endKeyPair = new RIDKeyPair<int>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
                         leafNode->numKeys--;

                         insertIntLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

                         // Compare the extra key Pair
                         RIDKeyPair<int>* currKeyPair = new RIDKeyPair<int>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
                         swapIntRIDKeyPair(endKeyPair, currKeyPair);
                         leafNode->keyArray[leafOccupancy-1] = currKeyPair->key;
                         leafNode->ridArray[leafOccupancy-1] = currKeyPair->rid;
                         // The array is now sorted

                         // Split the node
                         PageId newLeafNodeId;
                         Page* newLeafPage;
                         bufMgr->allocPage(file, newLeafNodeId, newLeafPage);
                         LeafNodeInt* newLeafNode = (LeafNodeInt*)newLeafPage;

                         // The position (k to end) in the array that is moved to another node
                         int k = (leafOccupancy+1)/2;
                         PageKeyPair<int>* pagePair = new PageKeyPair<int>(newLeafNodeId, leafNode->keyArray[k]);

                         // Set the left leaf node
                         leafNode->numKeys = k;
                         leafNode->rightSibPageNo = newLeafNodeId;

                         // Set the right leaf node
                         newLeafNode->rightSibPageNo = rightPageId;
                         newLeafNode->numKeys = 0;

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
                         bufMgr->unPinPage(file, currPageId, true);
                         bufMgr->unPinPage(file, newLeafNodeId, true);
                         bufMgr->unPinPage(file, rootPageNum, true);

                         // Reverse traversal up the tree
                         while(pageStack.size() > 0){
                           // Get the parent node pageId
                           currPageId = pageStack.top();
                           pageStack.pop();

                           bufMgr->readPage(file, currPageId, currPage);

                           NonLeafNodeInt* currNode = (NonLeafNodeInt*)currPage;

                           // Check if the parent node is full
                           if(currNode->numKeys >= nodeOccupancy){
                             // If the current node is full, split the node

                             // Remove the last item
                             PageKeyPair<int>* endPagePair = new PageKeyPair<int>(currNode->pageNoArray[nodeOccupancy-1], currNode->keyArray[nodeOccupancy-1]);
                             currNode->numKeys--;

                             // Insert the key and pageNo
                             insertIntNonLeafArray(currNode->keyArray, currNode->pageNoArray, currNode->numKeys, pagePair);

                             PageKeyPair<int>* currPagePair = new PageKeyPair<int>(currNode->pageNoArray[nodeOccupancy], currNode->keyArray[nodeOccupancy-1]);
                             swapIntPageKeyPair(endPagePair, currPagePair);
                             currNode->keyArray[leafOccupancy-1] = currPagePair->key;
                             currNode->pageNoArray[leafOccupancy-1] = currPagePair->pageNo;
                             // The array is now sorted

                             // Split the node
                             Page* newPage;
                             PageId newPageId;
                             bufMgr->allocPage(file, newPageId, newPage);
                             NonLeafNodeInt* newCurrNode = (NonLeafNodeInt*)newPage;

                             // The position (k to end) in the array that is moved to another node
                             k = (nodeOccupancy+1)/2;
                             pagePair->set(newPageId, currNode->keyArray[k]);

                             // Set the left node
                             currNode->numKeys = k;

                             // Set the right node
                             newCurrNode->numKeys = 0;
                             newCurrNode->level = currNode->level;

                             for(j = k; j < nodeOccupancy-1; j++){
                               newCurrNode->keyArray[j-k] = currNode->keyArray[j+1];
                               newCurrNode->pageNoArray[j-k] = currNode->pageNoArray[j+1];
                               newCurrNode->numKeys++;
                             }

                             newCurrNode->keyArray[j-k] = endPagePair->key;
                             newCurrNode->pageNoArray[j-k] = currNode->pageNoArray[j+1];
                             newCurrNode->pageNoArray[j-k+1] = endPagePair->pageNo;
                             newCurrNode->numKeys++;

                             // If it is the root
                             if(pageStack.size() == 0){
                               // std::cout << "ROOT" << std::endl;
                               Page* newRootPage;
                               bufMgr->allocPage(file, rootPageNum, newRootPage);

                               NonLeafNodeInt* newRootInt = (NonLeafNodeInt*)newRootPage;

                               newRootInt->level = currNode->level + 1;
                               newRootInt->pageNoArray[0] = currPageId;
                               newRootInt->pageNoArray[1] = pagePair->pageNo;
                               newRootInt->keyArray[0] = pagePair->key;
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
                             bufMgr->unPinPage(file, newPageId, true);
                             bufMgr->unPinPage(file, currPageId, true);
                           }
                           else{
                             // If the parent node is not full
                             insertIntNonLeafArray(currNode->keyArray, currNode->pageNoArray, currNode->numKeys, pagePair);

                             bufMgr->unPinPage(file, currPageId, true);
                             break;
                           }
                         }
                       }
                       else{
                         // If leafNode is not full
                         insertIntLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

                         // Write the changes
                         bufMgr->unPinPage(file, currPageId, true);
                         bufMgr->unPinPage(file, rootPageNum, false);
                         return;
                       }
                     }
                     break;
                   }
      case DOUBLE:{
                    // Initialize the variables
                    NonLeafNodeDouble* rootDouble = (NonLeafNodeDouble*)rootPage;
                    rootDouble->level = 0;
                    double keyDouble = *(double*)key;
                    RIDKeyPair<double>* keyPair = new RIDKeyPair<double>(rid, keyDouble);
                    Page* leafPage;

                    if(rootDouble->level == 0){
                      // If there is only one leaf node, then check if the leaf node exist
                      if(rootDouble->pageNoArray[0] == 0){
                        // If the leaf node does not exist yet(first insert), create the first leaf node
                        bufMgr->allocPage(file, rootDouble->pageNoArray[0], leafPage);
                        LeafNodeDouble* leafDouble = (LeafNodeDouble*)leafPage;

                        // Insert entry
                        leafDouble->keyArray[0] = keyPair->key;
                        leafDouble->ridArray[0] = keyPair->rid;
                        leafDouble->numKeys = 1;
                        leafDouble->rightSibPageNo = 0;

                        // Write page
                        bufMgr->unPinPage(file, rootDouble->pageNoArray[0], true);
                        bufMgr->unPinPage(file, rootPageNum, true);
                      }
                      else{
                        // If the first leaf node exist
                        bufMgr->readPage(file, rootDouble->pageNoArray[0], leafPage);

                        // Get the leaf node
                        LeafNodeDouble* leafNode = (LeafNodeDouble*)leafPage;

                        // If the leaf node is full, split the root
                        if(leafNode->numKeys >= leafOccupancy){
                          // Remove the last item to be inserted later
                          RIDKeyPair<double>* endKeyPair = new RIDKeyPair<double>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
                          leafNode->numKeys--;

                          // Insert the keyPair
                          insertDoubleLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

                          // Compare the extra key Pair
                          RIDKeyPair<double>* currKeyPair = new RIDKeyPair<double>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
                          swapDoubleRIDKeyPair(endKeyPair, currKeyPair);
                          leafNode->keyArray[leafOccupancy-1] = currKeyPair->key;
                          leafNode->ridArray[leafOccupancy-1] = currKeyPair->rid;
                          // The array is now sorted

                          // Split the node
                          Page* newLeafPage;
                          bufMgr->allocPage(file, rootDouble->pageNoArray[1], newLeafPage);
                          LeafNodeDouble* newLeafNode = (LeafNodeDouble*)newLeafPage;

                          // The position (k to end) in the array that is moved to another node
                          int k = (leafOccupancy+1)/2;

                          // Set the root node
                          rootDouble->level++;
                          rootDouble->keyArray[0] = leafNode->keyArray[k];
                          rootDouble->numKeys++;

                          // Set the left leaf node
                          leafNode->numKeys = k;
                          leafNode->rightSibPageNo = rootDouble->pageNoArray[1];

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
                          bufMgr->unPinPage(file, rootDouble->pageNoArray[0], true);
                          bufMgr->unPinPage(file, rootDouble->pageNoArray[1], true);
                          bufMgr->unPinPage(file, rootPageNum, true);
                        }
                        else{
                          // Insert the key
                          insertDoubleLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

                          // Write the changes
                          bufMgr->unPinPage(file, rootDouble->pageNoArray[0], true);
                          bufMgr->unPinPage(file, rootPageNum, false);
                        }
                      }
                    }
                    else{
                      // Search the tree
                      int currLevel = rootDouble->level;

                      // Traverse the non-leaf level
                      Page* currPage;
                      PageId currPageId = rootPageNum;
                      PageId rightPageId; // For setting the right sibling of the leaf node
                      PageId prevPageId = currPageId;

                      // Stack to store pageId for reverse traversal
                      std::stack<PageId> pageStack;

                      // Traverse down the tree to search for insert location
                      while(currLevel > 0){
                        bufMgr->readPage(file, currPageId, currPage);
                        pageStack.push(currPageId);
                        NonLeafNodeDouble* currDouble = (NonLeafNodeDouble*)currPage;

                        int i = 0;
                        for(i = 0; i < currDouble->numKeys; i++){
                          if(!(keyDouble >= currDouble->keyArray[i])){
                            break;
                          }
                        }

                        prevPageId = currPageId;
                        currPageId = currDouble->pageNoArray[i];

                        // For setting leafNode right sibling later
                        if(!(i >= nodeOccupancy)){
                          rightPageId = currDouble->pageNoArray[i+1];
                        }
                        else{
                          rightPageId = 0;
                        }

                        bufMgr->unPinPage(file, prevPageId, false);
                        currLevel --;
                      }

                      // Reach the leaf level
                      bufMgr->readPage(file, currPageId, currPage);
                      LeafNodeDouble* leafNode = (LeafNodeDouble*)currPage;

                      // Check if the leaf node is full
                      if(leafNode->numKeys >= leafOccupancy){
                        // Remove the last item
                        RIDKeyPair<double>* endKeyPair = new RIDKeyPair<double>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
                        leafNode->numKeys--;

                        insertDoubleLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

                        // Compare the extra key Pair
                        RIDKeyPair<double>* currKeyPair = new RIDKeyPair<double>(leafNode->ridArray[leafOccupancy-1], leafNode->keyArray[leafOccupancy-1]);
                        swapDoubleRIDKeyPair(endKeyPair, currKeyPair);
                        leafNode->keyArray[leafOccupancy-1] = currKeyPair->key;
                        leafNode->ridArray[leafOccupancy-1] = currKeyPair->rid;
                        // The array is now sorted

                        // Split the node
                        PageId newLeafNodeId;
                        Page* newLeafPage;
                        bufMgr->allocPage(file, newLeafNodeId, newLeafPage);
                        LeafNodeDouble* newLeafNode = (LeafNodeDouble*)newLeafPage;

                        // The position (k to end) in the array that is moved to another node
                        int k = (leafOccupancy+1)/2;
                        PageKeyPair<double>* pagePair = new PageKeyPair<double>(newLeafNodeId, leafNode->keyArray[k]);

                        // Set the left leaf node
                        leafNode->numKeys = k;
                        leafNode->rightSibPageNo = newLeafNodeId;

                        // Set the right leaf node
                        newLeafNode->rightSibPageNo = rightPageId;
                        newLeafNode->numKeys = 0;

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
                        bufMgr->unPinPage(file, currPageId, true);
                        bufMgr->unPinPage(file, newLeafNodeId, true);
                        bufMgr->unPinPage(file, rootPageNum, true);

                        // Reverse traversal up the tree
                        while(pageStack.size() > 0){
                          // Get the parent node pageId
                          currPageId = pageStack.top();
                          pageStack.pop();

                          bufMgr->readPage(file, currPageId, currPage);

                          NonLeafNodeDouble* currNode = (NonLeafNodeDouble*)currPage;

                          // Check if the parent node is full
                          if(currNode->numKeys >= nodeOccupancy){
                            // If the current node is full, split the node

                            // Remove the last item
                            PageKeyPair<double>* endPagePair = new PageKeyPair<double>(currNode->pageNoArray[nodeOccupancy-1], currNode->keyArray[nodeOccupancy-1]);
                            currNode->numKeys--;

                            // Insert the key and pageNo
                            insertDoubleNonLeafArray(currNode->keyArray, currNode->pageNoArray, currNode->numKeys, pagePair);

                            PageKeyPair<double>* currPagePair = new PageKeyPair<double>(currNode->pageNoArray[nodeOccupancy], currNode->keyArray[nodeOccupancy-1]);
                            swapDoublePageKeyPair(endPagePair, currPagePair);
                            currNode->keyArray[leafOccupancy-1] = currPagePair->key;
                            currNode->pageNoArray[leafOccupancy-1] = currPagePair->pageNo;
                            // The array is now sorted

                            // Split the node
                            Page* newPage;
                            PageId newPageId;
                            bufMgr->allocPage(file, newPageId, newPage);
                            NonLeafNodeDouble* newCurrNode = (NonLeafNodeDouble*)newPage;

                            // The position (k to end) in the array that is moved to another node
                            k = (nodeOccupancy+1)/2;
                            pagePair->set(newPageId, currNode->keyArray[k]);

                            // Set the left node
                            currNode->numKeys = k;

                            // Set the right node
                            newCurrNode->numKeys = 0;
                            newCurrNode->level = currNode->level;

                            for(j = k; j < nodeOccupancy-1; j++){
                              newCurrNode->keyArray[j-k] = currNode->keyArray[j+1];
                              newCurrNode->pageNoArray[j-k] = currNode->pageNoArray[j+1];
                              newCurrNode->numKeys++;
                            }

                            newCurrNode->keyArray[j-k] = endPagePair->key;
                            newCurrNode->pageNoArray[j-k] = currNode->pageNoArray[j+1];
                            newCurrNode->pageNoArray[j-k+1] = endPagePair->pageNo;
                            newCurrNode->numKeys++;

                            // If it is the root
                            if(pageStack.size() == 0){
                               std::cout << "ROOT" << std::endl;
                              Page* newRootPage;
                              bufMgr->allocPage(file, rootPageNum, newRootPage);

                              NonLeafNodeDouble* newrootDouble = (NonLeafNodeDouble*)newRootPage;

                              newrootDouble->level = currNode->level + 1;
                              newrootDouble->pageNoArray[0] = currPageId;
                              newrootDouble->pageNoArray[1] = pagePair->pageNo;
                              newrootDouble->keyArray[0] = pagePair->key;
                              newrootDouble->numKeys = 1;

                              bufMgr->unPinPage(file, rootPageNum, true);

                              // Read the metadata
                              Page* metadataPage;
                              bufMgr->readPage(file, headerPageNum, metadataPage);
                              IndexMetaInfo* metadata = (IndexMetaInfo*)metadataPage;
                              metadata->rootPageNo = rootPageNum;

                              bufMgr->unPinPage(file, headerPageNum, true);
                            }

                            // Write the changes
                            bufMgr->unPinPage(file, newPageId, true);
                            bufMgr->unPinPage(file, currPageId, true);
                          }
                          else{
                            // If the parent node is not full
                            insertDoubleNonLeafArray(currNode->keyArray, currNode->pageNoArray, currNode->numKeys, pagePair);

                            bufMgr->unPinPage(file, currPageId, true);
                            break;
                          }
                        }
                      }
                      else{
                        // If leafNode is not full
                        insertDoubleLeafArray(leafNode->keyArray, leafNode->ridArray, leafNode->numKeys, keyPair);

                        // Write the changes
                        bufMgr->unPinPage(file, currPageId, true);
                        bufMgr->unPinPage(file, rootPageNum, false);
                        return;
                      }
                    }
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

    // Check parameter
    if(lowOpParm != GT && lowOpParm != GTE){
      throw BadOpcodesException();
    }

    lowOp = lowOpParm;

    if(highOpParm != LT && highOpParm != LTE){
      throw BadOpcodesException();
    }

    highOp = highOpParm;
    //printTree();
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

                     this->scanExecuting = true;
                     bool entryFound = false;

                     // Search through the current leaf node
                     int i = 0;
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

                     if(!entryFound){
                       std::cout << "No entry found" << std::endl;
                     }

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
                    lowValDouble = *(double*)lowValParm;
                    highValDouble = *(double*)highValParm;

                    if(lowValDouble > highValDouble){
                      throw BadScanrangeException();
                    }

                    // Scan for the low Value
                    Page* rootPage;
                    bufMgr->readPage(file, rootPageNum, rootPage);
                    NonLeafNodeDouble* rootDouble = (NonLeafNodeDouble*)rootPage;
                    Page* currPage;
                    int currLevel = rootDouble->level;
                    PageId currPageId = rootPageNum;
                    PageId prevPageId = rootPageNum;

                    bufMgr->unPinPage(file, rootPageNum, false);

                    while(currLevel > 0){
                      bufMgr->readPage(file, currPageId, currPage);
                      NonLeafNodeDouble* currDouble = (NonLeafNodeDouble*)currPage;

                      int i = 0;

                      for(i = 0; i < currDouble->numKeys; i++){
                        if(!(lowValDouble >= currDouble->keyArray[i])){
                          break;
                        }
                      }

                      prevPageId = currPageId;
                      currPageId = currDouble->pageNoArray[i];

                      bufMgr->unPinPage(file, prevPageId, false);
                      currLevel--;
                    }

                    // Reach the leaf level
                    this->currentPageNum = currPageId;

                    bufMgr->readPage(file, this->currentPageNum, this->currentPageData);
                    LeafNodeDouble* leafDouble = (LeafNodeDouble*)this->currentPageData;

                    this->scanExecuting = true;
                    bool entryFound = false;

                    // Search through the current leaf node
                    int i = 0;
                    std::cout << leafDouble->numKeys << "keyssss" << std::endl;
                    for(i = 0; i < leafDouble->numKeys; i++){
                      if(lowOp == GT){
                          std::cout << "ever here?" << leafDouble->keyArray[i] << std::endl;
                        if(leafDouble->keyArray[i] > lowValDouble){
                          this->nextEntry = i;
                          entryFound = true;
                          break;
                        }
                      }
                      else if(lowOp == GTE){
                        if(leafDouble->keyArray[i] >= lowValDouble){
                          this->nextEntry = i;
                          entryFound = true;
                          break;
                        }
                      }
                    }

                    if(!entryFound){
                      std::cout << "No entry found" << std::endl;
                    }

                    // Search the right sibling if it is not found in this current leaf node
                    if(!entryFound){
                      PageId rightPageId = leafDouble->rightSibPageNo;
                      bufMgr->unPinPage(file, this->currentPageNum, false);
                      this->currentPageNum = rightPageId;

                      bufMgr->readPage(file, this->currentPageNum, this->currentPageData);
                      leafDouble = (LeafNodeDouble*)this->currentPageData;

                      for(i = 0; i < leafDouble->numKeys; i++){
                        if(lowOp == GT){
                          if(leafDouble->keyArray[i] > lowValDouble){
                            this->nextEntry = i;
                            entryFound = true;
                            break;
                          }
                        }
                        else if(lowOp == GTE){
                          if(leafDouble->keyArray[i] >= lowValDouble){
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
                      if(!(leafDouble->keyArray[this->nextEntry] < highValDouble)){
                        endScan();
                        throw NoSuchKeyFoundException();
                      }
                    }
                    else if(highOp == LTE){
                      if(!(leafDouble->keyArray[this->nextEntry] <= highValDouble)){
                        endScan();
                        throw NoSuchKeyFoundException();
                      }
                    }
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
      throw IndexScanCompletedException();
    }

    switch(attributeType){
      case INTEGER:{
                     LeafNodeInt* leafInt = (LeafNodeInt*)currentPageData;

                     if(highOp == LT){
                       if(!(leafInt->keyArray[this->nextEntry] < highValInt)){
                         throw IndexScanCompletedException();
                       }
                     }
                     else if(highOp == LTE){
                       if(!(leafInt->keyArray[this->nextEntry] <= highValInt)){
                         throw IndexScanCompletedException();
                       }
                     }

                     outRid = leafInt->ridArray[this->nextEntry];

                     if(this->nextEntry + 1 >= leafInt->numKeys){
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
                    LeafNodeDouble* leafDouble = (LeafNodeDouble*)currentPageData;

                    if(highOp == LT){
                      if(!(leafDouble->keyArray[this->nextEntry] < highValDouble)){
                        throw IndexScanCompletedException();
                      }
                    }
                    else if(highOp == LTE){
                      if(!(leafDouble->keyArray[this->nextEntry] <= highValDouble)){
                        throw IndexScanCompletedException();
                      }
                    }

                    outRid = leafDouble->ridArray[this->nextEntry];

                    if(this->nextEntry + 1 >= leafDouble->numKeys){
                      PageId rightPageId = leafDouble->rightSibPageNo;

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

  // Insert PageKeyPair into arrays in non-leaf
  void BTreeIndex::insertIntNonLeafArray(void* array, void* pageArray, int& numItems, PageKeyPair<int>* x){
    int* arr = (int*)array;
    PageId* pageArr = (PageId*)pageArray;

    int i = 0;
    for(i = 0; i < numItems; i++){
      if(arr[i] > x->key){
        break;
      }
    }

    for(int j = numItems; j > i; j--){
      arr[j] = arr[j-1];
      pageArr[j+1] = pageArr[j];
    }

    arr[i] = x->key;
    pageArr[i+1] = x->pageNo;

    numItems++;
  }

  // Insert RIDKeyPair into arrays in the leaf
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
      ridArr[j] = ridArr[j-1];
    }

    arr[i] = x->key;
    ridArr[i] = x->rid;

    numItems++;
  }

  // Swap x PageKeyPair with y PageKeyPair if x < y
  void BTreeIndex::swapIntPageKeyPair(PageKeyPair<int>* x, PageKeyPair<int>* y){
    if(x->key < y->key){
      int tempKey = x->key;
      PageId tempId = x->pageNo;

      x->set(y->pageNo, y->key);
      y->set(tempId, tempKey);
    }
  }

  // Swap x keyPair with ykeyPair if x < y
  void BTreeIndex::swapIntRIDKeyPair(RIDKeyPair<int>* x, RIDKeyPair<int>* y){
    if(x->key < y->key){
      int tempKey = x->key;
      RecordId tempRid = x->rid;

      x->set(y->rid, y->key);
      y->set(tempRid, tempKey);
    }
  }

  // Insert RIDKeyPair into arrays in the leaf
  void BTreeIndex::insertDoubleLeafArray(void* array, void* ridArray, int& numItems, RIDKeyPair<double>* x){
    double* arr = (double*)array;
    RecordId* ridArr = (RecordId*)ridArray;

    int i = 0;
    for(i = 0; i < numItems; i++){
      if(arr[i] > x->key){
        break;
      }
    }

    for(int j = numItems; j > i; j--){
      arr[j] = arr[j-1];
      ridArr[j] = ridArr[j-1];
    }

    arr[i] = x->key;
    ridArr[i] = x->rid;

    numItems++;
  }

  // Insert PageKeyPair into arrays in non-leaf
  void BTreeIndex::insertDoubleNonLeafArray(void* array, void* pageArray, int& numItems, PageKeyPair<double>* x){
    double* arr = (double*)array;
    PageId* pageArr = (PageId*)pageArray;

    int i = 0;
    for(i = 0; i < numItems; i++){
      if(arr[i] > x->key){
        break;
      }
    }

    for(int j = numItems; j > i; j--){
      arr[j] = arr[j-1];
      pageArr[j+1] = pageArr[j];
    }

    arr[i] = x->key;
    pageArr[i+1] = x->pageNo;

    numItems++;
  }

  // Swap x PageKeyPair with y PageKeyPair if x < y
  void BTreeIndex::swapDoublePageKeyPair(PageKeyPair<double>* x, PageKeyPair<double>* y){
    if(x->key < y->key){
      double tempKey = x->key;
      PageId tempId = x->pageNo;

      x->set(y->pageNo, y->key);
      y->set(tempId, tempKey);
    }
  }

  // Swap x keyPair with ykeyPair if x < y
  void BTreeIndex::swapDoubleRIDKeyPair(RIDKeyPair<double>* x, RIDKeyPair<double>* y){
    if(x->key < y->key){
      double tempKey = x->key;
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
                     std::cout << "Key array: " <<  std::endl;
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

