/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

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

  int ridIndex = 0;
  bool scanCompleted = false;
  BTreeIndex::BTreeIndex(const std::string & relationName,
      std::string & outIndexName,
      BufMgr *bufMgrIn,
      const int attrByteOffset,
      const Datatype attrType)
  {
    bool indexFileNotFound = false;

    //Code for constructing an index name
    std :: ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std :: string indexName = idxStr.str(); // indexName is the name of the index file

    outIndexName = indexName;
    this->bufMgr = bufMgrIn;

    try{
      file = new BlobFile(outIndexName, false);

      Page *pagePtr;
      PageId pageNum = 1;
      bufMgr->readPage(file, pageNum, pagePtr);

      //verify header information is correct
      struct IndexMetaInfo* header = (IndexMetaInfo*) pagePtr;
      if((header->attrType != attrType) || (header->attrByteOffset != attrByteOffset)) {
        throw BadIndexInfoException("Index header is incorrect.");
      }

      bufMgr->unPinPage(file, pageNum, false);
      //Identifing root page num
      rootPageNum = ((IndexMetaInfo*) pagePtr)->rootPageNo;
    }
    catch(FileNotFoundException e){
      indexFileNotFound = true;
    }

    //If index file does not exist
    if(indexFileNotFound){
      file = new BlobFile(outIndexName, true);     

      Page *pagePtr;
      Page *headPagePtr;
      PageId pageNum = 1;
      PageId headPageNum;

      bufMgr->allocPage(file, headPageNum, headPagePtr);
      bufMgr->allocPage(file, pageNum, pagePtr);

      struct IndexMetaInfo* header = (IndexMetaInfo*) headPagePtr;

      header->attrByteOffset = attrByteOffset;
      //    this->attributType = attrType;  
      header->attrType = attrType;
      //    this->attrByteOffset = attrByteOffset;
      header->rootPageNo = pageNum;
      headerPageNum = headPageNum;
      rootPageNum = pageNum;

      FileScan* scanner = new FileScan(relationName, bufMgr);
      try{
        RecordId id;
        const char *record;
        double *key;
        bool done = false;

        if(attrType == INTEGER){
          ((LeafNodeInt*) pagePtr)->level = 0;
          ((LeafNodeInt*) pagePtr)->entries = 0;
        }

        while(!done){
          //Use EOF exception to end 
          scanner->scanNext(id);
          record = scanner->getRecord().c_str();
          key = (double*)(record+attrByteOffset);
          insertEntry(key, id);
        }
      }
      catch(EndOfFileException e){
        //Once the end of the file has been reached, unpin pages
        bufMgr->unPinPage(file, pageNum, true);
        bufMgr->unPinPage(file, headPageNum, true);
      }
      delete scanner;
    }

    //Set fields based on what type of entry is being stored
    this->attrByteOffset = attrByteOffset;
    this->attributeType = attrType;
    if(attributeType == INTEGER){
      nodeOccupancy = INTARRAYNONLEAFSIZE;
      leafOccupancy = INTARRAYLEAFSIZE;
    }

  }

  // -----------------------------------------------------------------------------
  // BTreeIndex::~BTreeIndex -- destructor
  // -----------------------------------------------------------------------------

  BTreeIndex::~BTreeIndex()
  {
    //flush file from buffer manager and delete it
    bufMgr->flushFile(file);
    delete file;
  }

  // -----------------------------------------------------------------------------
  // BTreeIndex::insertEntry
  // -----------------------------------------------------------------------------

  int rootLevel = 0;
  const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
  {
    Page* top;
    PageId pageSplit = 0;
    void *middle = new int;
    if(attributeType == INTEGER){
      pageSplit = insertIntegerRecord(key, rid, rootPageNum); 
      if(pageSplit != 0){
        PageId rootId;
        Page *root;
        bufMgr->allocPage(file, rootId, root);
        struct NonLeafNodeInt *rootN = (NonLeafNodeInt*) root;

        rootN->entries = 1;
        rootLevel++;
        rootN->level = rootLevel;
        rootN->keyArray[0] = *((int*)middle);
        rootN->pageNoArray[0] = rootId;
        rootN->pageNoArray[1] = pageSplit;
      
        bufMgr->readPage(file, headerPageNum, top);
        struct IndexMetaInfo* meta = (IndexMetaInfo*) top;
        meta->rootPageNo = rootId;
        rootPageNum = rootId;
      
        bufMgr->unPinPage(file, rootId, true);
        bufMgr->unPinPage(file, headerPageNum, true);
      }
    }
  }

  PageId BTreeIndex::insertIntegerRecord(const void *key, const RecordId rid, PageId currPage){
    Page *page;
    PageId toReturn = 0;

    bufMgr->readPage(file, currPage, page);
    struct LeafNodeInt *leaf = (LeafNodeInt*) page;
    struct NonLeafNodeInt *nonLeaf = (NonLeafNodeInt*) page;

    
    if(nonLeaf->level != 0){ //non-leaf node
     //TODO

    }
    else{ //leaf node
      if(leaf->entries < leafOccupancy){ //We have room to insert entry
        //shiftInsert(leafNode, key, (void*)&rid, leafNode->level);
        sort(leaf, key, (void*)&rid, true);
        leaf->entries = leaf->entries + 1;
      }
      else{//We don't have room to insert
        PageId pageId;
        Page *page;
        int middleVal = leafOccupancy/2-1;
        int middle = leaf->keyArray[middleVal];
        bufMgr->allocPage(file, pageId, page);
        struct LeafNodeInt *newLeaf = (LeafNodeInt*) page;
        toReturn = pageId;

        //update internals
        newLeaf->level = 0;
        newLeaf->entries = 0;
        newLeaf->rightSibPageNo = leaf->rightSibPageNo;
        leaf->rightSibPageNo = pageId;

        //split
        for(int x = middleVal; x < leafOccupancy; x++){
          sort(newLeaf, (void*)&leaf->keyArray[x], (void*)&leaf->ridArray[x], true);       
          newLeaf->entries = newLeaf->entries + 1;
          leaf->entries = leaf->entries - 1;
          leaf->keyArray[x] = 0;
        }

        //insert the new entry in either the new leaf or old leaf depending on comparison to middle val
        if(*(int*)key > *(int*)middle){
          sort(newLeaf, key, (void*)&rid, true);
          newLeaf->entries = newLeaf->entries + 1;
        }
        else{  
          sort(leaf, key, (void*)&rid, true);
          leaf->entries = leaf->entries + 1;
        }
        
        //Unpin the new page
        bufMgr->unPinPage(file, pageId, true);
      }
      
      bufMgr->unPinPage(file, currPage, true);
      return toReturn;
    }
  }
  //const void BTreeIndex::sort(const void *key, const RecordId rid, Page *page, bool isLeaf){
  const void BTreeIndex::sort(void *page, const void *key, void *rid, bool isLeaf){
    int insertPosition = -1;
    int actualKey = *(int *) key;  

    if(isLeaf){
      int tempKey = 0;
      struct LeafNodeInt *leaf = (LeafNodeInt*) page;
      RecordId actualRecord = *(RecordId *)(void*)&rid;
      RecordId tempRecord = leaf->ridArray[0]; 

      for(int x = 0; x <= leaf->entries; x++){
        //Either insert at "right-most entry" or if less than current value, swap
        //Keep it sorted
        if(x == leaf->entries || actualKey < leaf->keyArray[x]){
          tempKey = leaf->keyArray[x];
          leaf->keyArray[x] = actualKey;
          actualKey = tempKey;
          if(insertPosition == -1){
            insertPosition = x;
          }
        }
      }
      for(int x = 0; x <= leaf->entries; x++){
        if(x >= insertPosition){
          tempRecord = leaf->ridArray[x];
          leaf->ridArray[x] = actualRecord;
          actualRecord = tempRecord;
        }
      }
    }
    else{ //non leaf
      struct NonLeafNodeInt *nonLeaf = (NonLeafNodeInt*)page;
      int tempKey = nonLeaf->keyArray[0];
      PageId tempPage = nonLeaf->pageNoArray[0];
      PageId pid = *(PageId*)page;
      
      for(int x=0; x <= nonLeaf->entries; x++){
        if(x == nonLeaf->entries || nonLeaf->keyArray[x] > actualKey){
          tempKey = nonLeaf->keyArray[x];
          nonLeaf->keyArray[x] = actualKey;
          actualKey = tempKey;
          if(insertPosition == -1){
            insertPosition = x;
          }
        }
      }
      for(int x=0; x <= nonLeaf->entries + 1; x++){
        if(x >= insertPosition + 1){
          tempPage = nonLeaf->pageNoArray[x];
          nonLeaf->pageNoArray[x] = pid;
          pid = tempPage;
        }
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
    bool done = false;
    scanCompleted = false;
    scanExecuting = true;
    currentPageNum = rootPageNum;

    if(attributeType == INTEGER){
      lowValInt = *(int*)lowValParm;
      lowOp = lowOpParm;
      highValInt = *(int*)highValParm;
      highOp = highOpParm;

      if(lowValInt > highValInt){
        scanExecuting = false;
        throw BadScanrangeException();
      }
      if(lowOp == LT || lowOp == LTE){
        scanExecuting = false;
        throw BadOpcodesException();
      }
      if(highOp == GT || highOp == GTE){
        scanExecuting = false;
        throw BadOpcodesException();
      }

      while(!done){
        bufMgr->readPage(file, currentPageNum, currentPageData);
        struct NonLeafNodeInt *nonLeaf = (NonLeafNodeInt*) currentPageData;
        struct LeafNodeInt *leaf = (LeafNodeInt*) currentPageData;
        if(nonLeaf->level != 0){ //non leaf
          PageId prevPage = currentPageNum;
          if(nonLeaf->keyArray[0] > lowValInt){//less than smallest
            currentPageNum = nonLeaf->pageNoArray[0];
          }
          else if(nonLeaf->keyArray[nonLeaf->entries -1] <= lowValInt){//bigger than greatest
            currentPageNum = nonLeaf->pageNoArray[nonLeaf->entries];
          }
          else{//somewhere in between
            for(int x = 0; x < nonLeaf->entries; x++){
              if(nonLeaf->keyArray[x] >= lowValInt){
                currentPageNum = nonLeaf->pageNoArray[x];
                break;
              }
            }
          }
          bufMgr->unPinPage(file, prevPage,false);
        }
        else{ //leaf
          for(int x = 0; x < leaf->entries; x++){
            if(leaf->keyArray[x] >= lowValInt && lowOp == GTE){
              done = true;
              ridIndex = x;
            }
            else if(leaf->keyArray[x] > lowValInt){//GT
              done = true;
              ridIndex = x;
            }
          }
          if(!done){//page not found. bad conditions
            scanCompleted=true;
            break;
          }
        }
      }
    }
  }

  // -----------------------------------------------------------------------------
  // BTreeIndex::scanNext
  // -----------------------------------------------------------------------------

  const void BTreeIndex::scanNext(RecordId& outRid) 
  {
    if(attributeType == INTEGER){
      struct LeafNodeInt* leaf = (LeafNodeInt*) currentPageData;
      PageId prev = currentPageNum;
      currentPageNum = leaf->rightSibPageNo;
      if(scanCompleted)
        throw IndexScanCompletedException();
      if(!scanExecuting)
        throw ScanNotInitializedException();
      
      if(highOp == LT && leaf->keyArray[ridIndex] < highValInt)
        outRid = leaf->ridArray[ridIndex];
      else if(highOp == LTE && leaf->keyArray[ridIndex] <= highValInt)
        outRid = leaf->ridArray[ridIndex];
      else
        throw IndexScanCompletedException();

      if(ridIndex < leaf->entries - 1){
        ridIndex += 1;
      }
      else{
        ridIndex = 0;
        bufMgr->unPinPage(file, prev, false);
        try{
          if(currentPageNum == 0)
            throw IndexScanCompletedException();
          bufMgr->readPage(file, currentPageNum, currentPageData);
        }
        catch(IndexScanCompletedException e){
          scanCompleted = true;
          currentPageNum = 0;
        }
      }
    }
  }

  // -----------------------------------------------------------------------------
  // BTreeIndex::endScan
  // -----------------------------------------------------------------------------
  //
  const void BTreeIndex::endScan() 
  {
    if(!scanExecuting)
      throw ScanNotInitializedException();
    
    if(currentPageNum != 0)
      bufMgr->unPinPage(file, currentPageNum, false);
    scanExecuting = false; 
  }

}
