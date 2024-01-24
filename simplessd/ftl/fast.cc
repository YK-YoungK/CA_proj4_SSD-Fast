#include "ftl/fast.hh"

#include <algorithm>
#include <limits>
#include <random>

#include "util/algorithm.hh"
#include "util/bitset.hh"

namespace SimpleSSD {

namespace FTL {

Fast::Fast(ConfigReader &c, Parameter &p, PAL::PAL *l,
                         DRAM::AbstractDRAM *d)
: AbstractFTL(p, l, d),
      pPAL(l),
      conf(c),
      bReclaimMore(false) {
  blocks.reserve(param.totalPhysicalBlocks);
  
  block_table.reserve(param.totalLogicalBlocks);
  for (int i = 0; i < 6; ++i)
  {
    RW_LPN[i].reserve(param.pagesInBlock);
    for (uint64_t j = 0; j < param.pagesInBlock; ++j)
      RW_LPN[i].push_back(-1);
  }


  for (uint32_t i = 0; i < param.totalPhysicalBlocks; i++) {
    freeBlocks.emplace_back(Block(i, param.pagesInBlock, param.ioUnitInPage));
  }

  nFreeBlocks = param.totalPhysicalBlocks;
  
  pagesInBlock = param.pagesInBlock;
  pageSize = param.pageSize;

  status.totalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;

  // Init
  SW_LBN = -1;
  SW_last_LPN = -1;
  next_evict_rw = 0;
  next_write_rw = 0;
  next_write_offset = 0;
  rw_empty = true;

  // Allocate free blocks for SW and RW
  SW_PBN = getFreeBlock();
  for (int i = 0; i < 6; ++i)
    RW_PBN[i] = getFreeBlock();


  memset(&stat, 0, sizeof(stat));

  bRandomTweak = conf.readBoolean(CONFIG_FTL, FTL_USE_RANDOM_IO_TWEAK);
  bitsetSize = bRandomTweak ? param.ioUnitInPage : 1;
}


Fast::~Fast() {}


uint32_t Fast::getFreeBlock() 
{
  uint32_t blockIndex = 0;
  if (nFreeBlocks > 0)
  {
    auto iter = freeBlocks.begin();

    blockIndex = iter->getBlockIndex();

    // Insert found block to block list
    if (blocks.find(blockIndex) != blocks.end())
    {
      panic("Corrupted");
    }

    blocks.emplace(blockIndex, std::move(*iter));

    // Remove found block from free block list
    freeBlocks.erase(iter);
    nFreeBlocks--;
  }
  else
    panic("No free block left");
  
  return blockIndex;
}


void Fast::calculateTotalPages(uint64_t &valid, uint64_t &invalid) {
  valid = 0;
  invalid = 0;

  for (auto &iter : blocks) {
    valid += iter.second.getValidPageCount();
    invalid += iter.second.getDirtyPageCount();
  }
}


bool Fast::initialize() {
  uint64_t nPagesToWarmup;
  uint64_t nPagesToInvalidate;
  uint64_t nTotalLogicalPages;
  uint64_t maxPagesBeforeGC;
  uint64_t tick;
  uint64_t valid;
  uint64_t invalid;
  FILLING_MODE mode;

  Request req(param.ioUnitInPage);

  debugprint(LOG_FTL_PAGE_MAPPING, "Initialization started");

  nTotalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;
  nPagesToWarmup =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_FILL_RATIO);
  nPagesToInvalidate =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_INVALID_PAGE_RATIO);
  mode = (FILLING_MODE)conf.readUint(CONFIG_FTL, FTL_FILLING_MODE);
  maxPagesBeforeGC =
      param.pagesInBlock *
      (param.totalPhysicalBlocks *
           (1 - conf.readFloat(CONFIG_FTL, FTL_GC_THRESHOLD_RATIO)) -
       param.pageCountToMaxPerf);  // # free blocks to maintain

  if (nPagesToWarmup + nPagesToInvalidate > maxPagesBeforeGC) {
    warn("ftl: Too high filling ratio. Adjusting invalidPageRatio.");
    nPagesToInvalidate = maxPagesBeforeGC - nPagesToWarmup;
  }

  debugprint(LOG_FTL_PAGE_MAPPING, "Total logical pages: %" PRIu64,
             nTotalLogicalPages);
  debugprint(LOG_FTL_PAGE_MAPPING,
             "Total logical pages to fill: %" PRIu64 " (%.2f %%)",
             nPagesToWarmup, nPagesToWarmup * 100.f / nTotalLogicalPages);
  debugprint(LOG_FTL_PAGE_MAPPING,
             "Total invalidated pages to create: %" PRIu64 " (%.2f %%)",
             nPagesToInvalidate,
             nPagesToInvalidate * 100.f / nTotalLogicalPages);

  req.ioFlag.set();

  // Step 1. Filling
  if (mode == FILLING_MODE_0 || mode == FILLING_MODE_1) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Step 2. Invalidating
  if (mode == FILLING_MODE_0) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else if (mode == FILLING_MODE_1) {
    // Random
    // We can successfully restrict range of LPN to create exact number of
    // invalid pages because we wrote in sequential mannor in step 1.
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nPagesToWarmup - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Report
  calculateTotalPages(valid, invalid);
  debugprint(LOG_FTL_PAGE_MAPPING, "Filling finished. Page status:");
  debugprint(LOG_FTL_PAGE_MAPPING,
             "  Total valid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             valid, valid * 100.f / nTotalLogicalPages, nPagesToWarmup,
             (int64_t)(valid - nPagesToWarmup));
  debugprint(LOG_FTL_PAGE_MAPPING,
             "  Total invalid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             invalid, invalid * 100.f / nTotalLogicalPages, nPagesToInvalidate,
             (int64_t)(invalid - nPagesToInvalidate));
  debugprint(LOG_FTL_PAGE_MAPPING, "Initialization finished");

  return true;
}


void Fast::read(Request &req, uint64_t &tick) {
  uint64_t begin = tick;

  if (req.ioFlag.count() > 0) {
    readInternal(req, tick);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "READ  | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::READ);
}


void Fast::readInternal(Request &req, uint64_t &tick) {
  PAL::Request palRequest(req);
  uint64_t beginAt;
  uint64_t finishedAt = tick;

  uint64_t req_lbn = getLBN(req.lpn);
  uint64_t req_pageOffset = getPageOffset(req.lpn);

  auto mappingList = block_table.find(req_lbn);

  if (mappingList != block_table.end()) {
      if (req.ioFlag.test(0) || !bRandomTweak) {
        auto pbn = mappingList->second;

        if (pbn < param.totalPhysicalBlocks &&
            req_pageOffset < param.pagesInBlock) {
          palRequest.blockIndex = pbn;
          palRequest.pageIndex = req_pageOffset;

          if (bRandomTweak) {
            palRequest.ioFlag.reset();
            palRequest.ioFlag.set(0);
          }
          else {
            palRequest.ioFlag.set();
          }

          auto block = blocks.find(palRequest.blockIndex);

          if (block == blocks.end()) {
            panic("Block is not in use");
          }

          // Check corresponding page is valid or not
          bool valid = block->second.checkValid(palRequest.pageIndex, 0);

          if (valid)    // if valid, read
          {
            beginAt = tick;

            block->second.read(palRequest.pageIndex, 0, beginAt);
            pPAL->read(palRequest, beginAt);

            finishedAt = MAX(finishedAt, beginAt);
          }
          else          // if invalid, search in log blocks; remark: the order of searching! 
          {
            bool findpage = false;
            if (req_lbn == SW_LBN)     // first search in SW
            {
              palRequest.blockIndex = SW_PBN;
              auto sw_block = blocks.find(palRequest.blockIndex);
              if (sw_block == blocks.end())
              {
                panic("SW block is not in use");
              }
              beginAt = tick;

              if (sw_block->second.checkValid(findpage, 0))
              {
                findpage = true;

                sw_block->second.read(palRequest.pageIndex, 0, beginAt);
                pPAL->read(palRequest, beginAt);

                finishedAt = MAX(finishedAt, beginAt);
              }
            }

            if (!findpage)      // fail to search in SW, search in RW
            {
              uint32_t rw_idx = 0, rw_offset = 0;
              while (!findpage)
              {
                if (rw_offset == pagesInBlock - 1)
                  rw_idx = rw_idx + 1;
                rw_offset = (rw_offset + 1) % pagesInBlock;

                
                if (RW_LPN[rw_idx][rw_offset] == req.lpn)
                {
                  palRequest.blockIndex = RW_PBN[rw_idx];
                  palRequest.pageIndex = rw_offset;
                  
                  auto rw_block = blocks.find(palRequest.blockIndex);
                  if (rw_block == blocks.end())
                    panic("RW block is not in use");
                  beginAt = tick;

                  rw_block->second.read(palRequest.pageIndex, 0, beginAt);
                  pPAL->read(palRequest, beginAt);

                  finishedAt = MAX(finishedAt, beginAt);

                  findpage = true;
                  break;
                }

                if (rw_idx == 5 && rw_offset == pagesInBlock - 1)
                  break;
              }
            } 
          }
        }
      }

    tick = finishedAt;
    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::READ_INTERNAL);
  }
}


uint64_t Fast::getLBN(uint64_t LPN)
{
  uint64_t LBN = LPN / pagesInBlock;
  return LBN;
}
uint64_t Fast::getPageOffset(uint64_t LPN)
{
  uint64_t offset = LPN % pagesInBlock;
  return offset;
}


void Fast::write(Request &req, uint64_t &tick) {
  uint64_t begin = tick;

  if (req.ioFlag.count() > 0) {
    writeInternal(req, tick);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "WRITE | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::WRITE);
}


void Fast::writeInternal(Request &req, uint64_t &tick, bool sendToPAL) {
  PAL::Request palRequest(req);
  uint64_t req_lbn = getLBN(req.lpn);
  uint64_t req_pageOffset = getPageOffset(req.lpn);

  std::unordered_map<uint32_t, Block>::iterator block;
  auto mappingList = block_table.find(req_lbn);


  if (mappingList != block_table.end()) {
      if (req.ioFlag.test(0) || !bRandomTweak) {
        auto pbn = mappingList->second;

        if (pbn < param.totalPhysicalBlocks &&
            req_pageOffset < param.pagesInBlock) {
          block = blocks.find(pbn);

          // Three cases: erased, not erased but valid, invalid
          bool page_erased = block->second.checkErased(req_pageOffset, 0);
          bool page_valid = block->second.checkValid(req_pageOffset, 0);
          if (page_erased && (!page_valid))    // erased, can write
          {
            // write directly, no need to update mapping table
            actualWrite(req, tick, sendToPAL, block, req_pageOffset);
          }
          else if ((!page_erased) && page_valid)
          {
            // write to log block, update mapping table, invalidate old page
            write2LogBlock(req, tick, false);
          }
          else if ((!page_erased) && (!page_valid))
          {
            // Write to log block, update mapping table, invalidate old page
            write2LogBlock(req, tick, true);
          }
          else
            panic("Wrong page status of erased and valid, erased but valid page");
        }
      }
  }
  else {
    // Create empty mapping
    uint32_t new_pbn = getFreeBlock();
    // update block level mapping table
    auto ret = block_table.emplace(
        req_lbn,
        new_pbn);

    if (!ret.second) {
      panic("Failed to insert new mapping");
    }

    mappingList = ret.first;

    // write data, then return (write miss)
    block = blocks.find(new_pbn);
    if (block == blocks.end())
      panic("Error when add a new block and write for the first time");

    // write, update mapping table
    actualWrite(req, tick, sendToPAL, block, req_pageOffset);
  }
}


void Fast::actualWrite(Request &req, uint64_t &tick, bool sendToPAL, std::unordered_map<uint32_t, Block>::iterator block, uint32_t pageIndex)
{
  PAL::Request palRequest(req);
  uint64_t beginAt;
  uint64_t finishedAt = tick;
  
    if (req.ioFlag.test(0) || !bRandomTweak) {
      beginAt = tick;

      block->second.write(pageIndex, req.lpn, 0, beginAt);

      if (sendToPAL) {
        palRequest.blockIndex = block->first;
        palRequest.pageIndex = pageIndex;

        if (bRandomTweak) {
          palRequest.ioFlag.reset();
          palRequest.ioFlag.set(0);
        }
        else {
          palRequest.ioFlag.set();
        }

        pPAL->write(palRequest, beginAt);
      }

      finishedAt = MAX(finishedAt, beginAt);
    }

  // Exclude CPU operation when initializing
  if (sendToPAL) {
    tick = finishedAt;
    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::WRITE_INTERNAL);
  }
}


void Fast::write2LogBlock(Request &req, uint64_t &tick, bool hasLog)//, bool sendToPAL)
{
  PAL::Request palRequest(req);
  uint64_t req_lbn = getLBN(req.lpn);
  uint64_t req_pageOffset = getPageOffset(req.lpn);
  Bitset bit(param.ioUnitInPage);
  uint64_t beginAt;
  uint64_t readFinishedAt = tick;
  uint64_t writeFinishedAt = tick;
  uint64_t eraseFinishedAt = tick;
  std::vector<PAL::Request> readRequests;
  std::vector<PAL::Request> writeRequests;
  std::vector<PAL::Request> eraseRequests;
  PAL::Request pal_req(param.ioUnitInPage);

    // check # of corresponding pages, should be 1 if hasLog==true (indicating the case that the page has been invalidated and write to log blocks before), and 0 if hasLog==false (otherwise)
    // Invalidate the page at the same time if it's in RW (for the case in SW, we deal with it later)
    int cnt = 0;
    // search in SW
    if (req_lbn == SW_LBN)
    {
      auto block = blocks.find(SW_PBN);
      if (block == blocks.end())
        panic("Fail to find SW when searching to invalidate log page");
      if (block->second.checkValid(req_pageOffset, 0))
        cnt++;
    }
    // search in RW
    for (int i = 0; i < 6; ++i)
    {
      auto block = blocks.find(RW_PBN[i]);
      if (block == blocks.end())
        panic("Fail to find RW when searching to invalidate log page");
      
      for (uint32_t pageIndex = 0; pageIndex < pagesInBlock; pageIndex++)
      {
        if (RW_LPN[i][pageIndex] != (uint64_t)(-1) && RW_LPN[i][pageIndex] != (uint64_t)(-2) && req.lpn == RW_LPN[i][pageIndex])
        {
          cnt++;
          block->second.invalidate(pageIndex, 0);
          RW_LPN[i][pageIndex] = -2;
        }
      }
    }

    if (cnt == 0 && hasLog)
      panic("Fail to find previous log block when write to log block for >=2 times");
    if (cnt != 0 && (!hasLog))
      panic("Fail because find log pages when first write to log pages");
    if (cnt >= 2 && hasLog)
      panic("Fail because didn't invalidate log pages properly before");


  if (req_pageOffset == 0)
  {
    if (SW_LBN == (uint64_t)(-1))
      goto empty_sw;
    else if (SW_last_LPN + 1 == (SW_LBN + 1) * pagesInBlock)     // No empty pages in SW log block
    {
      // switch between SW & data block
      auto block_info = block_table.find(SW_LBN);
      auto origin_pbn = block_info->second;
      block_info->second = SW_PBN;
      
      // erase data block
      pal_req.blockIndex = origin_pbn;
      pal_req.pageIndex = 0;
      pal_req.ioFlag.set();
      eraseRequests.push_back(pal_req);
    }
    else
    {
      // find out SW block and data block
      auto block_info = block_table.find(SW_LBN);
      auto data_pbn = block_info->second;
      auto data_block = blocks.find(data_pbn);
      auto sw_block = blocks.find(SW_PBN);
      if (data_block == blocks.end())
        panic("Data block is not in use");
      if (sw_block == blocks.end())
        panic("SW block is not in use");
      
      // merge SW with corresponding data block: Here, we can copy data block to SW
      // Copy valid pages to SW block (notice iterator start and end!)
      for (uint32_t pageIndex = (SW_last_LPN % pagesInBlock) + 1; pageIndex < param.pagesInBlock; pageIndex++) {
        // Valid?
        if (data_block->second.checkValid(pageIndex, 0)){
          if (!bRandomTweak) {
            bit.set();
          }

          // Issue Read
          pal_req.blockIndex = data_block->first;
          pal_req.pageIndex = pageIndex;
          pal_req.ioFlag = bit;

          readRequests.push_back(pal_req);


            if (bit.test(0)) {
              // Invalidate
              data_block->second.invalidate(pageIndex, 0);

              sw_block->second.write(pageIndex, SW_last_LPN + pageIndex - SW_last_LPN % pagesInBlock, 0, beginAt);

              // Issue Write
              pal_req.blockIndex = SW_PBN;
              pal_req.pageIndex = pageIndex;

              if (bRandomTweak) {
                pal_req.ioFlag.reset();
                pal_req.ioFlag.set(0);
              }
              else {
                pal_req.ioFlag.set();
              }

              writeRequests.push_back(pal_req);

              stat.validPageCopies++;
            }


          stat.validSuperPageCopies++;
        }
        // invalid and not erased of data block, indicating there is newer data in RW
        else if ((!data_block->second.checkValid(pageIndex, 0)) && (!data_block->second.checkErased(pageIndex, 0)))
        {
          sw_block->second.fakeWrite(pageIndex, 0);
          sw_block->second.invalidate(pageIndex, 0);
        }
      }

      // switch between SW & data block
      block_info->second = SW_PBN;

      // erase data block
      pal_req.blockIndex = data_pbn;
      pal_req.pageIndex = 0;
      pal_req.ioFlag.set();
      eraseRequests.push_back(pal_req);
    }
    // get new SW, update SW next position, update SW mapping info
    SW_PBN = getFreeBlock();

    empty_sw:
    SW_LBN = req_lbn;
    SW_last_LPN = req.lpn;
    auto sw_newBlock = blocks.find(SW_PBN);
    if (sw_newBlock == blocks.end())
      panic("New SW block is not in use");
    // write data to SW
    sw_newBlock->second.write(0, req.lpn, 0, beginAt);

    // Issue Write
    pal_req.blockIndex = SW_PBN;
    pal_req.pageIndex = 0;

    if (bRandomTweak) {
      pal_req.ioFlag.reset();
      pal_req.ioFlag.set(0);
    }
    else {
      pal_req.ioFlag.set();
    }

    writeRequests.push_back(pal_req);

    // invalidate req.lpn corresponding page
    uint64_t req_lbn = getLBN(req.lpn);
    auto block_idx = block_table.find(req_lbn);
    if (block_idx == block_table.end())
      panic("Fail to find data block when invaliding pages after writing to log block");
    auto block = blocks.find(block_idx->second);
    block->second.invalidate(getPageOffset(req.lpn), 0);
  }
  else
  {
    if (req_lbn == SW_LBN)       // owner of SW = req_lbn
    {
      // SW_last_LPN
      if (req.lpn == SW_last_LPN + 1)
      {
        // write data to SW
        auto sw_block = blocks.find(SW_PBN);
        if (sw_block == blocks.end())
          panic("SW block is not in use");
        sw_block->second.write(req.lpn % pagesInBlock, req.lpn, 0, beginAt);
        // Issue Write
        pal_req.blockIndex = SW_PBN;
        pal_req.pageIndex = 0;

        if (bRandomTweak) {
          pal_req.ioFlag.reset();
          pal_req.ioFlag.set(0);
        }
        else {
          pal_req.ioFlag.set();
        }

        writeRequests.push_back(pal_req);
        // update SW mapping info (only next position)
        SW_last_LPN += 1;

        // invalidate req.lpn corresponding page
        uint64_t req_lbn = getLBN(req.lpn);
        auto block_idx = block_table.find(req_lbn);
        if (block_idx == block_table.end())
          panic("Fail to find data block when invaliding pages after writing to log block");
        auto block = blocks.find(block_idx->second);
        block->second.invalidate(getPageOffset(req.lpn), 0);
      }
      else
      {
        // find out SW block and data block
        auto block_info = block_table.find(SW_LBN);
        auto data_pbn = block_info->second;
        auto data_block = blocks.find(data_pbn);
        auto sw_block = blocks.find(SW_PBN);
        if (data_block == blocks.end())
          panic("Data block is not in use");
        if (sw_block == blocks.end())
          panic("SW block is not in use");
        // Merge SW with data block
        // step 1: get new block
        uint32_t new_block_pbn = getFreeBlock();
        auto new_block = blocks.find(new_block_pbn);
        if (new_block == blocks.end())
          panic("New data block is not in use");
        
        // step 2: copy
        for (uint32_t pageIndex = 0; pageIndex < pagesInBlock; pageIndex++)
        {
          if (pageIndex == req_pageOffset)
          {
            // write the most up-to-date data
            if (!bRandomTweak) 
              bit.set();
            if (bit.test(0))
            {
              new_block->second.write(pageIndex, req.lpn, 0, beginAt);

              // Issue write
              pal_req.blockIndex = new_block_pbn;
              pal_req.pageIndex = pageIndex;

              if (bRandomTweak)
              {
                pal_req.ioFlag.reset();
                pal_req.ioFlag.set(0);
              }
              else
                pal_req.ioFlag.set();

              writeRequests.push_back(pal_req);
            }

            if (sw_block->second.checkValid(pageIndex, 0))
            {
              sw_block->second.invalidate(pageIndex, 0);
              if (data_block->second.checkValid(pageIndex, 0))
                panic("sw block and data block is both valid on the same page offset");
            }
            else if (data_block->second.checkValid(pageIndex, 0))
              data_block->second.invalidate(pageIndex, 0);
          }
          else if (sw_block->second.checkValid(pageIndex, 0))   // sw is valid
          {
            // copy sw data
            if (!bRandomTweak) 
              bit.set();
            
            // issue read
            pal_req.blockIndex = sw_block->first;
            pal_req.pageIndex = pageIndex;
            pal_req.ioFlag = bit;
            readRequests.push_back(pal_req);

            if (bit.test(0))
            {
              new_block->second.write(pageIndex, SW_LBN * pagesInBlock + pageIndex, 0, beginAt);

              // Issue write
              pal_req.blockIndex = new_block_pbn;
              pal_req.pageIndex = pageIndex;

              if (bRandomTweak)
              {
                pal_req.ioFlag.reset();
                pal_req.ioFlag.set(0);
              }
              else
                pal_req.ioFlag.set();

              writeRequests.push_back(pal_req);
            }

            if (data_block->second.checkValid(pageIndex, 0))
              panic("sw block and data block is both valid on the same page offset");
            sw_block->second.invalidate(pageIndex, 0);
          }
          else if (data_block->second.checkValid(pageIndex, 0))   // data is valid
          {
            // copy data block
            if (!bRandomTweak) 
              bit.set();
            
            // issue read
            pal_req.blockIndex = data_block->first;
            pal_req.pageIndex = pageIndex;
            pal_req.ioFlag = bit;
            readRequests.push_back(pal_req);

            if (bit.test(0))
            {
              new_block->second.write(pageIndex, SW_LBN * pagesInBlock + pageIndex, 0, beginAt);

              // Issue write
              pal_req.blockIndex = new_block_pbn;
              pal_req.pageIndex = pageIndex;

              if (bRandomTweak)
              {
                pal_req.ioFlag.reset();
                pal_req.ioFlag.set(0);
              }
              else
                pal_req.ioFlag.set();

              writeRequests.push_back(pal_req);
            }

            data_block->second.invalidate(pageIndex, 0);
          }
          // both invalid (indicating SW should be erased) and data block is not erased, indicating there is newer data in RW 
          else if ((!data_block->second.checkValid(pageIndex, 0)) && (!sw_block->second.checkValid(pageIndex, 0)))
          {
            if (!sw_block->second.checkErased(pageIndex, 0))
              panic("Fail to write to SW block after LBN owns the SW, causes this error");
            
            if (!data_block->second.checkErased(pageIndex, 0))
            {
              // fake write, to mark the corresponding page as not erased, for further reading indexing to RW
              new_block->second.fakeWrite(pageIndex, 0);
              new_block->second.invalidate(pageIndex, 0);
            }
          }
          else
            panic("Fail to find proper setting to page when offset!=0 and merging SW");
        }
        // step 3: switch block
        block_info->second = new_block_pbn;

        // step 4: erase sw & data block
        pal_req.blockIndex = SW_PBN;
        pal_req.pageIndex = 0;
        pal_req.ioFlag.set();
        eraseRequests.push_back(pal_req);

        pal_req.blockIndex = data_pbn;
        pal_req.pageIndex = 0;
        pal_req.ioFlag.set();
        eraseRequests.push_back(pal_req);
        
        // get new SW
        SW_PBN = getFreeBlock();
        SW_LBN = -1;
        SW_last_LPN = -1;
      }
    }
    else
    {
      if (next_write_rw == next_evict_rw && next_write_offset == 0 && (!rw_empty)) // no rooms in RW
      {
        // merge victim with data blocks
        std::unordered_map<uint64_t, uint32_t> tmp_block_table; 
        std::unordered_map<uint64_t, uint32_t> old_block_table; // old data blocks, need to be erased

        // step 1: allocate new blocks for victims
        for (uint32_t pageIndex = pagesInBlock - 1; pageIndex != (uint32_t)(-1); pageIndex--)
        {
          if (RW_LPN[next_evict_rw][pageIndex] == (uint64_t)(-1))
            panic("Error because invalid lpn: erase before write");
          
          if (RW_LPN[next_evict_rw][pageIndex] == (uint64_t)(-2)) // invalid page
            continue;
          uint64_t tmp_lbn = getLBN(RW_LPN[next_evict_rw][pageIndex]);

          if (tmp_block_table.find(tmp_lbn) == tmp_block_table.end())
          {
            // insert new block, get from free blocks
            uint32_t tmp_new_pbn = getFreeBlock();
            // change block-level mapping table info
            auto ret = tmp_block_table.emplace(tmp_lbn, tmp_new_pbn);
            if (!ret.second) 
              panic("Failed to insert new mapping to tmp block table");

            auto tmp_iter = block_table.find(tmp_lbn);
            if (tmp_iter == block_table.end())
            {
              panic("Fail because pbn in RW doesn't appear in data block");
            }
            
            auto ret2 = old_block_table.emplace(tmp_lbn, tmp_iter->second); 
            if (!ret2.second)
              panic("Failed to insert mapping to old block table");
            tmp_iter->second = tmp_new_pbn;
          }
        }
        // step 2: scan RW in correct order, write to new data blocks
        for (uint32_t i = 0; i < 6; ++i)
        {
          uint32_t rw_idx = (next_evict_rw + 5 - i) % 6;
          auto rw_block = blocks.find(RW_PBN[rw_idx]);
          if (rw_block == blocks.end())
            panic("Fail to find RW log block");

          for (uint32_t pageIndex = pagesInBlock - 1; pageIndex != (uint32_t)(-1); pageIndex--)
          {
            if (RW_LPN[rw_idx][pageIndex] == (uint64_t)(-1))
              panic("Error because invalid RW: try to erase before write");
            
            if (RW_LPN[rw_idx][pageIndex] == (uint64_t)(-2)) // invalid page
              continue;
            // find lpn and get lbn, two cases
            // case 1: lbn in tmp_block_table, check offset; if erased, write data & invalidate; else, invalidate.
            // case 2: lbn not in tmp_block_table, skip
            uint64_t tmplbn = getLBN(RW_LPN[rw_idx][pageIndex]);
            uint64_t tmpoffset = getPageOffset(RW_LPN[rw_idx][pageIndex]);
            auto new_block_pdn = tmp_block_table.find(tmplbn); 
            auto old_block_pdn = old_block_table.find(tmplbn);
            if (new_block_pdn != tmp_block_table.end())
            {
              auto new_block = blocks.find(new_block_pdn->second);
              
              if (new_block->second.checkValid(tmpoffset, 0))
                panic("Two valid same lpn in RW blocks");
              if (new_block->second.checkErased(tmpoffset, 0))  // erased 
              {
                // erased, write
                // Issue read
                pal_req.blockIndex = old_block_pdn->second;
                pal_req.pageIndex = pageIndex;
                pal_req.ioFlag = bit;
                readRequests.push_back(pal_req);

                // issue write
                new_block->second.write(tmpoffset, RW_LPN[rw_idx][pageIndex], 0, beginAt);
                pal_req.blockIndex = new_block_pdn->second;
                pal_req.pageIndex = tmpoffset;
                if (bRandomTweak) {
                  pal_req.ioFlag.reset();
                  pal_req.ioFlag.set(0);
                }
                else {
                  pal_req.ioFlag.set();
                }
                writeRequests.push_back(pal_req);
              }
              
              // invalidate the page
              RW_LPN[rw_idx][pageIndex] = -2;     // differ from -1: -1 means nothing, -2 means invalidating
              rw_block->second.invalidate(pageIndex, 0);
            }
          }
        }

        // step 3: scan data blocks, copy, switch, erase data blocks
        // switch is done in step 1.

        // 3.1: scan and copy data
        for (auto iter = tmp_block_table.begin(); iter != tmp_block_table.end(); iter++)
        {
          uint64_t tmp_lbn = iter->first;
          uint32_t new_pbn = iter->second;
          auto old_block_pbn = old_block_table.find(tmp_lbn);
          if (old_block_pbn == old_block_table.end())
            panic("No corresponding old data block");
          
          uint32_t old_pbn = old_block_pbn->second;

          auto new_block = blocks.find(new_pbn);
          auto old_block = blocks.find(old_pbn);
          if (new_block == blocks.end())
            panic("Fail to find new data block when merging RW");
          if (old_block == blocks.end())
            panic("Fail to find old data block when merging RW");
          for (uint32_t pageIndex = 0; pageIndex < pagesInBlock; pageIndex++)
          {
            // three cases:
            // 1. valid: copy
            // 2. erased: ignore
            // 3. invalid & unerased & (new)invalid: indicating the data is in SW, invalidate
            if (old_block->second.checkValid(pageIndex, 0))
            {
              if (new_block->second.checkValid(pageIndex, 0))
              {
                panic("collision between old block and new block when RW");
              }

              // issue read
              pal_req.blockIndex = old_block->first;
              pal_req.pageIndex = pageIndex;
              pal_req.ioFlag = bit;
              readRequests.push_back(pal_req);

              // issue write
              new_block->second.write(pageIndex, tmp_lbn * pagesInBlock + pageIndex, 0, beginAt);
              pal_req.blockIndex = new_pbn;
              pal_req.pageIndex = pageIndex;
              if (bRandomTweak) {
                pal_req.ioFlag.reset();
                pal_req.ioFlag.set(0);
              }
              else {
                pal_req.ioFlag.set();
              }
              writeRequests.push_back(pal_req);

              // invalidate the page
              old_block->second.invalidate(pageIndex, 0);
            }
            else if ((!old_block->second.checkValid(pageIndex, 0)) && (!old_block->second.checkErased(pageIndex, 0)) && (!new_block->second.checkValid(pageIndex, 0)))
            {
              new_block->second.fakeWrite(pageIndex, 0);
              new_block->second.invalidate(pageIndex, 0);
            }
          }
        }
        // 3.2: erase data blocks
        for (auto iter = old_block_table.begin(); iter != old_block_table.end(); iter++)
        {
          pal_req.blockIndex = iter->second;
          pal_req.pageIndex = 0;
          pal_req.ioFlag.set();
          eraseRequests.push_back(pal_req);
        }

        // step 4: erase victim RW
        pal_req.blockIndex = RW_PBN[next_evict_rw];
        pal_req.pageIndex = 0;
        pal_req.ioFlag.set();
        eraseRequests.push_back(pal_req);

        // get new RW
        RW_PBN[next_evict_rw] = getFreeBlock();
        next_evict_rw = (next_evict_rw + 1) % 6;
      }
      // write data to RW
      auto rw_block = blocks.find(RW_PBN[next_write_rw]);
      if (rw_block == blocks.end())
        panic("RW block is not in use");
      rw_block->second.write(next_write_offset, req.lpn, 0, beginAt);
      // issue write
      pal_req.blockIndex = RW_PBN[next_write_rw];
      pal_req.pageIndex = 0;

      if (bRandomTweak) {
        pal_req.ioFlag.reset();
        pal_req.ioFlag.set(0);
      }
      else {
        pal_req.ioFlag.set();
      }

      writeRequests.push_back(pal_req);

      // update RW_lpn  
      RW_LPN[next_write_rw][next_write_offset] = req.lpn;
      rw_empty = false;

      // update RW next position
      if (next_write_offset == pagesInBlock - 1)
        next_write_rw = (next_write_rw + 1) % 6;
      next_write_offset = (next_write_offset + 1) % pagesInBlock;

      // invalidate req.lpn corresponding page
      uint64_t req_lbn = getLBN(req.lpn);
      auto block_idx = block_table.find(req_lbn);
      if (block_idx == block_table.end())
        panic("Fail to find data block when invaliding pages after writing to log block");
      auto block = blocks.find(block_idx->second);
      block->second.invalidate(getPageOffset(req.lpn), 0);
    }
  }

  // Do actual I/O here
  // This handles PAL2 limitation (SIGSEGV, infinite loop, or so-on)
  for (auto &iter : readRequests) {
    beginAt = tick;

    pPAL->read(iter, beginAt);

    readFinishedAt = MAX(readFinishedAt, beginAt);
  }

  for (auto &iter : writeRequests) {
    beginAt = readFinishedAt;

    pPAL->write(iter, beginAt);

    writeFinishedAt = MAX(writeFinishedAt, beginAt);
  }

  for (auto &iter : eraseRequests) {
    beginAt = readFinishedAt;

    eraseInternal(iter, beginAt);

    eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  }

  tick = MAX(writeFinishedAt, eraseFinishedAt);
}


void Fast::eraseInternal(PAL::Request &req, uint64_t &tick) {
  static uint64_t threshold =
      conf.readUint(CONFIG_FTL, FTL_BAD_BLOCK_THRESHOLD);
  auto block = blocks.find(req.blockIndex);

  // Sanity checks
  if (block == blocks.end()) {
    panic("No such block");
  }

  if (block->second.getValidPageCount() != 0) {
    panic("There are valid pages in victim block");
  }

  // Erase block
  block->second.erase();

  pPAL->erase(req, tick);

  // Check erase count
  uint32_t erasedCount = block->second.getEraseCount();

  if (erasedCount < threshold) {
    // Reverse search
    auto iter = freeBlocks.end();

    while (true) {
      iter--;

      if (iter->getEraseCount() <= erasedCount) {
        // emplace: insert before pos
        iter++;

        break;
      }

      if (iter == freeBlocks.begin()) {
        break;
      }
    }

    // Insert block to free block list
    freeBlocks.emplace(iter, std::move(block->second));
    nFreeBlocks++;
  }

  // Remove block from block list
  blocks.erase(block);

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::ERASE_INTERNAL);
}


void Fast::trim(Request &, uint64_t &) {}
void Fast::format(LPNRange &, uint64_t &) {}
Status * Fast::getStatus(uint64_t, uint64_t)
{
  return NULL;
}

}

}