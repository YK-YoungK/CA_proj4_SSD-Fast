/*
 * Copyright (C) 2017 CAMELab
 *
 * This file is part of SimpleSSD.
 *
 * SimpleSSD is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SimpleSSD is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SimpleSSD.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __FTL_FAST_MAPPING__
#define __FTL_FAST_MAPPING__

#include <cinttypes>
#include <unordered_map>
#include <vector>

#include "ftl/abstract_ftl.hh"
#include "ftl/common/block.hh"
#include "ftl/ftl.hh"
#include "pal/pal.hh"

namespace SimpleSSD {

namespace FTL {

class Fast : public AbstractFTL {
 private:
  PAL::PAL *pPAL;

  ConfigReader &conf;

  std::unordered_map<uint64_t, uint32_t> block_table;   
  // Mapping from LBN to PBN
  uint64_t pagesInBlock;
  uint64_t pageSize;

  // SW
  uint64_t SW_LBN;              // SW LBN
  uint32_t SW_PBN;              // SW PBN
  uint64_t SW_last_LPN;         // SW last time LPN

  // RW
  uint32_t RW_PBN[6];
  std::vector<uint64_t> RW_LPN[6];
  uint32_t next_evict_rw;  // next evict RW, round evict
  uint32_t next_write_rw, next_write_offset; // next write RW and offset, round write
  bool rw_empty;

  // maintained by eraseInternal & getFreeblock
  std::unordered_map<uint32_t, Block> blocks;
  std::list<Block> freeBlocks;
  uint32_t nFreeBlocks;  // For some libraries which std::list::size() is O(n)

  bool bReclaimMore;
  bool bRandomTweak;
  uint32_t bitsetSize;

  struct {
    uint64_t gcCount;
    uint64_t reclaimedBlocks;
    uint64_t validSuperPageCopies;
    uint64_t validPageCopies;
  } stat;


  uint32_t getFreeBlock();
  void doGarbageCollection(std::vector<uint32_t> &, uint64_t &);

  // float calculateWearLeveling();
  void calculateTotalPages(uint64_t &, uint64_t &);

  void readInternal(Request &, uint64_t &);
  void writeInternal(Request &, uint64_t &, bool = true);
  void actualWrite(Request &, uint64_t &, bool, std::unordered_map<uint32_t, Block>::iterator, uint32_t);
  void write2LogBlock(Request &, uint64_t &, bool);
  void eraseInternal(PAL::Request &, uint64_t &);

  uint64_t getLBN(uint64_t);
  uint64_t getPageOffset(uint64_t);

 public:
  Fast(ConfigReader &, Parameter &, PAL::PAL *, DRAM::AbstractDRAM *);
  ~Fast();

  bool initialize() override;

  void read(Request &, uint64_t &) override;
  void write(Request &, uint64_t &) override;
  void trim(Request &, uint64_t &) override;

  void format(LPNRange &, uint64_t &) override;

  Status *getStatus(uint64_t, uint64_t) override;
};

}  // namespace FTL

}  // namespace SimpleSSD

#endif
