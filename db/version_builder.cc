//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"

namespace rocksdb {

bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->largest_seqno != b->largest_seqno) {
    return a->largest_seqno > b->largest_seqno;
  }
  if (a->smallest_seqno != b->smallest_seqno) {
    return a->smallest_seqno > b->smallest_seqno;
  }
  // Break ties by file number
  return a->fd.GetNumber() > b->fd.GetNumber();
}

namespace {
bool BySmallestKey(FileMetaData* a, FileMetaData* b,
                   const InternalKeyComparator* cmp) {
  int r = cmp->Compare(a->smallest, b->smallest);		/* a < b，则返回1 */
  if (r != 0) {
    return (r < 0);
  }
  // Break ties by file number
  return (a->fd.GetNumber() < b->fd.GetNumber());
}
}  // namespace

class VersionBuilder::Rep {
 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod { kLevel0 = 0, kLevelNon0 = 1, } sort_method;
    const InternalKeyComparator* internal_comparator;

    FileComparator() : internal_comparator(nullptr) {}

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      switch (sort_method) {
        case kLevel0:
          return NewestFirstBySeqNo(f1, f2);
        case kLevelNon0:
          return BySmallestKey(f1, f2, internal_comparator);
      }
      assert(false);
      return false;
    }
  };

  struct LevelState {
    std::unordered_set<uint64_t> deleted_files;			/* 存储的是将要deleted的sst文件的file_number */
    // Map from file number to file meta data.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
  };

  const EnvOptions& env_options_;
  Logger* info_log_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  int num_levels_;
  LevelState* levels_;
  // Store states of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::map<int, std::unordered_set<uint64_t>> invalid_levels_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;

 public:
  Rep(const EnvOptions& env_options, Logger* info_log, TableCache* table_cache,
      VersionStorageInfo* base_vstorage)
      : env_options_(env_options),
        info_log_(info_log),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        num_levels_(base_vstorage->num_levels()),
        has_invalid_levels_(false) {
    levels_ = new LevelState[num_levels_];
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
    level_nonzero_cmp_.internal_comparator =
        base_vstorage_->InternalComparator();
  }

  ~Rep() {
    for (int level = 0; level < num_levels_; level++) {
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        UnrefFile(pair.second);
      }
    }

    delete[] levels_;
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        table_cache_->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
      delete f;
    }
  }

  /*
   * 这里的一致性理解为：vstorage存储的每层sst文件,
   * level-0: 新添加的应该在最前面，后添加的应该在靠后面；通过比较largest_seq_no, smallest_seq_no或file_number
   * level-1~n: 每层level内相邻sst文件，前者smallest_key或者file_number要不大于后者的，而且相邻sst文件没有key-range重叠的
   */
  void CheckConsistency(VersionStorageInfo* vstorage) {
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // make sure the files are sorted correctly
    // added by ChengZhilong
    for (int level = 0; level < num_levels_; level++) {
//    for (int level = 0; level < num_levels_; level++) {
      auto& level_files = vstorage->LevelFiles(level);
      for (size_t i = 1; i < level_files.size(); i++) {
        auto f1 = level_files[i - 1];
        auto f2 = level_files[i];
        if (level == 0) {
          if (!level_zero_cmp_(f1, f2)) {			/* Level-0的sst文件，按照newest seq_no从前往后排序，最新的靠前 */
            fprintf(stderr, "L0 files are not sorted properly");	/* 如果f1的newest seq_no < f2的,则说明sst文件是乱序的 */
            abort();
          }

          if (f2->smallest_seqno == f2->largest_seqno) {	/* 则f2对应的sst文件是通过调用API添加到db的 */
            // This is an external file that we ingested
            SequenceNumber external_file_seqno = f2->smallest_seqno;
            if (!(external_file_seqno < f1->largest_seqno ||
                  external_file_seqno == 0)) {
              fprintf(stderr, "L0 file with seqno %" PRIu64 " %" PRIu64
                              " vs. file with global_seqno %" PRIu64 "\n",
                      f1->smallest_seqno, f1->largest_seqno,
                      external_file_seqno);
              abort();
            }
          } else if (f1->smallest_seqno <= f2->smallest_seqno) {
            fprintf(stderr, "L0 files seqno %" PRIu64 " %" PRIu64
                            " vs. %" PRIu64 " %" PRIu64 "\n",
                    f1->smallest_seqno, f1->largest_seqno, f2->smallest_seqno,
                    f2->largest_seqno);
            abort();
          }
        } else {
          if (!level_nonzero_cmp_(f1, f2)) {
            fprintf(stderr, "L%d files are not sorted properly", level);
            abort();
          }

          // Make sure there is no overlap in levels > 0
          if (vstorage->InternalComparator()->Compare(f1->largest,
                                                      f2->smallest) >= 0) {	/* 返回>=0，则f1->largest > f2->smallest */
            fprintf(stderr, "L%d have overlapping ranges %s vs. %s\n", level,
                    (f1->largest).DebugString(true).c_str(),
                    (f2->smallest).DebugString(true).c_str());
            abort();
          }
        }
      }
    }
  }

  void CheckConsistencyForDeletes(VersionEdit* /*edit*/, uint64_t number,
                                  int level) {
#ifdef NDEBUG
    if (!base_vstorage_->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // a file to be deleted better exist in the previous version
    /* 首先，该file很可能会在前一个version中存在，也就是base_vstorage_->files_[]中可能会有 */
    bool found = false;
    for (int l = 0; !found && l < num_levels_; l++) {
      const std::vector<FileMetaData*>& base_files =
          base_vstorage_->LevelFiles(l);
      for (size_t i = 0; i < base_files.size(); i++) {
        FileMetaData* f = base_files[i];
        if (f->fd.GetNumber() == number) {
          found = true;
          break;
        }
      }
    }
    // if the file did not exist in the previous version, then it
    // is possibly moved from lower level to higher level in current
    // version
    /* 或者该file可能在compact之后直接被迁移到更高level(因此file_number不变) */
    for (int l = level + 1; !found && l < num_levels_; l++) {
      auto& level_added = levels_[l].added_files;
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
        break;
      }
    }

    // maybe this file was added in a previous edit that was Applied
    /* 或者该file可能在之前的VersionEdit中被apply */
    if (!found) {
      auto& level_added = levels_[level].added_files;
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
      }
    }
    if (!found) {
      fprintf(stderr, "not found %" PRIu64 "\n", number);
      abort();
    }
  }

  bool CheckConsistencyForNumLevels() {
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }
    for (auto& level : invalid_levels_) {
      if (level.second.size() > 0) {
        return false;
      }
    }
    return true;
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    CheckConsistency(base_vstorage_);

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->GetDeletedFiles();
    for (const auto& del_file : del) {							/* 对于每个待deleted的sst文件，获取其所在level、以及file_number */
      const auto level = del_file.first;
      const auto number = del_file.second;
      if (level < num_levels_) {								/* 主要是将要被deleted的file添加到levels_[].deleted_files集合内 */
        levels_[level].deleted_files.insert(number);
        CheckConsistencyForDeletes(edit, number, level);		/* 检查该file是否真实存在(删除之前必须保证其存在) */

        auto exising = levels_[level].added_files.find(number);	/* 若该file是之前待added的，则直接从added_files集合中删除该file(后者优先级更高) */
        if (exising != levels_[level].added_files.end()) {
          UnrefFile(exising->second);
          levels_[level].added_files.erase(exising);
        }
      } else {													/* 若所属level超出num_levels_，则为invalid，记录在invalid_levels[]集合中 */
        auto exising = invalid_levels_[level].find(number);
        if (exising != invalid_levels_[level].end()) {
          invalid_levels_[level].erase(exising);
        } else {
          // Deleting an non-existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }

    // Add new files
    for (const auto& new_file : edit->GetNewFiles()) {			/* 对新添加file的操作过程同上 */
      const int level = new_file.first;
      if (level < num_levels_) {
        FileMetaData* f = new FileMetaData(new_file.second);
        f->refs = 1;

        assert(levels_[level].added_files.find(f->fd.GetNumber()) ==
               levels_[level].added_files.end());
        levels_[level].deleted_files.erase(f->fd.GetNumber());	/* "后来者优先级更高"，原先若记录着要被deleted，则从deleted_files中移除掉 */
        levels_[level].added_files[f->fd.GetNumber()] = f;
      } else {
        uint64_t number = new_file.second.fd.GetNumber();
        if (invalid_levels_[level].count(number) == 0) {
          invalid_levels_[level].insert(number);
        } else {
          // Creating an already existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }
  }

  // Save the current state in *v.
  void SaveTo(VersionStorageInfo* vstorage) {
    CheckConsistency(base_vstorage_);
    CheckConsistency(vstorage);

    // added by ChengZhilong
    // for (int level = 0; ...) -> for (int level = 1; ...)
    for (int level = 1; level < num_levels_; level++) {								/* 对每层都会进行排序 */
//    for (int level = 0; level < num_levels_; level++) {
      const auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;		/* 获取比较器，便于排序每层level的sst正确位置 */
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const auto& base_files = base_vstorage_->LevelFiles(level);
      auto base_iter = base_files.begin();
      auto base_end = base_files.end();
      const auto& unordered_added_files = levels_[level].added_files;
      vstorage->Reserve(level,
                        base_files.size() + unordered_added_files.size());			/* 调整files_容器的大小 */

      // Sort added files for the level.
      std::vector<FileMetaData*> added_files;
      added_files.reserve(unordered_added_files.size());
      for (const auto& pair : unordered_added_files) {
        added_files.push_back(pair.second);											/* 将所有unordered_added_files添加到added_files里 */
      }
      std::sort(added_files.begin(), added_files.end(), cmp);						/* 调用sort排序，基于比较器cmp */

#ifndef NDEBUG
      FileMetaData* prev_file = nullptr;
#endif

	  /* 这里将所有base_iter ~ base_end和added_files[]内的所有sst
	   * 元数据信息都按序添加到vstorage里
       */
      for (const auto& added : added_files) {
#ifndef NDEBUG
        if (level > 0 && prev_file != nullptr) {
          assert(base_vstorage_->InternalComparator()->Compare(
                     prev_file->smallest, added->smallest) <= 0);
        }
        prev_file = added;
#endif

        // Add all smaller files listed in base_
        for (auto bpos = std::upper_bound(base_iter, base_end, added, cmp);			/* 返回大于added对应值的第一个元素位置上的迭代器 */
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(vstorage, level, *base_iter);								/* 将*base_iter对应的fmd追加到vstorage指定的level上 */
        }

        MaybeAddFile(vstorage, level, added);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(vstorage, level, *base_iter);
      }
    }

    CheckConsistency(vstorage);		/* 末尾做CheckConsistency */
  }

  void LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                         bool prefetch_index_and_filter_in_cache,
                         const SliceTransform* prefix_extractor) {
    assert(table_cache_ != nullptr);
    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level].added_files) {
        auto* file_meta = file_meta_pair.second;
        assert(!file_meta->table_reader_handle);
        files_meta.emplace_back(file_meta, level);
      }
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> load_handlers_func = [&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx].first;
        int level = files_meta[file_idx].second;
        table_cache_->FindTable(
            env_options_, *(base_vstorage_->InternalComparator()),
            file_meta->fd, &file_meta->table_reader_handle, prefix_extractor,
            false /*no_io */, true /* record_read_stats */,
            internal_stats->GetFileReadHist(level), false, level,
            prefetch_index_and_filter_in_cache);
        if (file_meta->table_reader_handle != nullptr) {
          // Load table_reader
          file_meta->fd.table_reader = table_cache_->GetTableReaderFromHandle(
              file_meta->table_reader_handle);
        }
      }
    };

    std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    load_handlers_func();
    for (auto& t : threads) {
      t.join();
    }
  }

  void MaybeAddFile(VersionStorageInfo* vstorage, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->fd.GetNumber()) > 0) {			/* 若f标记的sst文件是准备要被删除掉的 */
      // f is to-be-deleted table file
      vstorage->RemoveCurrentStats(f);
    } else {
      vstorage->AddFile(level, f, info_log_);									/* 否则，将f追加到vstorage内的level层内 */
    }
  }
};

VersionBuilder::VersionBuilder(const EnvOptions& env_options,
                               TableCache* table_cache,
                               VersionStorageInfo* base_vstorage,
                               Logger* info_log)
    : rep_(new Rep(env_options, info_log, table_cache, base_vstorage)) {}

VersionBuilder::~VersionBuilder() { delete rep_; }

void VersionBuilder::CheckConsistency(VersionStorageInfo* vstorage) {
  rep_->CheckConsistency(vstorage);
}

void VersionBuilder::CheckConsistencyForDeletes(VersionEdit* edit,
                                                uint64_t number, int level) {
  rep_->CheckConsistencyForDeletes(edit, number, level);
}

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

void VersionBuilder::Apply(VersionEdit* edit) { rep_->Apply(edit); }

void VersionBuilder::SaveTo(VersionStorageInfo* vstorage) {
  rep_->SaveTo(vstorage);
}

void VersionBuilder::LoadTableHandlers(InternalStats* internal_stats,
                                       int max_threads,
                                       bool prefetch_index_and_filter_in_cache,
                                       const SliceTransform* prefix_extractor) {
  rep_->LoadTableHandlers(internal_stats, max_threads,
                          prefetch_index_and_filter_in_cache, prefix_extractor);
}

void VersionBuilder::MaybeAddFile(VersionStorageInfo* vstorage, int level,
                                  FileMetaData* f) {
  rep_->MaybeAddFile(vstorage, level, f);
}

}  // namespace rocksdb
