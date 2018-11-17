//
// Created by 张艺文 on 2018/11/12.
//
#include <algorithm>
#include <map>
#include <string>

#include "db/column_family.h"
#include "db/flush_job.h"
#include "db/version_set.h"
#include "rocksdb/cache.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/mock_table.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/nvm_write_cache/nvm_cache_options.h"
#include "utilities/nvm_write_cache/fixed_range_based_flush_job.h"
#include "utilities/nvm_write_cache/fixed_range_chunk_based_nvm_write_cache.h"

namespace rocksdb {

// TODO(icanadi) Mock out everything else:
// 1. VersionSet
// 2. Memtable
    class FixedRangeChunkBasedFlushJobTest : public testing::Test {
    public:
        FixedRangeChunkBasedFlushJobTest()
                : env_(Env::Default()),
                  dbname_(test::PerThreadDBPath("nvm_flush_job_test")),
                  options_(),
                  db_options_(options_),
                  column_family_names_({kDefaultColumnFamilyName, "foo", "bar"}),
                  table_cache_(NewLRUCache(50000, 16)),
                  write_buffer_manager_(db_options_.db_write_buffer_size),
                  versions_(new VersionSet(dbname_, &db_options_, env_options_,
                                           table_cache_.get(), &write_buffer_manager_,
                                           &write_controller_)),
                  shutting_down_(false),
                  mock_table_factory_(new mock::MockTableFactory()),
                  nvm_cache_options_(new NVMCacheOptions) {
            EXPECT_OK(env_->CreateDirIfMissing(dbname_));
            db_options_.db_paths.emplace_back(dbname_,
                                              std::numeric_limits<uint64_t>::max());
            db_options_.statistics = rocksdb::CreateDBStatistics();

            auto fixed_range_based_options = new FixedRangeBasedOptions(
                    16, 3,
                    new SimplePrefixExtractor(3),
                    NewBloomFilterPolicy(16, false),
                    0, 64ul<<20);

            nvm_cache_options_->use_nvm_write_cache_ = true;
            nvm_cache_options_->nvm_cache_type_ = kRangeFixedChunk;
            nvm_cache_options_->drain_strategy_ = kNoDrain;
            nvm_cache_options_->pmem_info_.pmem_path_ = "/pmem/NVMWriteCache";
            nvm_cache_options_->pmem_info_.pmem_size_ = static_cast<uint64_t>(4) * 1024 * 1024 * 1024;
            nvm_cache_options_->nvm_write_cache_ = new FixedRangeChunkBasedNVMWriteCache(fixed_range_based_options);

            db_options_.nvm_cache_options = nvm_cache_options_;

            // TODO(icanadi) Remove this once we mock out VersionSet
            NewDB();
            std::vector<ColumnFamilyDescriptor> column_families;
            cf_options_.table_factory = mock_table_factory_;
            for (const auto &cf_name : column_family_names_) {
                column_families.emplace_back(cf_name, cf_options_);
            }

            EXPECT_OK(versions_->Recover(column_families, false));
        }

        void NewDB() {
            VersionEdit new_db;
            new_db.SetLogNumber(0);
            new_db.SetNextFile(2);
            new_db.SetLastSequence(0);

            autovector<VersionEdit> new_cfs;
            SequenceNumber last_seq = 1;
            uint32_t cf_id = 1;
            for (size_t i = 1; i != column_family_names_.size(); ++i) {
                VersionEdit new_cf;
                new_cf.AddColumnFamily(column_family_names_[i]);
                new_cf.SetColumnFamily(cf_id++);
                new_cf.SetLogNumber(0);
                new_cf.SetNextFile(2);
                new_cf.SetLastSequence(last_seq++);
                new_cfs.emplace_back(new_cf);
            }

            const std::string manifest = DescriptorFileName(dbname_, 1);
            unique_ptr<WritableFile> file;
            Status s = env_->NewWritableFile(
                    manifest, &file, env_->OptimizeForManifestWrite(env_options_));
            ASSERT_OK(s);
            unique_ptr<WritableFileWriter> file_writer(
                    new WritableFileWriter(std::move(file), manifest, EnvOptions()));
            {
                log::Writer log(std::move(file_writer), 0, false);
                std::string record;
                new_db.EncodeTo(&record);
                s = log.AddRecord(record);

                for (const auto &e : new_cfs) {
                    record.clear();
                    e.EncodeTo(&record);
                    s = log.AddRecord(record);
                    ASSERT_OK(s);
                }
            }
            ASSERT_OK(s);
            // Make "CURRENT" file that points to the new manifest file.
            s = SetCurrentFile(env_, dbname_, 1, nullptr);
        }

        Env *env_;
        std::string dbname_;
        EnvOptions env_options_;
        Options options_;
        ImmutableDBOptions db_options_;
        const std::vector<std::string> column_family_names_;
        std::shared_ptr<Cache> table_cache_;
        WriteController write_controller_;
        WriteBufferManager write_buffer_manager_;
        ColumnFamilyOptions cf_options_;
        std::unique_ptr<VersionSet> versions_;
        InstrumentedMutex mutex_;
        std::atomic<bool> shutting_down_;
        std::shared_ptr<mock::MockTableFactory> mock_table_factory_;
        std::shared_ptr<NVMCacheOptions> nvm_cache_options_;
    };

    TEST_F(FixedRangeChunkBasedFlushJobTest, Empty) {
        JobContext job_context(0);
        auto cfd = versions_->GetColumnFamilySet()->GetDefault();
        EventLogger event_logger(db_options_.info_log.get());
        SnapshotChecker *snapshot_checker = nullptr;  // not relavant
        FixedRangeBasedFlushJob nvm_flush_job(
                dbname_,
                db_options_,
                &job_context,
                &event_logger,
                cfd,
                {},
                kMaxSequenceNumber,
                snapshot_checker,
                &mutex_,
                &shutting_down_,
                nullptr,
                db_options_.nvm_cache_options.get()
        );
        {
            InstrumentedMutexLock l(&mutex_);
            nvm_flush_job.Prepare();
            ASSERT_OK(nvm_flush_job.Run());
        }
        job_context.Clean();
    }

    TEST_F(FixedRangeChunkBasedFlushJobTest, NonEmpty) {
        JobContext job_context(0);
        auto cfd = versions_->GetColumnFamilySet()->GetDefault();
        auto new_mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                                 kMaxSequenceNumber);
        new_mem->Ref();
        auto inserted_keys = mock::MakeMockFile();
        // Test data:
        //   seqno [    1,    2 ... 8998, 8999, 9000, 9001, 9002 ... 9999 ]
        //   key   [ 1001, 1002 ... 9998, 9999,    0,    1,    2 ...  999 ]
        //   range-delete "9995" -> "9999" at seqno 10000
        for (int i = 1; i < 10000; ++i) {
            std::string key(ToString((i + 1000) % 10000));
            std::string value("value" + key);
            new_mem->Add(SequenceNumber(i), kTypeValue, key, value);
            if ((i + 1000) % 10000 < 9995) {
                InternalKey internal_key(key, SequenceNumber(i), kTypeValue);
                inserted_keys.insert({internal_key.Encode().ToString(), value});
            }
        }
        new_mem->Add(SequenceNumber(10000), kTypeRangeDeletion, "9995", "9999a");
        InternalKey internal_key("9995", SequenceNumber(10000), kTypeRangeDeletion);
        inserted_keys.insert({internal_key.Encode().ToString(), "9999a"});

        autovector<MemTable *> to_delete;
        cfd->imm()->Add(new_mem, &to_delete);
        for (auto &m : to_delete) {
            delete m;
        }

        EventLogger event_logger(db_options_.info_log.get());
        SnapshotChecker *snapshot_checker = nullptr;  // not relavant

        FixedRangeBasedFlushJob nvm_flush_job(
                dbname_,
                db_options_,
                &job_context,
                &event_logger,
                cfd,
                {},
                kMaxSequenceNumber,
                snapshot_checker,
                &mutex_,
                &shutting_down_,
                nullptr,
                db_options_.nvm_cache_options.get()
        );

        HistogramData hist;
        //FileMetaData file_meta;
        mutex_.Lock();
        nvm_flush_job.Prepare();
        ASSERT_OK(nvm_flush_job.Run());
        mutex_.Unlock();
        //db_options_.statistics->histogramData(FLUSH_TIME, &hist);
        //ASSERT_GT(hist.average, 0.0);

        //ASSERT_EQ(ToString(0), file_meta.smallest.user_key().ToString());
        //ASSERT_EQ(
        //        "9999a",
        //        file_meta.largest.user_key().ToString());  // range tombstone end key
        //ASSERT_EQ(1, file_meta.fd.smallest_seqno);
        //ASSERT_EQ(10000, file_meta.fd.largest_seqno);  // range tombstone seqnum 10000
        //mock_table_factory_->AssertSingleFile(inserted_keys);
        job_context.Clean();
    }

    TEST_F(FixedRangeChunkBasedFlushJobTest, FlushMemTablesSingleColumnFamily) {
        const size_t num_mems = 2;
        const size_t num_mems_to_flush = 1;
        const size_t num_keys_per_table = 100;
        JobContext job_context(0);
        ColumnFamilyData *cfd = versions_->GetColumnFamilySet()->GetDefault();
        std::vector<uint64_t> memtable_ids;
        std::vector<MemTable *> new_mems;
        for (size_t i = 0; i != num_mems; ++i) {
            MemTable *mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                                      kMaxSequenceNumber);
            mem->SetID(i);
            mem->Ref();
            new_mems.emplace_back(mem);
            memtable_ids.push_back(mem->GetID());

            for (size_t j = 0; j < num_keys_per_table; ++j) {
                std::string key(ToString(j + i * num_keys_per_table));
                std::string value("value" + key);
                mem->Add(SequenceNumber(j + i * num_keys_per_table), kTypeValue, key,
                         value);
            }
        }

        autovector<MemTable *> to_delete;
        for (auto mem : new_mems) {
            cfd->imm()->Add(mem, &to_delete);
        }

        EventLogger event_logger(db_options_.info_log.get());
        SnapshotChecker *snapshot_checker = nullptr;  // not relavant

        assert(memtable_ids.size() == num_mems);
        //uint64_t smallest_memtable_id = memtable_ids.front();
        //uint64_t flush_memtable_id = smallest_memtable_id + num_mems_to_flush - 1;

        FixedRangeBasedFlushJob nvm_flush_job(
                dbname_,
                db_options_,
                &job_context,
                &event_logger,
                cfd,
                {},
                kMaxSequenceNumber,
                snapshot_checker,
                &mutex_,
                &shutting_down_,
                nullptr,
                db_options_.nvm_cache_options.get()
        );
        HistogramData hist;
        //FileMetaData file_meta;
        mutex_.Lock();
        nvm_flush_job.Prepare();
        ASSERT_OK(nvm_flush_job.Run());
        mutex_.Unlock();
        //db_options_.statistics->histogramData(FLUSH_TIME, &hist);
        //ASSERT_GT(hist.average, 0.0);

        /*ASSERT_EQ(ToString(0), file_meta.smallest.user_key().ToString());
        ASSERT_EQ("99", file_meta.largest.user_key().ToString());
        ASSERT_EQ(0, file_meta.fd.smallest_seqno);
        ASSERT_EQ(SequenceNumber(num_mems_to_flush * num_keys_per_table - 1),
                  file_meta.fd.largest_seqno);*/

        for (auto m : to_delete) {
            delete m;
        }
        to_delete.clear();
        job_context.Clean();
    }

    /*TEST_F(FixedRangeChunkBasedFlushJobTest, FlushMemtablesMultipleColumnFamilies) {
        autovector<ColumnFamilyData *> all_cfds;
        for (auto cfd : *versions_->GetColumnFamilySet()) {
            all_cfds.push_back(cfd);
        }
        const std::vector<size_t> num_memtables = {2, 1, 3};
        assert(num_memtables.size() == column_family_names_.size());
        const size_t num_keys_per_memtable = 1000;
        JobContext job_context(0);
        std::vector<uint64_t> memtable_ids;
        std::vector<SequenceNumber> smallest_seqs;
        std::vector<SequenceNumber> largest_seqs;
        autovector<MemTable *> to_delete;
        SequenceNumber curr_seqno = 0;
        size_t k = 0;
        for (auto cfd : all_cfds) {
            smallest_seqs.push_back(curr_seqno);
            for (size_t i = 0; i != num_memtables[k]; ++i) {
                MemTable *mem = cfd->ConstructNewMemtable(
                        *cfd->GetLatestMutableCFOptions(), kMaxSequenceNumber);
                mem->SetID(i);
                mem->Ref();
                mem->TEST_AtomicFlushSequenceNumber() = 123;

                for (size_t j = 0; j != num_keys_per_memtable; ++j) {
                    std::string key(ToString(j + i * num_keys_per_memtable));
                    std::string value("value" + key);
                    mem->Add(curr_seqno++, kTypeValue, key, value);
                }

                cfd->imm()->Add(mem, &to_delete);
            }
            largest_seqs.push_back(curr_seqno - 1);
            memtable_ids.push_back(num_memtables[k++] - 1);
        }

        EventLogger event_logger(db_options_.info_log.get());
        SnapshotChecker *snapshot_checker = nullptr;  // not relevant
        std::vector<FlushJob> flush_jobs;
        k = 0;
        for (auto cfd : all_cfds) {
            std::vector<SequenceNumber> snapshot_seqs;
            flush_jobs.emplace_back(
                    dbname_, cfd, db_options_, *cfd->GetLatestMutableCFOptions(),
                    &memtable_ids[k], env_options_, versions_.get(), &mutex_,
                    &shutting_down_, snapshot_seqs, kMaxSequenceNumber, snapshot_checker,
                    &job_context, nullptr, nullptr, nullptr, kNoCompression,
                    db_options_.statistics.get(), &event_logger, true,
                    false *//* sync_output_directory *//*, false *//* write_manifest *//*);
            k++;
        }
        HistogramData hist;
        autovector<FileMetaData> file_metas;
        mutex_.Lock();
        for (auto &job : flush_jobs) {
            job.PickMemTable();
        }
        for (auto &job : flush_jobs) {
            FileMetaData meta;
// Run will release and re-acquire  mutex
            ASSERT_OK(job.Run(nullptr *//**//*, &meta));
            file_metas.emplace_back(meta);
        }
        autovector<const autovector<MemTable *> *> mems_list;
        for (size_t i = 0; i != all_cfds.size(); ++i) {
            const auto &mems = flush_jobs[i].GetMemTables();
            mems_list.push_back(&mems);
        }
        autovector<MemTableList *> imm_lists;
        autovector<const MutableCFOptions *> mutable_cf_options_list;
        for (auto cfd : all_cfds) {
            imm_lists.push_back(cfd->imm());
            mutable_cf_options_list.push_back(cfd->GetLatestMutableCFOptions());
        }

        bool atomic_flush_commit_in_progress = false;
        Status s = MemTableList::TryInstallMemtableFlushResults(
                imm_lists, all_cfds, mutable_cf_options_list, mems_list,
                &atomic_flush_commit_in_progress, nullptr *//* logs_prep_tracker *//*,
                versions_.get(), &mutex_, file_metas, &job_context.memtables_to_free,
                nullptr *//* db_directory *//*, nullptr *//* log_buffer *//*);
        ASSERT_OK(s);

        mutex_.Unlock();
        db_options_.statistics->histogramData(FLUSH_TIME, &hist);
        ASSERT_GT(hist.average, 0.0);
        k = 0;
        for (const auto &file_meta : file_metas) {
            ASSERT_EQ(ToString(0), file_meta.smallest.user_key().ToString());
            ASSERT_EQ("999", file_meta.largest.user_key()
                    .ToString());  // max key by bytewise comparator
            ASSERT_EQ(smallest_seqs[k], file_meta.fd.smallest_seqno);
            ASSERT_EQ(largest_seqs[k], file_meta.fd.largest_seqno);
// Verify that imm is empty
            ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
                      all_cfds[k]->imm()->GetEarliestMemTableID());
            ASSERT_EQ(0, all_cfds[k]->imm()->GetLatestMemTableID());
            ++k;
        }

        for (auto m : to_delete) {
            delete m;
        }
        to_delete.clear();
        job_context.Clean();
    }*/

    /*TEST_F(FixedRangeChunkBasedFlushJobTest, Snapshots) {
        JobContext job_context(0);
        auto cfd = versions_->GetColumnFamilySet()->GetDefault();
        auto new_mem = cfd->ConstructNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                                 kMaxSequenceNumber);

        std::vector<SequenceNumber> snapshots;
        std::set<SequenceNumber> snapshots_set;
        int keys = 10000;
        int max_inserts_per_keys = 8;

        Random rnd(301);
        for (int i = 0; i < keys / 2; ++i) {
            snapshots.push_back(rnd.Uniform(keys * (max_inserts_per_keys / 2)) + 1);
            snapshots_set.insert(snapshots.back());
        }
        std::sort(snapshots.begin(), snapshots.end());

        new_mem->Ref();
        SequenceNumber current_seqno = 0;
        auto inserted_keys = mock::MakeMockFile();
        for (int i = 1; i < keys; ++i) {
            std::string key(ToString(i));
            int insertions = rnd.Uniform(max_inserts_per_keys);
            for (int j = 0; j < insertions; ++j) {
                std::string value(test::RandomHumanReadableString(&rnd, 10));
                auto seqno = ++current_seqno;
                new_mem->Add(SequenceNumber(seqno), kTypeValue, key, value);
                // a key is visible only if:
                // 1. it's the last one written (j == insertions - 1)
                // 2. there's a snapshot pointing at it
                bool visible = (j == insertions - 1) ||
                               (snapshots_set.find(seqno) != snapshots_set.end());
                if (visible) {
                    InternalKey internal_key(key, seqno, kTypeValue);
                    inserted_keys.insert({internal_key.Encode().ToString(), value});
                }
            }
        }

        autovector<MemTable *> to_delete;
        cfd->imm()->Add(new_mem, &to_delete);
        for (auto &m : to_delete) {
            delete m;
        }

        EventLogger event_logger(db_options_.info_log.get());
        SnapshotChecker *snapshot_checker = nullptr;  // not relavant
        FlushJob flush_job(
                dbname_, versions_->GetColumnFamilySet()->GetDefault(), db_options_,
                *cfd->GetLatestMutableCFOptions(), nullptr *//* memtable_id *//*,
                env_options_, versions_.get(), &mutex_, &shutting_down_, snapshots,
                kMaxSequenceNumber, snapshot_checker, &job_context, nullptr, nullptr,
                nullptr, kNoCompression, db_options_.statistics.get(), &event_logger,
                true, true *//* sync_output_directory *//*, true *//* write_manifest *//*);
        mutex_.Lock();
        flush_job.PickMemTable();
        ASSERT_OK(flush_job.Run());
        mutex_.Unlock();
        mock_table_factory_->AssertSingleFile(inserted_keys);
        HistogramData hist;
        db_options_.statistics->histogramData(FLUSH_TIME, &hist);
        ASSERT_GT(hist.average, 0.0);
        job_context.Clean();
    }*/

}  // namespace rocksdb

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


