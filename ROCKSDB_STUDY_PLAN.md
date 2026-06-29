# RocksDB v8.11.5 Study Plan

这份计划默认你已经理解 LevelDB 的主干：

```text
WAL -> MemTable -> immutable MemTable -> Flush -> L0 SST
Version / VersionSet / VersionEdit / MANIFEST
Get / Iterator
Level compaction
BlockBasedTable 基础格式
```

所以这里不再按 LevelDB 的路径重复读一遍 RocksDB，而是重点看 RocksDB 在 LevelDB 之上的工程化扩展：

```text
Column Family
SuperVersion
并发写入 / pipelined write
并发 flush / compaction 调度
更复杂的 compaction picker 和 compaction job
事务系统
Merge / DeleteRange / Wide Column
External SST ingest
BlobDB / integrated blob
Block cache / table reader / filter / table properties
Options / observability / tuning
```

第一轮目标不是把 RocksDB 所有功能看完，而是建立一个清晰的差异地图：

```text
LevelDB 是教学版 LSM。
RocksDB 是生产版 LSM storage engine。
```

---

## 0. 总体源码地图

先记住这些模块位置。

| 主题 | 重点文件 | 重点对象 |
| --- | --- | --- |
| DB 主对象 | `db/db_impl/db_impl.h` | `DBImpl` |
| DB 打开/恢复 | `db/db_impl/db_impl_open.cc` | `DB::Open`, `DBImpl::Open`, `DBImpl::Recover` |
| 写路径 | `db/db_impl/db_impl_write.cc` | `DBImpl::WriteImpl`, `PreprocessWrite` |
| 写线程队列 | `db/write_thread.h`, `db/write_thread.cc` | `WriteThread`, `Writer` |
| Column Family | `db/column_family.h`, `db/column_family.cc` | `ColumnFamilyData`, `ColumnFamilySet` |
| SuperVersion | `db/column_family.h` | `SuperVersion` |
| MemTable | `db/memtable.h`, `db/memtable.cc` | `MemTable` |
| Immutable MemTable | `db/memtable_list.h`, `db/memtable_list.cc` | `MemTableList`, `MemTableListVersion` |
| Version 元数据 | `db/version_set.h`, `db/version_set.cc` | `VersionSet`, `Version`, `VersionStorageInfo` |
| MANIFEST edit | `db/version_edit.h`, `db/version_edit.cc` | `VersionEdit`, `FileMetaData` |
| Flush | `db/flush_job.cc` | `FlushJob` |
| Compaction picker | `db/compaction/` | `CompactionPicker`, `LevelCompactionPicker` |
| Compaction 执行 | `db/compaction/compaction_job.cc` | `CompactionJob` |
| Compaction 迭代 | `db/compaction/compaction_iterator.cc` | `CompactionIterator` |
| Block table | `table/block_based/` | `BlockBasedTable`, `BlockBasedTableBuilder` |
| Table cache | `db/table_cache.h`, `db/table_cache.cc` | `TableCache` |
| WAL | `db/log_writer.cc`, `db/log_reader.cc`, `db/wal_manager.cc` | `log::Writer`, `log::Reader`, `WalManager` |
| Transaction | `utilities/transactions/` | `TransactionDB`, `PessimisticTransaction`, `OptimisticTransaction` |
| Blob | `db/blob/`, `utilities/blob_db/` | `BlobFileBuilder`, `BlobSource`, `BlobDB` |
| External SST | `db/external_sst_file_ingestion_job.cc` | `ExternalSstFileIngestionJob` |
| Options | `include/rocksdb/options.h`, `options/` | `DBOptions`, `ColumnFamilyOptions`, `MutableDBOptions` |

建议先只看这些主干，不要第一轮就看 Java/C API、backup、TTL、checkpoint、secondary DB、trace replay、remote compaction。

---

## 1. 先建立 LevelDB -> RocksDB 差异地图

这一节只读接口和对象，不深挖流程。

### LevelDB 对应关系

| LevelDB | RocksDB |
| --- | --- |
| `DBImpl` 单 CF | `DBImpl` + 多个 `ColumnFamilyData` |
| `VersionSet` 管一个 current version | `VersionSet` 管多个 CF 的 versions |
| `MemTable* mem_` | 每个 CF 一个 mutable memtable |
| `MemTable* imm_` | `MemTableList`，可有多个 immutable memtable |
| 单线程后台 compaction | flush / compaction 多线程调度 |
| 简单 write queue | `WriteThread`、batch group、pipelined write |
| leveled compaction | leveled / universal / FIFO / subcompaction |
| MANIFEST 只记录文件 | MANIFEST 还记录 CF create/drop、更多 metadata |
| 无事务层 | TransactionDB / OptimisticTransactionDB |
| value 在 LSM 内 | 可选 blob/value separation |
| 简单 block cache | high/low priority cache、partitioned index/filter、table reader cache |

### 要回答的问题

- RocksDB 为什么要引入 Column Family？
- `SuperVersion` 解决了 LevelDB 哪类并发问题？
- 多个 immutable memtable 对 write stall / flush 有什么影响？
- 为什么 RocksDB 需要更复杂的 compaction picker？
- RocksDB 的事务是不是传统数据库事务？

---

## 2. Column Family 和 SuperVersion

这是 RocksDB 相对 LevelDB 的第一核心增量。

入口文件：

- `db/column_family.h`
- `db/column_family.cc`
- `db/db_impl/db_impl_open.cc`
- `db/version_set.h`
- `db/version_set.cc`

阅读顺序：

1. `ColumnFamilyData`
2. `ColumnFamilySet`
3. `SuperVersion`
4. `DBImpl::CreateColumnFamily`
5. `DBImpl::DropColumnFamily`
6. `VersionSet::LogAndApply`

核心理解：

```text
ColumnFamilyData 是一个 CF 的运行时状态：
  - memtable
  - immutable memtable list
  - current Version
  - options
  - SuperVersion

SuperVersion 是读路径看到的一致视图：
  - mem
  - imm
  - current version
```

LevelDB 里读路径可以直接围绕 `mem_ / imm_ / current_` 理解。RocksDB 多了 CF、并发 flush/compaction、option 动态修改之后，需要用 `SuperVersion` 把读路径看到的对象组合成稳定快照。

重点问题：

- `ColumnFamilyData` 和 `ColumnFamilyHandle` 的关系是什么？
- 一个 DB 是一个 MANIFEST 还是每个 CF 一个 MANIFEST？
- 不同 CF 是否可以有不同 comparator？
- `SuperVersion` 为什么要引用计数？
- `InstallSuperVersion` 在什么时候发生？
- Drop CF 后，旧 iterator 持有的 `SuperVersion` 怎么办？

验收标准：

- 能画出 `DBImpl -> ColumnFamilySet -> ColumnFamilyData -> SuperVersion -> Version`。
- 能解释为什么 Get 不需要长期持有 DB mutex。
- 能解释 CF 生命周期如何写入 MANIFEST。

---

## 3. Options 系统和生产配置

RocksDB 的复杂度很大一部分来自 options。

入口文件：

- `include/rocksdb/options.h`
- `options/options.cc`
- `options/db_options.cc`
- `options/cf_options.cc`
- `db/db_impl/db_impl_open.cc`

重点对象：

- `Options`
- `DBOptions`
- `ColumnFamilyOptions`
- `MutableDBOptions`
- `ImmutableDBOptions`
- `ConfigOptions`

理解路线：

1. `Options : public DBOptions, public ColumnFamilyOptions`
2. `DBOptions(options)` 和 `ColumnFamilyOptions(options)` 的拆分逻辑。
3. `SanitizeOptions`
4. mutable options 的动态修改。
5. options 如何影响后台线程、flush、compaction、cache、WAL。

重点问题：

- 为什么 RocksDB 要把 DBOptions 和 ColumnFamilyOptions 分开？
- 哪些 option 是 DB 级别，哪些是 CF 级别？
- `max_background_jobs` 如何拆成 flush / compaction 限额？
- 为什么 `max_background_compactions` / `max_background_flushes` deprecated？
- 哪些配置可以动态修改？

验收标准：

- 能解释 `Options options; DBOptions db_options(options); ColumnFamilyOptions cf_options(options);` 的构造逻辑。
- 能说出 10 个常见 tuning option 分别影响哪条路径。

---

## 4. 写路径增强：WriteThread / BatchGroup / Pipelined Write

LevelDB 的写路径是一个 writer queue。RocksDB 在这个基础上做了更多并发优化。

入口文件：

- `db/db_impl/db_impl_write.cc`
- `db/write_thread.h`
- `db/write_thread.cc`
- `db/write_batch.cc`
- `db/memtable.cc`

主路径：

```text
DBImpl::Put
  -> DB::Put
  -> DBImpl::Write
  -> DBImpl::WriteImpl
  -> DBImpl::PreprocessWrite
  -> WriteThread::JoinBatchGroup
  -> WriteThread::EnterAsBatchGroupLeader
  -> log::Writer::AddRecord
  -> WriteBatchInternal::InsertInto
  -> MemTable::Add
```

第一轮只看默认路径：

- 非 transaction
- 非 unordered_write
- 非 two_write_queues
- 非 pipelined write

第二轮再看增强路径：

- `PipelinedWriteImpl`
- `unordered_write`
- `allow_concurrent_memtable_write`
- `enable_pipelined_write`
- `manual_wal_flush`

重点问题：

- `WriteThread` 相对 LevelDB writer queue 改进了什么？
- leader / follower 如何组成 batch group？
- sequence number 在哪里分配？
- WAL 写和 memtable 写能否并行？
- `pipelined_write` 为什么能提升吞吐？
- `unordered_write` 为什么会放松 snapshot immutability？

验收标准：

- 能画出普通 WriteImpl 的状态机。
- 能解释 pipelined write 把 WAL 和 memtable 阶段怎么拆开。
- 能解释为什么 `unordered_write` 会影响 snapshot 语义。

---

## 5. MemTableList / Flush / SuperVersion 安装

LevelDB 只有一个 `imm_`。RocksDB 有 immutable memtable list，并且 flush 可以并发。

入口文件：

- `db/memtable_list.h`
- `db/memtable_list.cc`
- `db/db_impl/db_impl_compaction_flush.cc`
- `db/flush_job.cc`
- `db/builder.cc`
- `table/block_based/block_based_table_builder.cc`

主路径：

```text
PreprocessWrite
  -> SwitchMemtable / SwitchWAL
  -> SchedulePendingFlush
  -> MaybeScheduleFlushOrCompaction
  -> BackgroundCallFlush
  -> FlushMemTableToOutputFile
  -> FlushJob::Run
  -> FlushJob::WriteLevel0Table
  -> VersionSet::LogAndApply
  -> InstallSuperVersionAndScheduleWork
```

重点问题：

- mutable memtable 什么时候切成 immutable？
- 一个 WAL 是否严格一对一对应一个 memtable？
- 多个 immutable memtable 如何排序？
- flush 完成后 `VersionEdit` 里写了什么？
- flush 完为什么要安装新的 `SuperVersion`？
- parallel flush 如何影响 write stall？
- atomic flush 是什么，第一轮为什么可以先跳过？

验收标准：

- 能解释从 memtable 满到 L0 SST 安装完成的全链路。
- 能解释 flush queue 和 compaction queue 的关系。

---

## 6. Compaction：RocksDB 最重要的增强模块

Compaction 是 RocksDB 相比 LevelDB 最值得重点学习的地方。

入口文件：

- `db/compaction/compaction_picker.h`
- `db/compaction/compaction_picker_level.cc`
- `db/compaction/compaction_picker_universal.cc`
- `db/compaction/compaction_picker_fifo.cc`
- `db/compaction/compaction.cc`
- `db/compaction/compaction_job.cc`
- `db/compaction/compaction_iterator.cc`
- `db/compaction/subcompaction_state.h`
- `db/db_impl/db_impl_compaction_flush.cc`

阅读顺序：

1. `DBImpl::MaybeScheduleFlushOrCompaction`
2. `DBImpl::BackgroundCallCompaction`
3. `DBImpl::BackgroundCompaction`
4. `ColumnFamilyData::PickCompaction`
5. `LevelCompactionPicker::PickCompaction`
6. `CompactionJob::Run`
7. `CompactionJob::ProcessKeyValueCompaction`
8. `CompactionIterator`
9. `VersionSet::LogAndApply`

### 6.1 Picker

重点：

- leveled compaction score
- L0 compaction
- intra-L0 compaction
- trivial move
- manual compaction
- universal compaction
- FIFO compaction
- bottommost level
- grandparent overlap

问题：

- RocksDB 的 compaction score 如何计算？
- L0 为什么特殊？
- universal compaction 和 leveled compaction 的目标差异是什么？
- FIFO compaction 适合什么场景？
- manual compaction 如何和自动 compaction 交互？

### 6.2 CompactionJob

重点：

- input iterator 构造
- snapshot visibility
- deletion drop
- merge operand handling
- range tombstone
- output file 切分
- subcompaction
- compaction filter
- bottommost compression
- table properties
- event listener / stats

问题：

- compaction 为什么可以删除旧版本？
- deletion marker 什么时候可以 drop？
- merge operand 在 compaction 中如何处理？
- output file 为什么要按 size / grandparent overlap 切？
- subcompaction 如何拆 range？
- compaction 完成后如何原子安装新 version？

### 6.3 Write Stall

RocksDB 的 write stall 比 LevelDB 丰富很多。

重点：

- L0 file count
- pending compaction bytes
- immutable memtable 数量
- hard / soft pending compaction bytes
- delayed write rate
- stop / slowdown

问题：

- write stall 是由 flush 触发还是 compaction 触发？
- slowdown 和 stop 的区别是什么？
- `level0_slowdown_writes_trigger` 和 `level0_stop_writes_trigger` 如何工作？
- `soft_pending_compaction_bytes_limit` 和 `hard_pending_compaction_bytes_limit` 如何工作？

验收标准：

- 能画出 picker -> job -> version install 的全链路。
- 能解释 RocksDB compaction 相对 LevelDB 多出来的复杂度来自哪里。
- 能解释一次 write stall 的定位路径。

---

## 7. 读路径增强：SuperVersion / MultiGet / PinnableSlice / Merge

入口文件：

- `db/db_impl/db_impl.cc`
- `db/memtable.cc`
- `db/memtable_list.cc`
- `db/version_set.cc`
- `db/table_cache.cc`
- `table/block_based/block_based_table_reader.cc`
- `db/db_iter.cc`
- `db/merge_context.h`

LevelDB 基础路径你已经熟悉，RocksDB 重点看增强点：

### 7.1 SuperVersion 读视图

问题：

- Get 如何拿到 `SuperVersion`？
- `SuperVersion` 如何避免读路径长期持锁？
- options / version / memtable 改变后旧读如何安全结束？

### 7.2 MultiGet

重点：

- batch key lookup
- 按 CF / file / data block 组织读
- block cache 命中优化
- IO batching

问题：

- MultiGet 为什么不是简单循环 Get？
- 它如何减少重复打开 table / 读 block 的开销？

### 7.3 PinnableSlice

问题：

- 为什么 Get 不一定要拷贝 value？
- value 什么时候可以 pin 在 block cache / memtable？
- pin 的生命周期如何管理？

### 7.4 Merge Operator

重点文件：

- `db/merge_context.h`
- `db/merge_helper.cc`
- `utilities/merge_operators/`

问题：

- Merge operand 和 Put/Delete 的可见性如何统一？
- Get 遇到 merge operand 如何向下收集？
- compaction 中什么时候可以 partial merge？
- `GetMergeOperands()` 为什么需要 hard limit？

验收标准：

- 能解释 RocksDB Get 相比 LevelDB 多了哪些工程优化。
- 能解释 Merge 为什么是 LSM 里很自然但也很复杂的能力。

---

## 8. BlockBasedTable / Cache / Filter / Table Properties

LevelDB 的 table 格式已经有 block/index/filter。RocksDB 的重点是生产级 cache 和 table reader。

入口文件：

- `table/block_based/block_based_table_reader.cc`
- `table/block_based/block_based_table_builder.cc`
- `table/block_based/filter_block_reader_common_prefix.cc`
- `table/block_based/partitioned_filter_block.cc`
- `cache/`
- `db/table_cache.cc`

重点主题：

- block cache
- row cache
- table cache
- high priority cache
- cache key
- partitioned index
- partitioned filter
- prefix bloom
- whole key bloom
- data block hash index
- compression dictionary
- checksum
- table properties

问题：

- block cache 和 table cache 分别缓存什么？
- index/filter 为什么也要进 block cache？
- partitioned index/filter 解决什么问题？
- prefix bloom 和 whole key bloom 有什么差异？
- table properties 如何服务 compaction / query / debug？
- cache priority 如何避免 index/filter 被 data block 挤掉？

验收标准：

- 能画出一次点查在 block table 内部经过 index/filter/data block 的流程。
- 能解释 block cache tuning 的几个关键参数。

---

## 9. TransactionDB / OptimisticTransactionDB

RocksDB 的事务不是 B+Tree 数据库那种 page-level undo/redo 事务。它是在 LSM 写路径之上的事务层。

入口文件：

- `utilities/transactions/transaction_db_impl.cc`
- `utilities/transactions/pessimistic_transaction_db.cc`
- `utilities/transactions/pessimistic_transaction.cc`
- `utilities/transactions/optimistic_transaction_db_impl.cc`
- `utilities/transactions/optimistic_transaction.cc`
- `utilities/transactions/transaction_lock_mgr.cc`
- `utilities/transactions/write_prepared_txn_db.cc`
- `utilities/transactions/write_unprepared_txn_db.cc`

阅读顺序：

1. `TransactionDB::Open`
2. `PessimisticTransactionDB::BeginTransaction`
3. `PessimisticTransaction::TryLock`
4. `PessimisticTransaction::Commit`
5. `OptimisticTransaction::Commit`
6. `WritePreparedTxnDB`
7. `WriteUnpreparedTxnDB`

重点概念：

- `WriteBatchWithIndex`
- key-level lock
- conflict detection
- snapshot validation
- transaction prepare / commit / rollback
- write policy
- commit sequence
- prepared section recovery

三种策略：

| 策略 | 特点 |
| --- | --- |
| `WRITE_COMMITTED` | 最容易理解，commit 后写入 |
| `WRITE_PREPARED` | prepare 先进入 DB，读路径要判断 prepared/committed 可见性 |
| `WRITE_UNPREPARED` | 更激进，减少 prepare 开销，但读路径/恢复更复杂 |

重点问题：

- Optimistic transaction 和 pessimistic transaction 差异是什么？
- key lock 是怎么实现的？
- commit 时如何检测冲突？
- WritePrepared 为什么需要修改读可见性判断？
- prepared transaction crash recovery 如何处理？
- TransactionDB 和普通 DB 写路径如何复用？

验收标准：

- 能解释 RocksDB 事务为什么不是传统关系数据库事务。
- 能画出 pessimistic transaction 的 lock -> write batch -> commit 流程。
- 能解释 WritePrepared 的读路径为什么复杂。

---

## 10. Column Family 进阶：多 CF 调度和资源隔离

第一轮 CF 只理解对象关系。第二轮要看多 CF 下的资源调度。

重点文件：

- `db/column_family.cc`
- `db/db_impl/db_impl_compaction_flush.cc`
- `db/version_set.cc`
- `options/`

重点问题：

- 不同 CF 如何共享后台线程？
- flush/compaction priority 如何在 CF 之间调度？
- 一个 CF write stall 是否会影响其他 CF？
- CF options 如何独立配置？
- 多 CF 的 MANIFEST edit 如何保证一致性？
- atomic flush 解决什么问题？

验收标准：

- 能解释 MyRocks 为什么大量依赖 Column Family。
- 能解释 CF 是逻辑隔离，不是完整独立 DB。

---

## 11. External SST Ingestion / Bulk Load

这是 RocksDB 相对 LevelDB 非常实用的增强能力。

入口文件：

- `db/external_sst_file_ingestion_job.cc`
- `include/rocksdb/sst_file_writer.h`
- `table/sst_file_writer.cc`

主流程：

```text
SstFileWriter 生成外部 SST
  -> DB::IngestExternalFile
  -> ExternalSstFileIngestionJob::Prepare
  -> AssignLevelAndSeqnoForIngestedFile
  -> AssignGlobalSeqnoForIngestedFile
  -> VersionEdit::AddFile
  -> VersionSet::LogAndApply
```

重点问题：

- ingest 是不是 bulk load？
- 外部 SST 可以放到 L0 以外吗？
- 如果外部 SST 和已有数据 overlap，sequence number 怎么处理？
- global sequence number 是写入每个 key，还是 file-level 逻辑？
- `allow_global_seqno` / `write_global_seqno` 的区别是什么？
- ingest 和 compaction 去重是什么关系？

验收标准：

- 能解释 external SST 不需要先进 WAL/memtable。
- 能解释 global seqno 如何保证可见性语义。

---

## 12. Blob / Value Separation

RocksDB 后来引入 integrated blob，把大 value 从 LSM 主路径中分离出去。

入口文件：

- `db/blob/`
- `utilities/blob_db/`
- `db/compaction/compaction_job.cc`

重点概念：

- blob file
- value separation
- blob index
- blob GC
- compaction 和 blob 生命周期
- large value 对 write amplification 的影响

重点问题：

- 为什么大 value 放在 LSM 里会放大 compaction cost？
- blob index 存在哪里？
- Get 如何从 SST 里的 blob index 找到 blob value？
- blob GC 和 compaction 如何配合？
- integrated blob 和 legacy BlobDB 有什么区别？

验收标准：

- 能解释 value separation 解决什么问题，也能解释它引入了什么新复杂度。

---

## 13. DeleteRange / Wide Column / Merge / Timestamp

这些属于 RocksDB 丰富语义层，不是第一优先级，但值得按主题看。

### DeleteRange

入口：

- `db/range_tombstone_fragmenter.cc`
- `db/range_del_aggregator.cc`

问题：

- range tombstone 如何和 point key 合并可见性？
- compaction 中如何处理 range delete？

### Wide Column

入口：

- `db/wide/`
- `include/rocksdb/wide_columns.h`

问题：

- `PutEntity` 是什么？
- wide columns 是存储层结构化 value，还是完整宽表数据库？
- 普通 Get 和 GetEntity 如何兼容？

### User-defined Timestamp

入口：

- `db/db_impl/db_impl.cc`
- `db/db_iter.cc`
- comparator 相关接口

问题：

- timestamp 如何进入 internal key / comparator？
- snapshot sequence 和 user timestamp 是什么关系？

---

## 14. Observability / Debug / Tuning

RocksDB 的生产价值很大一部分在观测和调优。

入口：

- `monitoring/`
- `include/rocksdb/statistics.h`
- `include/rocksdb/listener.h`
- `tools/db_bench_tool.cc`
- `db_stress_tool/`
- `tools/ldb_cmd.cc`

重点：

- `Statistics`
- `PerfContext`
- `IOStatsContext`
- event listener
- compaction stats
- stall stats
- block cache stats
- `db_bench`
- `db_stress`
- `ldb`

重点问题：

- 如何定位 write stall？
- 如何看 block cache 命中率？
- 如何看 compaction read/write bytes？
- 如何判断是 flush 跟不上还是 compaction 跟不上？
- db_bench 如何构造 workload？
- db_stress 为什么是 RocksDB 很重要的测试工具？

验收标准：

- 能设计一个 RocksDB dashboard。
- 能解释一次 write stall 的观测链路。
- 能用 db_bench 验证一个 option 的影响。

---

## 15. 建议阅读顺序

如果目标是基于 LevelDB 快速理解 RocksDB，建议顺序如下：

```text
Week 1:
  ColumnFamily / SuperVersion / Options

Week 2:
  WriteThread / WriteImpl / Flush / MemTableList

Week 3-4:
  Compaction picker / CompactionJob / Write stall

Week 5:
  Read path enhancements / BlockBasedTable / Cache

Week 6:
  TransactionDB / OptimisticTransactionDB / WritePrepared

Week 7:
  External SST ingest / Blob / DeleteRange / Merge

Week 8:
  Observability / db_bench / db_stress / tuning
```

如果时间更少，只看四条主线：

```text
1. ColumnFamily + SuperVersion
2. WriteThread + Flush + Compaction
3. TransactionDB
4. Block cache / Table / Observability
```

---

## 16. 不建议第一轮深挖的内容

这些可以放到第二轮：

- Java / C binding
- backup engine
- checkpoint
- TTL DB
- secondary DB
- remote compaction
- trace replay
- Cassandra format
- custom env / custom file system
- persistent cache
- `db_stress` 内部所有细节

---

## 17. 最终验收标准

学完这份计划后，应该能回答：

1. RocksDB 为什么需要 Column Family，CF 和 DB 的边界是什么？
2. `SuperVersion` 解决了什么并发读问题？
3. RocksDB write path 相对 LevelDB 的 writer queue 有哪些增强？
4. RocksDB flush 为什么要支持多个 immutable memtable 和并发 flush？
5. RocksDB compaction picker 为什么复杂，leveled/universal/FIFO 分别适合什么场景？
6. Write stall 如何产生，如何定位？
7. Block cache / table cache / partitioned filter 分别解决什么问题？
8. TransactionDB 是怎么在 LSM 上实现事务的？
9. External SST ingest 如何处理 sequence number？
10. Blob/value separation 解决什么问题，又引入什么复杂度？
11. 如何用 statistics/perf context/db_bench 定位性能问题？

如果这些问题能结合代码入口说清楚，就说明你已经不是在按 LevelDB 重复读 RocksDB，而是在读 RocksDB 真正新增的生产系统能力。
