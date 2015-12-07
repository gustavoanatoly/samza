/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.storage.kv

import org.apache.samza.metrics.{Counter, MetricsHelper, MetricsRegistry, MetricsRegistryMap}
import org.rocksdb._

class RocksDbStatisticMetrics(val storeName: String = "unknown",
                       val options: Options,
                       val writeOptions: WriteOptions,
                       val metrics: KeyValueStoreMetrics) {

  val statistic = options.statisticsPtr()

  /**
   * Counters
   */
  val counterBlockCacheMiss = metrics.newCounter("block-cache-miss")
  val counterBlockCacheHit = metrics.newCounter("block-cache-hit")
  val counterBlockCacheAdd = metrics.newCounter("block-cache-add")
  val counterBlockCacheIndexMiss = metrics.newCounter("block-cache-index-miss")
  val counterBlockCacheIndexHit = metrics.newCounter("block-cache-index-hit")
  val counterBlockCacheFilterMiss = metrics.newCounter("block-cache-filter-miss")
  val counterBlockCacheFilterHit = metrics.newCounter("block-cache-filter-hit")
  val counterBlockCacheDataMiss = metrics.newCounter("block-cache-data-miss")
  val counterBlockCacheDataHit = metrics.newCounter("block-cache-data-hit")
  val counterBloomFilterUseful = metrics.newCounter("bloom-filter-useful")
  val counterMemtableHit = metrics.newCounter("memtable-hit")
  val counterMemtableMiss = metrics.newCounter("memtable-miss")
  val counterGetHitL0 = metrics.newCounter("get-hit-l0")
  val counterGetHitL1 = metrics.newCounter("get-hit-l1")
  val counterGetHitL2AndUp = metrics.newCounter("get-hit-l2-and-up")
  val counterCompactionKeyDropNewerEntry = metrics.newCounter("compaction-key-drop-newer-entry")
  val counterCompactionKeyDropObsolete = metrics.newCounter("compaction-key-drop-obsolete")
  val counterCompactionKeyDropUser = metrics.newCounter("compaction-key-drop-user")
  val counterNumberKeysWritten = metrics.newCounter("number-keys-written")
  val counterNumberKeysRead = metrics.newCounter("number-keys-read")
  val counterNumberKeysUpdated = metrics.newCounter("number-keys-updated")
  val counterBytesWritten = metrics.newCounter("bytes-written")
  val counterBytesRead = metrics.newCounter("bytes-read")
  val counterNoFileCloses = metrics.newCounter("no-file-closes")
  val counterNoFileOpens = metrics.newCounter("no-file-opens")
  val counterNoFileErrors = metrics.newCounter("no-file-errors")
  val counterStallL0SlowdownMicros = metrics.newCounter("stall-l0-slowdown-micros")
  val counterStallMemtableCompactionMicros = metrics.newCounter("stall-memtable-compaction-micros")
  val counterStallL0NumFilesMicros = metrics.newCounter("stall-l0-num-files-micros")
  val counterStallMicros = metrics.newCounter("stall-micros")
  val counterDbMutexWaitMicros = metrics.newCounter("db-mutex-wait-micros")
  val counterRateLimitDelayMillis = metrics.newCounter("rate-limit-delay-millis")
  val counterNoIterators = metrics.newCounter("no-iterators")
  val counterNumberMultigetCalls = metrics.newCounter("number-multiget-calls")
  val counterNumberMultigetKeysRead = metrics.newCounter("number-multiget-keys-read")
  val counterNumberMultigetBytesRead = metrics.newCounter("number-multiget-bytes-read")
  val counterNumberFilteredDeletes = metrics.newCounter("number-filtered-deletes")
  val counterNumberMergeFailures = metrics.newCounter("number-merge-failures")
  val counterSequenceNumber = metrics.newCounter("sequence-number")
  val counterBloomFilterPrefixChecked = metrics.newCounter("bloom-filter-prefix-checked")
  val counterBloomFilterPrefixUseful = metrics.newCounter("bloom-filter-prefix-useful")
  val counterNumberOfReseeksInIteration = metrics.newCounter("number-of-reseeks-in-iteration")
  val counterGetUpdatesSinceCalls = metrics.newCounter("get-updates-since-calls")
  val counterBlockCacheCompressedMiss = metrics.newCounter("block-cache-compressed-miss")
  val counterBlockCacheCompressedHit = metrics.newCounter("block-cache-compressed-hit")
  val counterWalFileSynced = metrics.newCounter("wal-file-synced")
  val counterWalFileBytes = metrics.newCounter("wal-file-bytes")
  val counterWriteDoneBySelf = metrics.newCounter("write-done-by-self")
  val counterWriteDoneByOther = metrics.newCounter("write-done-by-other")
  val counterWriteTimedOut = metrics.newCounter("write-timedout")
  val counterWriteWithWall = metrics.newCounter("write-with-wal")
  val counterCompactReadBytes = metrics.newCounter("compact-read-bytes")
  val counterCompactWriteBytes = metrics.newCounter("compact-write-bytes")
  val counterFlushWriteBytes = metrics.newCounter("flush-write-bytes")
  val counterNumberDirectLoadTableProperties = metrics.newCounter("number-direct-load-table-properties")
  val counterNumberSupervisionAcquires = metrics.newCounter("number-superversion-acquires")
  val counterNumberSupervisionReleases = metrics.newCounter("number-superversion-releases")
  val counterNumberSupervisionCleanups = metrics.newCounter("number-superversion-cleanups")
  val counterNumberBlockNoCompressed = metrics.newCounter("number-block-not-compressed")

  /**
   * Gauges
   */
  val gaugeCompactionOutfileSyncMicrosHistogram = metrics.newGauge("compaction-outfile-sync-micros", histogram(HistogramType.COMPACTION_OUTFILE_SYNC_MICROS))
  val gaugeCompactionTimeHistogram = metrics.newGauge("compaction-time", histogram(HistogramType.COMPACTION_TIME))
  val gaugeDbGetHistogram = metrics.newGauge("db-get", histogram(HistogramType.DB_GET))
  val gaugeDbMultiGetHistogram = metrics.newGauge("db-multiget", histogram(HistogramType.DB_MULTIGET))
  val gaugeDbSeekHistogram = metrics.newGauge("db-seek", histogram(HistogramType.DB_SEEK))
  val gaugeDbWriteHistogram = metrics.newGauge("db-write", histogram(HistogramType.DB_WRITE))
  val gaugeHardRateLimitDelayCountHistogram = metrics.newGauge("hard-rate-limit-delay-count", histogram(HistogramType.HARD_RATE_LIMIT_DELAY_COUNT))
  val gaugeManifestFileSyncMicrosHistogram = metrics.newGauge("manifest-file-sync-micros", histogram(HistogramType.MANIFEST_FILE_SYNC_MICROS))
  val gaugeNumFilesInSingleCompactionHistogram = metrics.newGauge("num-files-in-single-compaction", histogram(HistogramType.NUM_FILES_IN_SINGLE_COMPACTION))
  val gaugeReadBlockCompactionMicrosHistogram = metrics.newGauge("read-block-compaction-micros", histogram(HistogramType.READ_BLOCK_COMPACTION_MICROS))
  val gaugeReadBlockGetMicrosHistogram = metrics.newGauge("read-block-get-micros", histogram(HistogramType.READ_BLOCK_GET_MICROS))
  val gaugeSoftRateLimitDelayCountHistogram = metrics.newGauge("soft-rate-limit-delay-count", histogram(HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT))
  val gaugeStallL0NumFilesCountHistogram = metrics.newGauge("stall-l0-num-files-count", histogram(HistogramType.STALL_L0_NUM_FILES_COUNT))
  val gaugeStallL0SlowdownCountHistogram = metrics.newGauge("stall-l0-slowdown-count", histogram(HistogramType.STALL_L0_SLOWDOWN_COUNT))
  val gaugeStallMemtableCompactionCountHistogram = metrics.newGauge("stall-memtable-compaction-count", histogram(HistogramType.STALL_MEMTABLE_COMPACTION_COUNT))
  val gaugeTableOpenIOMicrosHistogram = metrics.newGauge("table-open-io-micros", histogram(HistogramType.TABLE_OPEN_IO_MICROS))
  val gaugeTableSyncMicrosHistogram = metrics.newGauge("table-sync-micros", histogram(HistogramType.TABLE_SYNC_MICROS))
  val gaugeWalFileSyncMicrosHistogram = metrics.newGauge("wal-file-sync-micros", histogram(HistogramType.WAL_FILE_SYNC_MICROS))
  val gaugeWriteRawBlockMicrosHistogram = metrics.newGauge("write-raw-block-micros", histogram(HistogramType.WRITE_RAW_BLOCK_MICROS))
  val gaugeWriteStallHistogram = metrics.newGauge("write-stall", histogram(HistogramType.WRITE_STALL))

  private def ticker(tickerType: TickerType) = {
    statistic.getTickerCount(tickerType)
  }

  private def histogram(histogramType: HistogramType): HistogramData = {
    statistic.geHistogramData(histogramType)
  }

  /**
   * This method it is responsible to update statistics.
   */
  def updateRocksDbStatistic(): Unit = {

    counterBlockCacheMiss.set(ticker(TickerType.BLOCK_CACHE_MISS))
    counterBlockCacheHit.set(ticker(TickerType.BLOCK_CACHE_HIT))
    counterBlockCacheAdd.set(ticker(TickerType.BLOCK_CACHE_ADD))
    counterBlockCacheIndexMiss.set(ticker(TickerType.BLOCK_CACHE_INDEX_MISS))
    counterBlockCacheIndexHit.set(ticker(TickerType.BLOCK_CACHE_INDEX_HIT))
    counterBlockCacheFilterMiss.set(ticker(TickerType.BLOCK_CACHE_FILTER_MISS))
    counterBlockCacheFilterHit.set(ticker(TickerType.BLOCK_CACHE_FILTER_HIT))
    counterBlockCacheDataMiss.set(ticker(TickerType.BLOCK_CACHE_DATA_MISS))
    counterBlockCacheDataHit.set(ticker(TickerType.BLOCK_CACHE_DATA_HIT))
    counterBloomFilterUseful.set(ticker(TickerType.BLOOM_FILTER_USEFUL))
    counterMemtableHit.set(ticker(TickerType.MEMTABLE_HIT))
    counterMemtableMiss.set(ticker(TickerType.MEMTABLE_MISS))
    counterGetHitL0.set(ticker(TickerType.GET_HIT_L0))
    counterGetHitL1.set(ticker(TickerType.GET_HIT_L1))
    counterGetHitL2AndUp.set(ticker(TickerType.GET_HIT_L2_AND_UP))
    counterCompactionKeyDropNewerEntry.set(ticker(TickerType.COMPACTION_KEY_DROP_NEWER_ENTRY))
    counterCompactionKeyDropObsolete.set(ticker(TickerType.COMPACTION_KEY_DROP_OBSOLETE))
    counterCompactionKeyDropUser.set(ticker(TickerType.COMPACTION_KEY_DROP_USER))
    counterNumberKeysWritten.set(ticker(TickerType.NUMBER_KEYS_WRITTEN))
    counterNumberKeysRead.set(ticker(TickerType.NUMBER_KEYS_READ))
    counterNumberKeysUpdated.set(ticker(TickerType.NUMBER_KEYS_UPDATED))
    counterBytesWritten.set(ticker(TickerType.BYTES_WRITTEN))
    counterBytesRead.set(ticker(TickerType.BYTES_READ))
    counterNoFileCloses.set(ticker(TickerType.NO_FILE_CLOSES))
    counterNoFileOpens.set(ticker(TickerType.NO_FILE_OPENS))
    counterNoFileErrors.set(ticker(TickerType.NO_FILE_ERRORS))
    counterStallL0SlowdownMicros.set(ticker(TickerType.STALL_L0_SLOWDOWN_MICROS))
    counterStallMemtableCompactionMicros.set(ticker(TickerType.STALL_MEMTABLE_COMPACTION_MICROS))
    counterStallL0NumFilesMicros.set(ticker(TickerType.STALL_L0_NUM_FILES_MICROS))
    counterStallMicros.set(ticker(TickerType.STALL_MICROS))
    counterDbMutexWaitMicros.set(ticker(TickerType.DB_MUTEX_WAIT_MICROS))
    counterRateLimitDelayMillis.set(ticker(TickerType.RATE_LIMIT_DELAY_MILLIS))
    counterNoIterators.set(ticker(TickerType.NO_ITERATORS))
    counterNumberMultigetCalls.set(ticker(TickerType.NUMBER_MULTIGET_CALLS))
    counterNumberMultigetKeysRead.set(ticker(TickerType.NUMBER_MULTIGET_KEYS_READ))
    counterNumberMultigetBytesRead.set(ticker(TickerType.NUMBER_MULTIGET_BYTES_READ))
    counterNumberFilteredDeletes.set(ticker(TickerType.NUMBER_FILTERED_DELETES))
    counterNumberMergeFailures.set(ticker(TickerType.NUMBER_MERGE_FAILURES))
    counterSequenceNumber.set(ticker(TickerType.SEQUENCE_NUMBER))
    counterBloomFilterPrefixChecked.set(ticker(TickerType.BLOOM_FILTER_PREFIX_CHECKED))
    counterBloomFilterPrefixUseful.set(ticker(TickerType.BLOOM_FILTER_PREFIX_USEFUL))
    counterNumberOfReseeksInIteration.set(ticker(TickerType.NUMBER_OF_RESEEKS_IN_ITERATION))
    counterGetUpdatesSinceCalls.set(ticker(TickerType.GET_UPDATES_SINCE_CALLS))
    counterBlockCacheCompressedMiss.set(ticker(TickerType.BLOCK_CACHE_COMPRESSED_MISS))
    counterBlockCacheCompressedHit.set(ticker(TickerType.BLOCK_CACHE_COMPRESSED_HIT))
    counterWalFileSynced.set(ticker(TickerType.WAL_FILE_SYNCED))
    counterWalFileBytes.set(ticker(TickerType.WAL_FILE_BYTES))
    counterWriteDoneBySelf.set(ticker(TickerType.WRITE_DONE_BY_SELF))
    counterWriteDoneByOther.set(ticker(TickerType.WRITE_DONE_BY_OTHER))
    counterWriteTimedOut.set(ticker(TickerType.WRITE_TIMEDOUT))
    counterWriteWithWall.set(ticker(TickerType.WRITE_WITH_WAL))
    counterCompactReadBytes.set(ticker(TickerType.COMPACT_READ_BYTES))
    counterCompactWriteBytes.set(ticker(TickerType.COMPACT_WRITE_BYTES))
    counterFlushWriteBytes.set(ticker(TickerType.FLUSH_WRITE_BYTES))
    counterNumberDirectLoadTableProperties.set(ticker(TickerType.NUMBER_DIRECT_LOAD_TABLE_PROPERTIES))
    counterNumberSupervisionAcquires.set(ticker(TickerType.NUMBER_SUPERVERSION_ACQUIRES))
    counterNumberSupervisionReleases.set(ticker(TickerType.NUMBER_SUPERVERSION_RELEASES))
    counterNumberSupervisionCleanups.set(ticker(TickerType.NUMBER_SUPERVERSION_CLEANUPS))
    counterNumberBlockNoCompressed.set(ticker(TickerType.NUMBER_BLOCK_NOT_COMPRESSED))

    gaugeCompactionOutfileSyncMicrosHistogram.set(histogram(HistogramType.COMPACTION_OUTFILE_SYNC_MICROS))
    gaugeCompactionTimeHistogram.set(histogram(HistogramType.COMPACTION_TIME))
    gaugeDbGetHistogram.set(histogram(HistogramType.DB_GET))
    gaugeDbMultiGetHistogram.set(histogram(HistogramType.DB_MULTIGET))
    gaugeDbSeekHistogram.set(histogram(HistogramType.DB_SEEK))
    gaugeDbWriteHistogram.set(histogram(HistogramType.DB_WRITE))
    gaugeHardRateLimitDelayCountHistogram.set(histogram(HistogramType.HARD_RATE_LIMIT_DELAY_COUNT))
    gaugeManifestFileSyncMicrosHistogram.set(histogram(HistogramType.MANIFEST_FILE_SYNC_MICROS))
    gaugeNumFilesInSingleCompactionHistogram.set(histogram(HistogramType.NUM_FILES_IN_SINGLE_COMPACTION))
    gaugeReadBlockCompactionMicrosHistogram.set(histogram(HistogramType.READ_BLOCK_COMPACTION_MICROS))
    gaugeReadBlockGetMicrosHistogram.set(histogram(HistogramType.READ_BLOCK_GET_MICROS))
    gaugeSoftRateLimitDelayCountHistogram.set(histogram(HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT))
    gaugeStallL0NumFilesCountHistogram.set(histogram(HistogramType.STALL_L0_NUM_FILES_COUNT))
    gaugeStallL0SlowdownCountHistogram.set(histogram(HistogramType.STALL_L0_SLOWDOWN_COUNT))
    gaugeStallMemtableCompactionCountHistogram.set(histogram(HistogramType.STALL_MEMTABLE_COMPACTION_COUNT))
    gaugeTableOpenIOMicrosHistogram.set(histogram(HistogramType.TABLE_OPEN_IO_MICROS))
    gaugeTableSyncMicrosHistogram.set(histogram(HistogramType.TABLE_SYNC_MICROS))
    gaugeWalFileSyncMicrosHistogram.set(histogram(HistogramType.WAL_FILE_SYNC_MICROS))
    gaugeWriteRawBlockMicrosHistogram.set(histogram(HistogramType.WRITE_RAW_BLOCK_MICROS))
    gaugeWriteStallHistogram.set(histogram(HistogramType.WRITE_STALL))
  }
}
