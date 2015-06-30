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
import org.rocksdb.{HistogramData, HistogramType, Options, TickerType}

class RocksDbStatisticMetrics(val storeName: String = "unknown",
                       val options: Options,
                       val registry: MetricsRegistry = new MetricsRegistryMap()) extends MetricsHelper {

  val statistic = options.statisticsPtr()

  private def ticker(tickerType: TickerType) = {
    statistic.getTickerCount(tickerType)
  }

  private def histogram(histogramType: HistogramType): HistogramData = {
    statistic.geHistogramData(histogramType)
  }

  def blockCacheMiss() = {
    val counter = newCounter("block-cache-miss")
    counter.set(ticker(TickerType.BLOCK_CACHE_MISS))
    counter
  }

  def blockCacheHit() = {
    val counter = newCounter("block-cache-hit")
    counter.set(ticker(TickerType.BLOCK_CACHE_HIT))
    counter
  }

  def blockCacheAdd() = {
    val counter = newCounter("block-cache-add")
    counter.set(ticker(TickerType.BLOCK_CACHE_ADD))
    counter
  }

  def blockCacheIndexMiss() = {
    val counter = newCounter("block-cache-index-miss")
    counter.set(ticker(TickerType.BLOCK_CACHE_INDEX_MISS))
    counter
  }

  def blockCacheIndexHit() = {
    val counter = newCounter("block-cache-index-hit")
    counter.set(ticker(TickerType.BLOCK_CACHE_INDEX_HIT))
    counter
  }

  def blockCacheFilterMiss() = {
    val counter = newCounter("block-cache-filter-miss")
    counter.set(ticker(TickerType.BLOCK_CACHE_FILTER_MISS))
    counter
  }

  def blockCacheFilterHit() = {
    val counter = newCounter("block-cache-filter-hit")
    counter.set(ticker(TickerType.BLOCK_CACHE_FILTER_HIT))
    counter
  }

  def blockCacheDataMiss() = {
    val counter = newCounter("block-cache-data-miss")
    counter.set(ticker(TickerType.BLOCK_CACHE_DATA_MISS))
    counter
  }

  def blockCacheDataHit() = {
    val counter = newCounter("block-cache-data-hit")
    counter.set(ticker(TickerType.BLOCK_CACHE_DATA_HIT))
    counter
  }

  def bloomFilterUseful() = {
    val counter = newCounter("bloom-filter-useful")
    counter.set(ticker(TickerType.BLOOM_FILTER_USEFUL))
    counter
  }

  def memtableHit() = {
    val counter = newCounter("memtable-hit")
    counter.set(ticker(TickerType.MEMTABLE_HIT))
    counter
  }

  def memtableMiss() = {
    val counter = newCounter("memtable-miss")
    counter.set(ticker(TickerType.MEMTABLE_MISS))
    counter
  }

  def getHitL0() = {
    val counter = newCounter("get-hit-l0")
    counter.set(ticker(TickerType.GET_HIT_L0))
    counter
  }

  def getHitL1() = {
    val counter = newCounter("get-hit-l1")
    counter.set(ticker(TickerType.GET_HIT_L1))
    counter
  }

  def getHitL2AndUp() = {
    val counter = newCounter("get-hit-l2-and-up")
    counter.set(ticker(TickerType.GET_HIT_L2_AND_UP))
    counter
  }

  def compactionKeyDropNewerEntry() = {
    val counter = newCounter("compaction-key-drop-newer-entry")
    counter.set(ticker(TickerType.COMPACTION_KEY_DROP_NEWER_ENTRY))
    counter
  }

  def compactionKeyDropObsolete() = {
    val counter = newCounter("compaction-key-drop-obsolete")
    counter.set(ticker(TickerType.COMPACTION_KEY_DROP_OBSOLETE))
    counter
  }

  def compactionKeyDropUser() = {
    val counter = newCounter("compaction-key-drop-user")
    counter.set(ticker(TickerType.COMPACTION_KEY_DROP_USER))
    counter
  }

  def numberKeysWritten() = {
    val counter = newCounter("number-keys-written")
    counter.set(ticker(TickerType.NUMBER_KEYS_WRITTEN))
    counter
  }

  def numberKeysRead() = {
    val counter = newCounter("number-keys-read")
    counter.set(ticker(TickerType.NUMBER_KEYS_READ))
    counter
  }

  def numberKeysUpdated() = {
    val counter = newCounter("number-keys-updated")
    counter.set(ticker(TickerType.NUMBER_KEYS_UPDATED))
    counter
  }

  def bytesWritten() = {
    val counter = newCounter("bytes-written")
    counter.set(ticker(TickerType.BYTES_WRITTEN))
    counter
  }

  def bytesRead() = {
    val counter = newCounter("bytes-read")
    counter.set(ticker(TickerType.BYTES_READ))
    counter
  }

  def noFileCloses() = {
    val counter = newCounter("no-file-closes")
    counter.set(ticker(TickerType.NO_FILE_CLOSES))
    counter
  }

  def noFileOpens() = {
    val counter = newCounter("no-file-opens")
    counter.set(ticker(TickerType.NO_FILE_OPENS))
    counter
  }

  def noFileErrors() = {
    val counter = newCounter("no-file-errors")
    counter.set(ticker(TickerType.NO_FILE_ERRORS))
    counter
  }

  def stallL0SlowdownMicros() = {
    val counter = newCounter("stall-l0-slowdown-micros")
    counter.set(ticker(TickerType.STALL_L0_SLOWDOWN_MICROS))
    counter
  }

  def stallMemtableCompactionMicros() = {
    val counter = newCounter("stall-memtable-compaction-micros")
    counter.set(ticker(TickerType.STALL_MEMTABLE_COMPACTION_MICROS))
    counter
  }

  def stallL0NumFilesMicros() = {
    val counter = newCounter("stall-l0-num-files-micros")
    counter.set(ticker(TickerType.STALL_L0_NUM_FILES_MICROS))
    counter
  }

  def stallMicros() = {
    val counter = newCounter("stall-micros")
    counter.set(ticker(TickerType.STALL_MICROS))
    counter
  }

  def dbMutexWaitMicros() = {
    val counter = newCounter("db-mutex-wait-micros")
    counter.set(ticker(TickerType.DB_MUTEX_WAIT_MICROS))
    counter
  }

  def rateLimitDelayMillis() = {
    val counter = newCounter("rate-limit-delay-millis")
    counter.set(ticker(TickerType.RATE_LIMIT_DELAY_MILLIS))
    counter
  }

  def noIterators() = {
    val counter = newCounter("no-iterators")
    counter.set(ticker(TickerType.NO_ITERATORS))
    counter
  }

  def numberMultigetCalls() = {
    val counter = newCounter("number-multiget-calls")
    counter.set(ticker(TickerType.NUMBER_MULTIGET_CALLS))
    counter
  }

  def numberMultigetKeysRead() = {
    val counter = newCounter("number-multiget-keys-read")
    counter.set(ticker(TickerType.NUMBER_MULTIGET_KEYS_READ))
    counter
  }

  def numberMultigetBytesRead() = {
    val counter = newCounter("number-multiget-bytes-read")
    counter.set(ticker(TickerType.NUMBER_MULTIGET_BYTES_READ))
    counter
  }

  def numberFilteredDeletes() = {
    val counter = newCounter("number-filtered-deletes")
    counter.set(ticker(TickerType.NUMBER_FILTERED_DELETES))
    counter
  }

  def numberMergeFailures() = {
    val counter = newCounter("number-merge-failures")
    counter.set(ticker(TickerType.NUMBER_MERGE_FAILURES))
    counter
  }

  def sequenceNumber() = {
    val counter = newCounter("sequence-number")
    counter.set(ticker(TickerType.SEQUENCE_NUMBER))
    counter
  }

  def bloomFilterPrefixChecked() = {
    val counter = newCounter("bloom-filter-prefix-checked")
    counter.set(ticker(TickerType.BLOOM_FILTER_PREFIX_CHECKED))
    counter
  }

  def bloomFilterPrefixUseful() = {
    val counter = newCounter("bloom-filter-prefix-useful")
    counter.set(ticker(TickerType.BLOOM_FILTER_PREFIX_USEFUL))
    counter
  }

  def numberOfReseeksInIteration() = {
    val counter = newCounter("number-of-reseeks-in-iteration")
    counter.set(ticker(TickerType.NUMBER_OF_RESEEKS_IN_ITERATION))
    counter
  }

  def getUpdatesSinceCalls() = {
    val counter = newCounter("get-updates-since-calls")
    counter.set(ticker(TickerType.GET_UPDATES_SINCE_CALLS))
    counter
  }

  def blockCacheCompressedMiss() = {
    val counter = newCounter("block-cache-compressed-miss")
    counter.set(ticker(TickerType.BLOCK_CACHE_COMPRESSED_MISS))
    counter
  }

  def blockCacheCompressedHit() = {
    val counter = newCounter("block-cache-compressed-hit")
    counter.set(ticker(TickerType.BLOCK_CACHE_COMPRESSED_HIT))
    counter
  }

  def walFileSynced() = {
    val counter = newCounter("wal-file-synced")
    counter.set(ticker(TickerType.WAL_FILE_SYNCED))
    counter
  }

  def walFileBytes() = {
    val counter = newCounter("wal-file-bytes")
    counter.set(ticker(TickerType.WAL_FILE_BYTES))
    counter
  }

  def walDoneBySelf() = {
    val counter = newCounter("write-done-by-self")
    counter.set(ticker(TickerType.WRITE_DONE_BY_SELF))
    counter
  }

  def writeDoneByOther() = {
    val counter = newCounter("write-done-by-other")
    counter.set(ticker(TickerType.WRITE_DONE_BY_OTHER))
    counter
  }

  def writeTimeout() = {
    val counter = newCounter("write-timedout")
    counter.set(ticker(TickerType.WRITE_TIMEDOUT))
    counter
  }

  def writeWithWal() = {
    val counter = newCounter("write-with-wal")
    counter.set(ticker(TickerType.WRITE_WITH_WAL))
    counter
  }

  def compactReadBytes() = {
    val counter = newCounter("compact-read-bytes")
    counter.set(ticker(TickerType.COMPACT_READ_BYTES))
    counter
  }

  def compactWriteBytes() = {
    val counter = newCounter("compact-write-bytes")
    counter.set(ticker(TickerType.COMPACT_WRITE_BYTES))
    counter
  }

  def flushWriteBytes() = {
    val counter = newCounter("flush-write-bytes")
    counter.set(ticker(TickerType.FLUSH_WRITE_BYTES))
    counter
  }

  def numberDirectLoadTableProperties() = {
    val counter = newCounter("number-direct-load-table-properties")
    counter.set(ticker(TickerType.NUMBER_DIRECT_LOAD_TABLE_PROPERTIES))
    counter
  }

  def numberSupervisionAcquiries() = {
    val counter = newCounter("number-superversion-acquires")
    counter.set(ticker(TickerType.NUMBER_SUPERVERSION_ACQUIRES))
    counter
  }

  def numberSupervisionReleases() = {
    val counter = newCounter("number-superversion-releases")
    counter.set(ticker(TickerType.NUMBER_SUPERVERSION_RELEASES))
    counter
  }

  def numberSupervisionCleanups() = {
    val counter = newCounter("number-superversion-cleanups")
    counter.set(ticker(TickerType.NUMBER_SUPERVERSION_CLEANUPS))
    counter
  }

  def numberBlockNotCompressed() = {
    val counter = newCounter("number-block-not-compressed")
    counter.set(ticker(TickerType.NUMBER_BLOCK_NOT_COMPRESSED))
    counter
  }

  def compactionOutfileSyncMicrosHistogram() = newGauge("compaction-outfile-sync-micros", histogram(HistogramType.COMPACTION_OUTFILE_SYNC_MICROS))
  def compactionTimeHistogram() = newGauge("compaction-time", histogram(HistogramType.COMPACTION_TIME))
  def dbGetHistogram() = newGauge("db-get", histogram(HistogramType.DB_GET))
  def dbMultiGetHistogram() = newGauge("db-multiget", histogram(HistogramType.DB_MULTIGET))
  def dbSeekHistogram() = newGauge("db-seek", histogram(HistogramType.DB_SEEK))
  def dbWriteHistogram() = newGauge("db-write", histogram(HistogramType.DB_WRITE))
  def hardRateLimitDelayCountHistogram() = newGauge("hard-rate-limit-delay-count", histogram(HistogramType.HARD_RATE_LIMIT_DELAY_COUNT))
  def manifestFileSyncMicrosHistogram() = newGauge("manifest-file-sync-micros", histogram(HistogramType.MANIFEST_FILE_SYNC_MICROS))
  def numFilesInSingleCompactionHistogram() = newGauge("num-files-in-single-compaction", histogram(HistogramType.NUM_FILES_IN_SINGLE_COMPACTION))
  def readBlockCompactionMicrosHistogram() = newGauge("read-block-compaction-micros", histogram(HistogramType.READ_BLOCK_COMPACTION_MICROS))
  def readBlockGetMicrosHistogram() = newGauge("read-block-get-micros", histogram(HistogramType.READ_BLOCK_GET_MICROS))
  def softRateLimitDelayCountHistogram() = newGauge("soft-rate-limit-delay-count", histogram(HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT))
  def stallL0NumFilesCountHistogram() = newGauge("stall-l0-num-files-count", histogram(HistogramType.STALL_L0_NUM_FILES_COUNT))
  def stallL0SlowdownCountHistogram() = newGauge("stall-l0-slowdown-count", histogram(HistogramType.STALL_L0_SLOWDOWN_COUNT))
  def stallMemtableCompactionCountHistogram() = newGauge("stall-memtable-compaction-count", histogram(HistogramType.STALL_MEMTABLE_COMPACTION_COUNT))
  def tableOpenIOMicrosHistogram() = newGauge("table-open-io-micros", histogram(HistogramType.TABLE_OPEN_IO_MICROS))
  def tableSyncMicrosHistogram() = newGauge("table-sync-micros", histogram(HistogramType.TABLE_SYNC_MICROS))
  def walFileSyncMicrosHistogram() = newGauge("wal-file-sync-micros", histogram(HistogramType.WAL_FILE_SYNC_MICROS))
  def writeRawBlockMicrosHistogram() = newGauge("write-raw-block-micros", histogram(HistogramType.WRITE_RAW_BLOCK_MICROS))
  def writeStallHistogram() = newGauge("write-stall", histogram(HistogramType.WRITE_STALL))
}
