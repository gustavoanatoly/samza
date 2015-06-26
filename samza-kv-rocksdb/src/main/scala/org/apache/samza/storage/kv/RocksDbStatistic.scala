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

class RocksDbStatistic(val storeName: String = "unknown",
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
    val counter = newCounter("BLOCK_CACHE_MISS")
    counter.set(ticker(TickerType.BLOCK_CACHE_MISS))
    counter
  }

  def blockCacheHit() = {
    val counter = newCounter("BLOCK_CACHE_HIT")
    counter.set(ticker(TickerType.BLOCK_CACHE_HIT))
    counter
  }

  def blockCacheAdd() = {
    val counter = newCounter("BLOCK_CACHE_ADD")
    counter.set(ticker(TickerType.BLOCK_CACHE_ADD))
    counter
  }

  def blockCacheIndexMiss() = {
    val counter = newCounter("BLOCK_CACHE_INDEX_MISS")
    counter.set(ticker(TickerType.BLOCK_CACHE_INDEX_MISS))
    counter
  }

  def blockCacheIndexHit() = {
    val counter = newCounter("BLOCK_CACHE_INDEX_HIT")
    counter.set(ticker(TickerType.BLOCK_CACHE_INDEX_HIT))
    counter
  }

  def blockCacheFilterMiss() = {
    val counter = newCounter("BLOCK_CACHE_FILTER_MISS")
    counter.set(ticker(TickerType.BLOCK_CACHE_FILTER_MISS))
    counter
  }

  def blockCacheFilterHit() = {
    val counter = newCounter("BLOCK_CACHE_FILTER_HIT")
    counter.set(ticker(TickerType.BLOCK_CACHE_FILTER_HIT))
    counter
  }

  def blockCacheDataMiss() = {
    val counter = newCounter("BLOCK_CACHE_DATA_MISS")
    counter.set(ticker(TickerType.BLOCK_CACHE_DATA_MISS))
    counter
  }

  def blockCacheDataHit() = {
    val counter = newCounter("BLOCK_CACHE_DATA_HIT")
    counter.set(ticker(TickerType.BLOCK_CACHE_DATA_HIT))
    counter
  }

  def bloomFilterUseful() = {
    val counter = newCounter("BLOOM_FILTER_USEFUL")
    counter.set(ticker(TickerType.BLOOM_FILTER_USEFUL))
    counter
  }

  def memtableHit() = {
    val counter = newCounter("MEMTABLE_HIT")
    counter.set(ticker(TickerType.MEMTABLE_HIT))
    counter
  }

  def memtableMiss() = {
    val counter = newCounter("MEMTABLE_MISS")
    counter.set(ticker(TickerType.MEMTABLE_MISS))
    counter
  }

  def getHitL0() = {
    val counter = newCounter("GET_HIT_L0")
    counter.set(ticker(TickerType.GET_HIT_L0))
    counter
  }

  def getHitL1() = {
    val counter = newCounter("GET_HIT_L1")
    counter.set(ticker(TickerType.GET_HIT_L1))
    counter
  }

  def getHitL2AndUp() = {
    val counter = newCounter("GET_HIT_L2_AND_UP")
    counter.set(ticker(TickerType.GET_HIT_L2_AND_UP))
    counter
  }

  def compactionKeyDropNewerEntry() = {
    val counter = newCounter("COMPACTION_KEY_DROP_NEWER_ENTRY")
    counter.set(ticker(TickerType.COMPACTION_KEY_DROP_NEWER_ENTRY))
    counter
  }

  def compactionKeyDropObsolete() = {
    val counter = newCounter("COMPACTION_KEY_DROP_OBSOLETE")
    counter.set(ticker(TickerType.COMPACTION_KEY_DROP_OBSOLETE))
    counter
  }

  def compactionKeyDropUser() = {
    val counter = newCounter("COMPACTION_KEY_DROP_USER")
    counter.set(ticker(TickerType.COMPACTION_KEY_DROP_USER))
    counter
  }

  def numberKeysWritten() = {
    val counter = newCounter("NUMBER_KEYS_WRITTEN")
    counter.set(ticker(TickerType.NUMBER_KEYS_WRITTEN))
    counter
  }

  def numberKeysRead() = {
    val counter = newCounter("NUMBER_KEYS_READ")
    counter.set(ticker(TickerType.NUMBER_KEYS_READ))
    counter
  }

  def numberKeysUpdated() = {
    val counter = newCounter("NUMBER_KEYS_UPDATED")
    counter.set(ticker(TickerType.NUMBER_KEYS_UPDATED))
    counter
  }

  def bytesWritten() = {
    val counter = newCounter("BYTES_WRITTEN")
    counter.set(ticker(TickerType.BYTES_WRITTEN))
    counter
  }

  def bytesRead() = {
    val counter = newCounter("BYTES_READ")
    counter.set(ticker(TickerType.BYTES_READ))
    counter
  }

  def noFileCloses() = {
    val counter = newCounter("NO_FILE_CLOSES")
    counter.set(ticker(TickerType.NO_FILE_CLOSES))
    counter
  }

  def noFileOpens() = {
    val counter = newCounter("NO_FILE_OPENS")
    counter.set(ticker(TickerType.NO_FILE_OPENS))
    counter
  }

  def noFileErrors() = {
    val counter = newCounter("NO_FILE_ERRORS")
    counter.set(ticker(TickerType.NO_FILE_ERRORS))
    counter
  }

  def stallL0SlowdownMicros() = {
    val counter = newCounter("STALL_L0_SLOWDOWN_MICROS")
    counter.set(ticker(TickerType.STALL_L0_SLOWDOWN_MICROS))
    counter
  }

  def stallMemtableCompactionMicros() = {
    val counter = newCounter("STALL_MEMTABLE_COMPACTION_MICROS")
    counter.set(ticker(TickerType.STALL_MEMTABLE_COMPACTION_MICROS))
    counter
  }

  def stallL0NumFilesMicros() = {
    val counter = newCounter("STALL_L0_NUM_FILES_MICROS")
    counter.set(ticker(TickerType.STALL_L0_NUM_FILES_MICROS))
    counter
  }

  def stallMicros() = {
    val counter = newCounter("STALL_MICROS")
    counter.set(ticker(TickerType.STALL_MICROS))
    counter
  }

  def dbMutexWaitMicros() = {
    val counter = newCounter("DB_MUTEX_WAIT_MICROS")
    counter.set(ticker(TickerType.DB_MUTEX_WAIT_MICROS))
    counter
  }

  def rateLimitDelayMillis() = {
    val counter = newCounter("RATE_LIMIT_DELAY_MILLIS")
    counter.set(ticker(TickerType.RATE_LIMIT_DELAY_MILLIS))
    counter
  }

  def noIterators() = {
    val counter = newCounter("NO_ITERATORS")
    counter.set(ticker(TickerType.NO_ITERATORS))
    counter
  }

  def numberMultigetCalls() = {
    val counter = newCounter("NUMBER_MULTIGET_CALLS")
    counter.set(ticker(TickerType.NUMBER_MULTIGET_CALLS))
    counter
  }

  def numberMultigetKeysRead() = {
    val counter = newCounter("NUMBER_MULTIGET_KEYS_READ")
    counter.set(ticker(TickerType.NUMBER_MULTIGET_KEYS_READ))
    counter
  }

  def numberMultigetBytesRead() = {
    val counter = newCounter("NUMBER_MULTIGET_BYTES_READ")
    counter.set(ticker(TickerType.NUMBER_MULTIGET_BYTES_READ))
    counter
  }

  def numberFilteredDeletes() = {
    val counter = newCounter("NUMBER_FILTERED_DELETES")
    counter.set(ticker(TickerType.NUMBER_FILTERED_DELETES))
    counter
  }

  def numberMergeFailures() = {
    val counter = newCounter("NUMBER_MERGE_FAILURES")
    counter.set(ticker(TickerType.NUMBER_MERGE_FAILURES))
    counter
  }

  def sequenceNumber() = {
    val counter = newCounter("SEQUENCE_NUMBER")
    counter.set(ticker(TickerType.SEQUENCE_NUMBER))
    counter
  }

  def bloomFilterPrefixChecked() = {
    val counter = newCounter("BLOOM_FILTER_PREFIX_CHECKED")
    counter.set(ticker(TickerType.BLOOM_FILTER_PREFIX_CHECKED))
    counter
  }

  def bloomFilterPrefixUseful() = {
    val counter = newCounter("BLOOM_FILTER_PREFIX_USEFUL")
    counter.set(ticker(TickerType.BLOOM_FILTER_PREFIX_USEFUL))
    counter
  }

  def numberOfReseeksInIteration() = {
    val counter = newCounter("NUMBER_OF_RESEEKS_IN_ITERATION")
    counter.set(ticker(TickerType.NUMBER_OF_RESEEKS_IN_ITERATION))
    counter
  }

  def getUpdatesSinceCalls() = {
    val counter = newCounter("GET_UPDATES_SINCE_CALLS")
    counter.set(ticker(TickerType.GET_UPDATES_SINCE_CALLS))
    counter
  }

  def blockCacheCompressedMiss() = {
    val counter = newCounter("BLOCK_CACHE_COMPRESSED_MISS")
    counter.set(ticker(TickerType.BLOCK_CACHE_COMPRESSED_MISS))
    counter
  }

  def blockCacheCompressedHit() = {
    val counter = newCounter("BLOCK_CACHE_COMPRESSED_HIT")
    counter.set(ticker(TickerType.BLOCK_CACHE_COMPRESSED_HIT))
    counter
  }

  def walFileSynced() = {
    val counter = newCounter("WAL_FILE_SYNCED")
    counter.set(ticker(TickerType.WAL_FILE_SYNCED))
    counter
  }

  def walFileBytes() = {
    val counter = newCounter("WAL_FILE_BYTES")
    counter.set(ticker(TickerType.WAL_FILE_BYTES))
    counter
  }

  def walDoneBySelf() = {
    val counter = newCounter("WRITE_DONE_BY_SELF")
    counter.set(ticker(TickerType.WRITE_DONE_BY_SELF))
    counter
  }

  def writeDoneByOther() = {
    val counter = newCounter("WRITE_DONE_BY_OTHER")
    counter.set(ticker(TickerType.WRITE_DONE_BY_OTHER))
    counter
  }

  def writeTimeout() = {
    val counter = newCounter("WRITE_TIMEDOUT")
    counter.set(ticker(TickerType.WRITE_TIMEDOUT))
    counter
  }

  def writeWithWal() = {
    val counter = newCounter("WRITE_WITH_WAL")
    counter.set(ticker(TickerType.WRITE_WITH_WAL))
    counter
  }

  def compactReadBytes() = {
    val counter = newCounter("COMPACT_READ_BYTES")
    counter.set(ticker(TickerType.COMPACT_READ_BYTES))
    counter
  }

  def compactWriteBytes() = {
    val counter = newCounter("COMPACT_WRITE_BYTES")
    counter.set(ticker(TickerType.COMPACT_WRITE_BYTES))
    counter
  }

  def flushWriteBytes() = {
    val counter = newCounter("FLUSH_WRITE_BYTES")
    counter.set(ticker(TickerType.FLUSH_WRITE_BYTES))
    counter
  }

  def numberDirectLoadTableProperties() = {
    val counter = newCounter("NUMBER_DIRECT_LOAD_TABLE_PROPERTIES")
    counter.set(ticker(TickerType.NUMBER_DIRECT_LOAD_TABLE_PROPERTIES))
    counter
  }

  def numberSupervisionAcquiries() = {
    val counter = newCounter("NUMBER_SUPERVERSION_ACQUIRES")
    counter.set(ticker(TickerType.NUMBER_SUPERVERSION_ACQUIRES))
    counter
  }

  def numberSupervisionReleases() = {
    val counter = newCounter("NUMBER_SUPERVERSION_RELEASES")
    counter.set(ticker(TickerType.NUMBER_SUPERVERSION_RELEASES))
    counter
  }

  def numberSupervisionCleanups() = {
    val counter = newCounter("NUMBER_SUPERVERSION_CLEANUPS")
    counter.set(ticker(TickerType.NUMBER_SUPERVERSION_CLEANUPS))
    counter
  }

  def numberBlockNotCompressed() = {
    val counter = newCounter("NUMBER_BLOCK_NOT_COMPRESSED")
    counter.set(ticker(TickerType.NUMBER_BLOCK_NOT_COMPRESSED))
    counter
  }

  def compactionOutfileSyncMicrosHistogram() = newGauge("COMPACTION_OUTFILE_SYNC_MICROS", histogram(HistogramType.COMPACTION_OUTFILE_SYNC_MICROS))
  def compactionTimeHistogram() = newGauge("COMPACTION_TIME", histogram(HistogramType.COMPACTION_TIME))
  def dbGetHistogram() = newGauge("DB_GET", histogram(HistogramType.DB_GET))
  def dbMultiGetHistogram() = newGauge("DB_MULTIGET", histogram(HistogramType.DB_MULTIGET))
  def dbSeekHistogram() = newGauge("DB_SEEK", histogram(HistogramType.DB_SEEK))
  def dbWriteHistogram() = newGauge("DB_WRITE", histogram(HistogramType.DB_WRITE))
  def hardRateLimitDelayCountHistogram() = newGauge("HARD_RATE_LIMIT_DELAY_COUNT", histogram(HistogramType.HARD_RATE_LIMIT_DELAY_COUNT))
  def manifestFileSyncMicrosHistogram() = newGauge("MANIFEST_FILE_SYNC_MICROS", histogram(HistogramType.MANIFEST_FILE_SYNC_MICROS))
  def numFilesInSingleCompactionHistogram() = newGauge("NUM_FILES_IN_SINGLE_COMPACTION", histogram(HistogramType.NUM_FILES_IN_SINGLE_COMPACTION))
  def readBlockCompactionMicrosHistogram() = newGauge("READ_BLOCK_COMPACTION_MICROS", histogram(HistogramType.READ_BLOCK_COMPACTION_MICROS))
  def readBlockGetMicrosHistogram() = newGauge("READ_BLOCK_GET_MICROS", histogram(HistogramType.READ_BLOCK_GET_MICROS))
  def softRateLimitDelayCountHistogram() = newGauge("SOFT_RATE_LIMIT_DELAY_COUNT", histogram(HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT))
  def stallL0NumFilesCountHistogram() = newGauge("STALL_L0_NUM_FILES_COUNT", histogram(HistogramType.STALL_L0_NUM_FILES_COUNT))
  def stallL0SlowdownCountHistogram() = newGauge("STALL_L0_SLOWDOWN_COUNT", histogram(HistogramType.STALL_L0_SLOWDOWN_COUNT))
  def stallMemtableCompactionCountHistogram() = newGauge("STALL_MEMTABLE_COMPACTION_COUNT", histogram(HistogramType.STALL_MEMTABLE_COMPACTION_COUNT))
  def tableOpenIOMicrosHistogram() = newGauge("TABLE_OPEN_IO_MICROS", histogram(HistogramType.TABLE_OPEN_IO_MICROS))
  def tableSyncMicrosHistogram() = newGauge("TABLE_SYNC_MICROS", histogram(HistogramType.TABLE_SYNC_MICROS))
  def walFileSyncMicrosHistogram() = newGauge("WAL_FILE_SYNC_MICROS", histogram(HistogramType.WAL_FILE_SYNC_MICROS))
  def writeRawBlockMicrosHistogram() = newGauge("WRITE_RAW_BLOCK_MICROS", histogram(HistogramType.WRITE_RAW_BLOCK_MICROS))
  def writeStallHistogram() = newGauge("WRITE_STALL", histogram(HistogramType.WRITE_STALL))
}
