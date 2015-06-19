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


import java.io.File

import org.apache.samza.config.MapConfig
import org.apache.samza.util.ExponentialSleepStrategy
import org.junit.{Assert, Test}
import org.rocksdb.{HistogramType, Options, TickerType}

class TestRocksDbKeyValueStore
{
  @Test
  def testTTL() {
    val map = new java.util.HashMap[String, String]();
    map.put("rocksdb.ttl.ms", "1000")
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)
    val rocksDB = RocksDbKeyValueStore.openDB(new File(System.getProperty("java.io.tmpdir")),
                                              options,
                                              config,
                                              false,
                                              "someStore")
    val key = "test".getBytes("UTF-8")
    rocksDB.put(key, "val".getBytes("UTF-8"))
    Assert.assertNotNull(rocksDB.get(key))
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy
    retryBackoff.run(loop => {
      if(rocksDB.get(key) == null) {
        loop.done
      }
      rocksDB.compactRange()
    }, (exception, loop) => {
      exception match {
        case e: Exception =>
          loop.done
          throw e
      }
    })
    Assert.assertNull(rocksDB.get(key))
    rocksDB.close()
  }

  @Test
  def testStatistic() {
    val numberOfOperations: Int = 1000
    val options = new Options()
    options.setCreateIfMissing(true).createStatistics()
    val rocksDB = RocksDbKeyValueStore.openDB(new File(System.getProperty("java.io.tmpdir")),
      options,
      new MapConfig(),
      false,
      "someStore")

    for (key <- 1 to numberOfOperations) {
      rocksDB.put(("k" + key).getBytes("UTF-8"), "a".getBytes("UTF-8"))
      rocksDB.get(("k" + key).getBytes("UTF-8"))
    }

    Thread.sleep(1000)

    val stat = options.statisticsPtr()

    Assert.assertEquals(stat.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN), numberOfOperations)
    Assert.assertEquals(stat.getTickerCount(TickerType.NUMBER_KEYS_READ), numberOfOperations)
    Assert.assertTrue(stat.getTickerCount(TickerType.BYTES_WRITTEN) > 0)
    Assert.assertTrue(stat.geHistogramData(HistogramType.DB_GET).getAverage > 0)
    Assert.assertTrue(stat.geHistogramData(HistogramType.DB_GET).getMedian > 0)
    Assert.assertTrue(stat.geHistogramData(HistogramType.DB_GET).getPercentile95 > 0)
    Assert.assertTrue(stat.geHistogramData(HistogramType.DB_GET).getPercentile99 > 0)
    Assert.assertTrue(stat.geHistogramData(HistogramType.DB_GET).getStandardDeviation > 0)

    for (tt <- TickerType.values()) {
      println(">> Ticker statistic - " + tt.name() + ": " + stat.getTickerCount(tt))
    }
    for (hist <- HistogramType.values()) {
      println(">> Hist statistic - " + hist.name() + " Avarage: " + stat.geHistogramData(hist).getAverage)
      println(">> Hist statistic - " + hist.name() + " Median: " + stat.geHistogramData(hist).getMedian)
      println(">> Hist statistic - " + hist.name() + " Percentile95: " + stat.geHistogramData(hist).getPercentile95)
      println(">> Hist statistic - " + hist.name() + " Percentile99: " + stat.geHistogramData(hist).getPercentile99)
      println(">> Hist statistic - " + hist.name() + " Standard Deviation: " + stat.geHistogramData(hist).getStandardDeviation)
    }
    rocksDB.close()
  }
}
