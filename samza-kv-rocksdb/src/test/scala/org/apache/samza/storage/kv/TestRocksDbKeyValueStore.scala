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
import org.rocksdb.{WriteOptions, HistogramType, Options, TickerType}

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
    val storeName: String = "someStore"
    val tmpDir = new File(System.getProperty("java.io.tmpdir"))
    val rocksDB = new RocksDbKeyValueStore(tmpDir,
      options,
      new MapConfig(),
      false,
      storeName)

    for (key <- 1 to numberOfOperations) {
      rocksDB.put(("k" + key).getBytes("UTF-8"), "aeiou".getBytes("UTF-8"))
      rocksDB.get(("k" + key).getBytes("UTF-8"))
    }

    // Update statisctics
    rocksDB.flush

    Assert.assertEquals(numberOfOperations, rocksDB.getStatistic().numberKeysRead().getCount)
    Assert.assertEquals(numberOfOperations, rocksDB.getStatistic().numberKeysWritten().getCount)
    Assert.assertTrue(rocksDB.getStatistic().bytesWritten().getCount > 0)
    Assert.assertTrue(rocksDB.getStatistic().bytesRead().getCount > 0)
    Assert.assertTrue(rocksDB.getStatistic().dbGetHistogram().getValue.getAverage > 0)
    rocksDB.close()
  }

  @Test
  def testFlushStatistic() {
    val numberOfOperations: Int = 1000
    val numberOfOperationsAdded: Int = 3
    val options = new Options()
    val storeName: String = "someStore"
    val tmpDir = new File(System.getProperty("java.io.tmpdir"))
    val rocksDB = new RocksDbKeyValueStore(tmpDir,
      options,
      new MapConfig(),
      false,
      storeName)

    for (key <- 1 to numberOfOperations) {
      rocksDB.put(("k" + key).getBytes("UTF-8"), "aeiou".getBytes("UTF-8"))
      rocksDB.get(("k" + key).getBytes("UTF-8"))
    }

    rocksDB.flush

    val numberKeysRead = rocksDB.getStatistic().numberKeysRead().getCount
    val numberBytesWritten = rocksDB.getStatistic().numberKeysWritten().getCount
    val dbGetAverage = rocksDB.getStatistic().dbGetHistogram().getValue.getAverage

    Assert.assertEquals(numberOfOperations, numberKeysRead)
    Assert.assertEquals(numberOfOperations, numberBytesWritten)
    Assert.assertTrue(dbGetAverage > 0)



    for (key <- 1 to numberOfOperationsAdded) {
      rocksDB.put(("k" + key).getBytes("UTF-8"), "aeiou".getBytes("UTF-8"))
      rocksDB.get(("k" + key).getBytes("UTF-8"))
    }

    rocksDB.flush

    Assert.assertEquals(numberOfOperations + numberOfOperationsAdded, rocksDB.getStatistic().numberKeysRead().getCount)
    Assert.assertEquals(numberOfOperations + numberOfOperationsAdded, rocksDB.getStatistic().numberKeysWritten().getCount)
    Assert.assertTrue(rocksDB.getStatistic().bytesWritten().getCount > numberBytesWritten)
    Assert.assertTrue(rocksDB.getStatistic().bytesRead().getCount > numberKeysRead)
    Assert.assertTrue(rocksDB.getStatistic().dbGetHistogram().getValue.getAverage > dbGetAverage)

    rocksDB.close()
  }
}
