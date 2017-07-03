/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive.thriftserver.ha

import java.nio.charset.Charset

import scala.util.Random

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hive.jdbc.ZooKeeperHiveClientException
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.internal.Logging

/**
 * This is a optional user api for ThriftServer HA client
 * to pick a thriftserver randomly from zookeeper with giving zkQuorum and zkNamespace
 * Moreover, you can use hive jdbc driver directly!
 * Created by jinjuting on 2017/6/30-10:22.
 */
private[hive] class ThriftServerHAClient(zkQuorum: String, zkNamespace: String)
  extends HiveServer2 with Logging{
  var jdbcURL = ""
  private val zooKeeperClient = CuratorFrameworkFactory
    .builder
    .connectString(zkQuorum)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .build
  zooKeeperClient.start()
  try {
    val serverHosts = zooKeeperClient.getChildren().forPath("/" + zkNamespace)
    if (serverHosts.isEmpty()) {
      throw new ZooKeeperHiveClientException(
        "There is no existing ThriftServer uris from ZooKeeper.")
    }
    val serverNode = serverHosts.get(Random.nextInt(serverHosts.size()))
    jdbcURL = new String(
      zooKeeperClient.getData().forPath("/" + zkNamespace + "/" + serverNode),
      Charset.forName("UTF-8"))
    logInfo("Selected ThriftServer instance with uri: " + jdbcURL)
  } catch {
    case e: Exception =>
      throw new ZooKeeperHiveClientException("Unable to read ThriftServer uri from ZooKeeper", e)
  } finally {
    if (zooKeeperClient != null) {
      zooKeeperClient.close()
    }
  }
}
