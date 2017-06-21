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

import java.io.IOException
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import org.apache.commons.logging.LogFactory
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hive.common.util.HiveVersionInfo
import org.apache.hive.service.server.HiveServer2
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging


/**
 * Kylin Spark ThriftServer Project, implement ThriftServer HA with zookeeper
 * Created by jinjuting on 2017/6/21-15:16.
 */
private[hive] class ThriftServerHA (sparkContext: SparkContext, initInstanceURI: String)
  extends HiveServer2 with Logging {
  var LOG = LogFactory.getLog(classOf[HiveServer2])

  private class DeRegisterWatcher(znode: PersistentEphemeralNode) extends Watcher {
    override def process(watchedEvent: WatchedEvent): Unit = {
      if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
        if (znode != null) {
          try {
            znode.close()
            logInfo("This HiveServer2 instance is now de-registered from ZooKeeper. "
              + "The server will be shut down after the last client sesssion completes.")
          } catch {
            case e: IOException => logError("Failed to close the persistent ephemeral znode")
          } finally {
            znode.close()
          }
        }
      }
    }
  }
  def addServerInstanceToZooKeeper(): Unit = {
    val zooKeeperEnsemble = sparkContext.getConf.get(
      "spark.thriftserver.zookeeper.quorum", "")
    val rootNamespace = sparkContext.getConf.get(
      "spark.thriftserver.zookeeper.namespace", "")
    val sessionTimeout = sparkContext.getConf.getInt(
      "spark.thriftserver.zookeeper.connection.session.timeout", 5000)
    val baseSleepTime = sparkContext.getConf.getInt(
      "spark.thriftserver.zookeeper.connection.basesleeptime", 1000)
    val maxRetries = sparkContext.getConf.getInt(
      "spark.thriftserver.zookeeper.connection.max.tries", 3)
    val instanceURI = initInstanceURI

    logInfo(
      "spark.thriftserver.zookeeper.quorum" +
        "has been set to: " + zooKeeperEnsemble)
    logInfo(
      "spark.thriftserver.zookeeper.namespace" +
        "has been set to: " + rootNamespace)
    logInfo(
      "spark.thriftserver.zookeeper.connection.session.timeout" +
        "has been set to: " + sessionTimeout)
    logInfo(
      "spark.thriftserver.zookeeper.connection.basesleeptime" +
        "has been set to: " + baseSleepTime)
    logInfo(
      "spark.thriftserver.zookeeper.connection.max.tries" +
        "has been set to: " + maxRetries)

    val zooKeeperClient: CuratorFramework =
      CuratorFrameworkFactory
        .builder()
        .connectString(zooKeeperEnsemble)
        .sessionTimeoutMs(sessionTimeout)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
        .build()
    zooKeeperClient.start()
    try{
      if (zooKeeperClient.getChildren.forPath("/").contains(rootNamespace)) {
        logWarning(
          "You have created the root name space: " +
            rootNamespace + " on ZooKeeper for HiveServer2")
      }
      else {
        zooKeeperClient
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath("/" + rootNamespace)
        logInfo("Created the root name space: " + rootNamespace + " on ZooKeeper for HiveServer2")
      }
    } catch {
      case e: KeeperException =>
        logError("Unable to create HiveServer2 namespace: " + rootNamespace + " on ZooKeeper")
        if (zooKeeperClient != null) {
          zooKeeperClient.close()
        }
    }

    val pathPrefix = "/" + rootNamespace + "/" +
      "serverUri=" +
      instanceURI + ";" +
      "version=" + HiveVersionInfo.getVersion() + ";" +
      "sequence="
    val znodeData = instanceURI
    val znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"))

    val znode: PersistentEphemeralNode = new PersistentEphemeralNode(
      zooKeeperClient, PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
      pathPrefix, znodeDataUTF8)
    znode.start()
    try{
      val znodeCreationTimeout = 120
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " +
          znodeCreationTimeout + "s exhausted")
      }
      val znodePath = znode.getActualPath()
      if (zooKeeperClient
        .checkExists()
        .usingWatcher(new DeRegisterWatcher(znode))
        .forPath(znodePath) == null) {
        throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.")
      }
      logInfo("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI)
    } catch {
      case e: Exception =>
        logError("Unable to create a znode for this server instance")
        // Close the client connection with ZooKeeper
        if (zooKeeperClient != null) {
          zooKeeperClient.close()
        }
        if (znode != null) {
          znode.close()
        }

    }
  }
}
