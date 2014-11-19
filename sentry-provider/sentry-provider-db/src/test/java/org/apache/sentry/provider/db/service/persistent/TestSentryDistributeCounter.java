/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.CommitContextFactory.DistributedCounter;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSentryDistributeCounter {

  private HAContext haContext;
  private TestingServer server;

  @Before
  public void setup() throws Exception {
    server = new TestingServer();
    // HA conf
    Configuration conf = new Configuration(false);
    conf.set(ServerConfig.SENTRY_HA_ENABLED, "true");
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE, "sentry-test");
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM, server.getConnectString());
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_RETRIES_MAX_COUNT, "0");
    haContext = new HAContext(conf);
  }

  @After
  public void teardown() {
    if (haContext != null) {
      CloseableUtils.closeQuietly(haContext.getCuratorFramework());
    }
    if (server != null) {
      try {
        server.stop();
      } catch (IOException e) {
      }
    }
  }

  @Test
  public void testDistributedCounter() throws Exception {
    DistributedCounter counter = new DistributedCounter(haContext);
    assertEquals(1, counter.getGlobalSequenceId());
    assertEquals(2, counter.getGlobalSequenceId());
    assertEquals(3, counter.getGlobalSequenceId());
  }

  @Test
  public void testCommitContextFactory() throws Exception {
    CommitContextFactory ccf = new CommitContextFactory(haContext);
    final int sequenceId = -1;
    assertEquals(1, ccf.getCommitContext(UUID.randomUUID(), sequenceId).getSequenceId());
    assertEquals(2, ccf.getCommitContext(UUID.randomUUID(), sequenceId).getSequenceId());
    assertEquals(3, ccf.getCommitContext(UUID.randomUUID(), sequenceId).getSequenceId());;
    int port = server.getPort();
    File file = server.getTempDirectory();
    server.stop();
    assertEquals(sequenceId, ccf.getCommitContext(UUID.randomUUID(), sequenceId).getSequenceId());
    assertEquals(sequenceId, ccf.getCommitContext(UUID.randomUUID(), sequenceId).getSequenceId());
    server = new TestingServer(port, file);
    assertEquals(4, ccf.getCommitContext(UUID.randomUUID(), sequenceId).getSequenceId());
  }

}
