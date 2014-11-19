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

import java.util.UUID;

import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores the UUID associated with the server who processed
 * a commit and a commit order sequence id.
 */
public class CommitContextFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryPolicyStoreProcessor.class);

  private boolean haEnabled = false;
  private DistributedCounter counter;

  public CommitContextFactory(HAContext haContext) {
    if (haContext != null) {
      this.haEnabled = true;
      this.counter = new DistributedCounter(haContext);
    }
  }

  public CommitContext getCommitContext(UUID serverUUID, long sequenceId) {
    if (haEnabled) {
      try {
        return new CommitContext(serverUUID, counter.getGlobalSequenceId());
      } catch (Exception e) {
        LOGGER.error("Error in get global sequence Id, return the sequenceId in service!" ,e);
        return new CommitContext(serverUUID, sequenceId);
      }
    } else {
      return new CommitContext(serverUUID, sequenceId);
    }
  }

  public static class DistributedCounter {

    private DistributedAtomicLong count;

    public DistributedCounter(HAContext haContext) {
      if (haContext.getCuratorFramework().getState() != CuratorFrameworkState.STARTED) {
        haContext.getCuratorFramework().start();
      }
      String counterPath = haContext.getNamespace() + "/"
          + HAContext.SENTRY_SEQUENCE_ID_COUNTER_NAMESPACE;
      count = new DistributedAtomicLong(haContext.getCuratorFramework(),
          counterPath, haContext.getRetryPolicy());
    }

    public long getGlobalSequenceId() throws Exception {
      return count.add((long) 1).postValue();
    }

  }
}
