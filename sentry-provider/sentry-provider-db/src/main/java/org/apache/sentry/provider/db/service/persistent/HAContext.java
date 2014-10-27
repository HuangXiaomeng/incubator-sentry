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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

/**
 * Stores the HA related context
 */
public class HAContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(HAContext.class);

  public final static String SENTRY_SERVICE_REGISTER_NAMESPACE = "sentry-service";
  private final String zookeeperQuorum;
  private final int sessionTimeout;
  private final String namespace;

  private final CuratorFramework curatorFramework;
  private final RetryPolicy retryPolicy;

  public HAContext(Configuration conf) throws Exception {
    this.zookeeperQuorum = conf.get(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM,
        ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM_DEFAULT);
    this.sessionTimeout = conf.getInt(ServerConfig.SENTRY_HA_ZOOKEEPER_SESSION_TIMEOUT,
        ServerConfig.SENTRY_HA_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
    this.namespace = conf.get(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE,
        ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE_DEFAULT);
    validateConf();
    retryPolicy = new RetryNTimes(5, sessionTimeout);
    this.curatorFramework = CuratorFrameworkFactory.builder()
        .namespace(this.namespace)
        .connectString(this.zookeeperQuorum)
        .retryPolicy(retryPolicy)
        .build();
  }

  public CuratorFramework getCuratorFramework() {
    return this.curatorFramework;
  }

  public String getZookeeperQuorum() {
    return zookeeperQuorum;
  }

  public String getNamespace() {
    return namespace;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  private void validateConf() {
    Preconditions.checkNotNull(zookeeperQuorum, "Zookeeper Quorum should not be null.");
    Preconditions.checkNotNull(namespace, "Zookeeper namespace should not be null.");
  }

}
