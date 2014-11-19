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
import static junit.framework.Assert.assertTrue;

import java.util.Set;

import javax.jdo.JDOHelper;
import javax.jdo.JDOOptimisticVerificationException;

import org.apache.curator.test.TestingServer;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHASentryStore extends TestSentryStore {

  private TestingServer server;

  @Before
  public void setup() throws Exception {
    server = new TestingServer();
    conf.set(ServerConfig.SENTRY_HA_ENABLED, "true");
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_NAMESPACE, "/sentry-test");
    conf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM, server.getConnectString());
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
    try {
      server.close();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void testRolePrivilegeWithVersionLock() throws Exception {
    String roleName1 = "test-version-role";
    String grantor = "g1";
    sentryStore.createSentryRole(roleName1);

    TSentryPrivilege privilege_tbl1 = new TSentryPrivilege();
    privilege_tbl1.setPrivilegeScope("TABLE");
    privilege_tbl1.setServerName("server1");
    privilege_tbl1.setDbName("db1");
    privilege_tbl1.setTableName("tbl1");
    privilege_tbl1.setCreateTime(System.currentTimeMillis());

    TSentryPrivilege privilege_tbl1_insert = new TSentryPrivilege(
        privilege_tbl1);
    privilege_tbl1_insert.setAction("INSERT");

    TSentryPrivilege privilege_tbl2_all = new TSentryPrivilege(privilege_tbl1);
    privilege_tbl2_all.setTableName("tbl2");
    privilege_tbl2_all.setAction("*");


    MSentryRole mRole0 = sentryStore.getMSentryRoleByName(roleName1);
    Set privileges = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(0, privileges.size());
    long version0 = (Long) JDOHelper.getVersion(mRole0);
    assertEquals(1,version0);

    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl1_insert);
    privileges = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(1, privileges.size());

    MSentryRole mRole1 = sentryStore.getMSentryRoleByName(roleName1);
    long version1 = (Long) JDOHelper.getVersion(mRole1);
    version0 = (Long) JDOHelper.getVersion(mRole0);
    assertTrue(1 == version0);
    assertTrue(2 == version1);

    try{
      sentryStore.alterSentryRoleGrantPrivilege(grantor, mRole0, privilege_tbl2_all);
      throw new Exception("Here should fail");
    } catch (JDOOptimisticVerificationException e) {
      // should fail by JDOOptimisticVerificationException.
      if ( !(e instanceof JDOOptimisticVerificationException)) {
        throw new Exception("Here should be JDOOptimisticVerificationException", e);
      }
    }
    privileges = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(1, privileges.size());
    sentryStore.alterSentryRoleGrantPrivilege(grantor, roleName1, privilege_tbl2_all);
    privileges = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(2, privileges.size());
    MSentryRole mRole2 = sentryStore.getMSentryRoleByName(roleName1);
    long version2 = (Long) JDOHelper.getVersion(mRole2);
    version1 = (Long) JDOHelper.getVersion(mRole1);
    version0 = (Long) JDOHelper.getVersion(mRole0);
    assertTrue(0 < version0);
    assertTrue(version0 < version1);
    assertTrue(version1 < version2);

    sentryStore.dropPrivilege(toTSentryAuthorizable(privilege_tbl1));
    MSentryRole mRole3 = sentryStore.getMSentryRoleByName(roleName1);
    privileges = sentryStore.getAllTSentryPrivilegesByRoleName(roleName1);
    assertEquals(1, privileges.size());
    long version3 = (Long) JDOHelper.getVersion(mRole3);
    version2 = (Long) JDOHelper.getVersion(mRole2);
    version1 = (Long) JDOHelper.getVersion(mRole1);
    version0 = (Long) JDOHelper.getVersion(mRole0);
    assertTrue(0 < version0);
    assertTrue(version0 < version1);
    assertTrue(version1 < version2);
    assertTrue(version2 < version3);

  }
}
