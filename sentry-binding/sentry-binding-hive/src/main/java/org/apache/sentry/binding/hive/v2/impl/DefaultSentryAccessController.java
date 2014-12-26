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
package org.apache.sentry.binding.hive.v2.impl;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.SentryAccessController;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;

public class DefaultSentryAccessController extends SentryAccessController {

  public DefaultSentryAccessController(HiveAuthzConf authzConf,
      HiveAuthzBinding hiveAuthzBinding,
      HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx)
      throws Exception {
    super(authzConf, hiveAuthzBinding, authenticator, ctx);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
      throws SentryAccessControlException {
    // TODO Auto-generated method stub

  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
      throws SentryAccessControlException {
    // TODO Auto-generated method stub

  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws SentryAccessControlException {
    // TODO Auto-generated method stub

  }

  @Override
  public void dropRole(String roleName) throws SentryAccessControlException {
    // TODO Auto-generated method stub

  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc)
      throws SentryAccessControlException {
    // TODO Auto-generated method stub

  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals,
      List<String> roles, boolean grantOption, HivePrincipal grantorPrinc)
      throws SentryAccessControlException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<String> getAllRoles() throws SentryAccessControlException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
      HivePrivilegeObject privObj) throws SentryAccessControlException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setCurrentRole(String roleName)
      throws SentryAccessControlException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<String> getCurrentRoleNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws SentryAccessControlException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(
      HivePrincipal principal) throws SentryAccessControlException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
    // TODO Auto-generated method stub

  }

}
