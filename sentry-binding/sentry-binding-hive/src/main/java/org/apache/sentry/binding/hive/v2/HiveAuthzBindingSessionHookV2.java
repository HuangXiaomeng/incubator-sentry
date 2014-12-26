/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.binding.hive.v2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;

public class HiveAuthzBindingSessionHookV2
    implements org.apache.hive.service.cli.session.HiveSessionHook {

  /**
   * The session hook for sentry authorization that sets the required session level configuration
   * 1. Setup the sentry hooks -
   *    semantic, exec and filter hooks
   * 2. Set additional config properties required for auth
   *      set HIVE_EXTENDED_ENITITY_CAPTURE = true
   *      set SCRATCHDIRPERMISSION = 700
   * 3. Add sensetive config parameters to the config restrict list so that they can't be overridden by users
   */
  @Override
  public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
    // Add sentry hooks to the session configuration
    HiveConf sessionConf = sessionHookContext.getSessionConf();

    // enable sentry authorization V2
    sessionConf.setBoolean(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, true);
    sessionConf.setBoolean(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, false);
    sessionConf.set(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER.varname,
        "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator");
    sessionConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, SentryAuthorizerFactory.class.getName());

    // set user name
    sessionConf.set(HiveAuthzConf.HIVE_ACCESS_SUBJECT_NAME, sessionHookContext.getSessionUser());
    sessionConf.set(HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME, sessionHookContext.getSessionUser());

    // Set MR ACLs to session user
    appendConfVar(sessionConf, JobContext.JOB_ACL_VIEW_JOB,
        sessionHookContext.getSessionUser());
    appendConfVar(sessionConf, JobContext.JOB_ACL_MODIFY_JOB,
        sessionHookContext.getSessionUser());
  }

  // Setup given sentry hooks
  private void appendConfVar(HiveConf sessionConf, String confVar,
      String sentryConfVal) {
    String currentValue = sessionConf.get(confVar, "").trim();
    if (currentValue.isEmpty()) {
      currentValue = sentryConfVal;
    } else {
      currentValue = sentryConfVal + "," + currentValue;
    }
    sessionConf.set(confVar, currentValue);
  }
}
