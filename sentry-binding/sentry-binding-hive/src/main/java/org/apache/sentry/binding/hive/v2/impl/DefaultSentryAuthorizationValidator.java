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

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding.HiveHook;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.v2.SentryAuthorizationValidator;
import org.apache.sentry.binding.hive.v2.util.SentryAccessControlException;
import org.apache.sentry.binding.hive.v2.util.SentryAuthorizerUtil;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;

public class DefaultSentryAuthorizationValidator extends SentryAuthorizationValidator {

  public static final Log LOG = LogFactory.getLog(DefaultSentryAuthorizationValidator.class);

  public DefaultSentryAuthorizationValidator(HiveAuthzConf authzConf, HiveAuthzBinding hiveAuthzBinding,
      HiveAuthenticationProvider authenticator) throws Exception {
    super(authzConf, hiveAuthzBinding, authenticator);
  }

  public DefaultSentryAuthorizationValidator(HiveHook hiveHook, HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    this(authzConf, new HiveAuthzBinding(hiveHook, conf, authzConf), authenticator);
  }

  public DefaultSentryAuthorizationValidator(HiveConf conf, HiveAuthzConf authzConf,
      HiveAuthenticationProvider authenticator) throws Exception {
    this(HiveHook.HiveServer2, conf, authzConf, authenticator);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType,
      List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs,
      HiveAuthzContext context) throws SentryAccessControlException {
    if (LOG.isDebugEnabled()) {
      String msg = "Checking privileges for operation " + hiveOpType + " by user "
          + authenticator.getUserName() + " on " + " input objects " + inputHObjs
          + " and output objects " + outputHObjs + ". Context Info: " + context;
      LOG.debug(msg);
    }

    HiveOperation hiveOp = SentryAuthorizerUtil.convert2HiveOperation(hiveOpType);
    HiveAuthzPrivileges stmtAuthPrivileges = HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(hiveOp);
    if (stmtAuthPrivileges == null) {
      // We don't handle authorizing this statement
      return;
    }

    List<List<DBModelAuthorizable>> inputHierarchyList =
        SentryAuthorizerUtil.convert2SentryPrivilegeList(hiveAuthzBinding.getAuthServer(), inputHObjs);
    List<List<DBModelAuthorizable>> outputHierarchyList =
        SentryAuthorizerUtil.convert2SentryPrivilegeList(hiveAuthzBinding.getAuthServer(), outputHObjs);

    // workaroud for metadata queries
    addExtendHierarchy(hiveOp, stmtAuthPrivileges, inputHierarchyList,
        outputHierarchyList, context.getCommandString());

    try {
      hiveAuthzBinding.authorize(hiveOp, stmtAuthPrivileges, new Subject(authenticator.getUserName()),
          inputHierarchyList, outputHierarchyList);
    } catch (AuthorizationException e) {
      throw new SentryAccessControlException(e);
    }
  }

  private void addExtendHierarchy(HiveOperation hiveOp, HiveAuthzPrivileges stmtAuthPrivileges,
      List<List<DBModelAuthorizable>> inputHierarchyList,
      List<List<DBModelAuthorizable>> outputHierarchyList,
      String command) throws SentryAccessControlException {
    String currDatabase = null;
    switch (stmtAuthPrivileges.getOperationScope()) {
      case SERVER:
        // validate server level privileges if applicable. Eg create UDF,register jar etc ..
        List<DBModelAuthorizable> serverHierarchy = new ArrayList<DBModelAuthorizable>();
        serverHierarchy.add(hiveAuthzBinding.getAuthServer());
        inputHierarchyList.add(serverHierarchy);
        break;
      case DATABASE:
        // workaround for metadata queries.
        if (hiveOp.equals(HiveOperation.DROPDATABASE)
            || hiveOp.equals(HiveOperation.CREATETABLE)
            || hiveOp.equals(HiveOperation.IMPORT)
            || hiveOp.equals(HiveOperation.DESCDATABASE)
            || hiveOp.equals(HiveOperation.ALTERTABLE_RENAME)) {
          SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(hiveOp, command);
          currDatabase = analyzer.getCurrentDb();

          List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
          externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
          externalAuthorizableHierarchy.add(new Database(currDatabase));

          if (hiveOp.equals(HiveOperation.DROPDATABASE)
              || hiveOp.equals(HiveOperation.DESCDATABASE)
              || hiveOp.equals(HiveOperation.ALTERTABLE_RENAME)) {
            inputHierarchyList.add(externalAuthorizableHierarchy);
          } else {
            outputHierarchyList.add(externalAuthorizableHierarchy);
          }
        }
        break;
      case TABLE:
        // workaround for drop table/view.
        if (hiveOp.equals(HiveOperation.DROPTABLE)
            || hiveOp.equals(HiveOperation.DROPVIEW)
            || hiveOp.equals(HiveOperation.DESCTABLE)
            // show
            || hiveOp.equals(HiveOperation.SHOW_TBLPROPERTIES)
            || hiveOp.equals(HiveOperation.SHOWINDEXES)
            // alter table
            || hiveOp.equals(HiveOperation.ALTERTABLE_RENAMEPART)
            || hiveOp.equals(HiveOperation.ALTERPARTITION_SERDEPROPERTIES)
            || hiveOp.equals(HiveOperation.ALTERPARTITION_FILEFORMAT)
            || hiveOp.equals(HiveOperation.ALTERTABLE_TOUCH)
            || hiveOp.equals(HiveOperation.ALTERPARTITION_PROTECTMODE)
            || hiveOp.equals(HiveOperation.MSCK)
            // index
            || hiveOp.equals(HiveOperation.ALTERINDEX_REBUILD)) {
          SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(hiveOp, command);
          currDatabase = analyzer.getCurrentDb();
          String currTable = analyzer.getCurrentTb();

          List<DBModelAuthorizable> externalAuthorizableHierarchy = new ArrayList<DBModelAuthorizable>();
          externalAuthorizableHierarchy.add(hiveAuthzBinding.getAuthServer());
          externalAuthorizableHierarchy.add(new Database(currDatabase));
          externalAuthorizableHierarchy.add(new Table(currTable));

          if (hiveOp.equals(HiveOperation.ALTERTABLE_RENAMEPART)
              || hiveOp.equals(HiveOperation.ALTERPARTITION_SERDEPROPERTIES)
              || hiveOp.equals(HiveOperation.ALTERPARTITION_FILEFORMAT)
              || hiveOp.equals(HiveOperation.ALTERTABLE_TOUCH)
              || hiveOp.equals(HiveOperation.ALTERPARTITION_PROTECTMODE)
              || hiveOp.equals(HiveOperation.MSCK)) {
            outputHierarchyList.add(externalAuthorizableHierarchy);
          } else {
            inputHierarchyList.add(externalAuthorizableHierarchy);
          }
        }
        break;
      case URI:
        if (hiveOp.equals(HiveOperation.CREATEFUNCTION)) {
          SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(hiveOp, command);
          currDatabase = analyzer.getCurrentDb();
          String udfClassName = analyzer.getCurrentTb();
          try {
            CodeSource udfSrc = Class.forName(udfClassName).getProtectionDomain().getCodeSource();
            if (udfSrc == null) {
              throw new SentryAccessControlException("Could not resolve the jar for UDF class " + udfClassName);
            }
            String udfJar = udfSrc.getLocation().getPath();
            if (udfJar == null || udfJar.isEmpty()) {
              throw new SentryAccessControlException("Could not find the jar for UDF class " + udfClassName +
                  "to validate privileges");
            }
            AccessURI udfURI = SentryAuthorizerUtil.parseURI(udfSrc.getLocation().toString(), true);
            List<DBModelAuthorizable> udfUriHierarchy = new ArrayList<DBModelAuthorizable>();
            udfUriHierarchy.add(hiveAuthzBinding.getAuthServer());
            udfUriHierarchy.add(udfURI);
            inputHierarchyList.add(udfUriHierarchy);
          } catch (Exception e) {
            throw new SentryAccessControlException("Error retrieving udf class", e);
          }
        }
        break;
      case CONNECT:
        /* The 'CONNECT' is an implicit privilege scope currently used for
         *  - USE <db>
         *  It's allowed when the user has any privilege on the current database. For application
         *  backward compatibility, we allow (optional) implicit connect permission on 'default' db.
         */
        List<DBModelAuthorizable> connectHierarchy = new ArrayList<DBModelAuthorizable>();
        connectHierarchy.add(hiveAuthzBinding.getAuthServer());
        if (hiveOp.equals(HiveOperation.SWITCHDATABASE)) {
          currDatabase = command.split(" ")[1];
        }
        // by default allow connect access to default db
        Table currTbl = Table.ALL;
        Database currDB = new Database(currDatabase);
        Column currCol = Column.ALL;
        if ((DEFAULT_DATABASE_NAME.equalsIgnoreCase(currDatabase) &&
            "false".equalsIgnoreCase(authzConf.
                get(HiveAuthzConf.AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB.getVar(), "false")))) {
          currDB = Database.ALL;
          currTbl = Table.SOME;
        }

        connectHierarchy.add(currDB);
        connectHierarchy.add(currTbl);
        connectHierarchy.add(currCol);

        inputHierarchyList.add(connectHierarchy);
        break;
    }
  }
}
