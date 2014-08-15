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
package org.apache.sentry.binding.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveAuthzBindingHookContext implements HiveSemanticAnalyzerHookContext {
  Configuration conf;
  Set<ReadEntity> inputs = null;
  Set<WriteEntity> outputs = null;
  Map<Table, List<String>> tab2Cols = null;
  Map<Partition, List<String>> part2Cols = null;
  private String userName;
  private String ipAddr;
  private String command;

  @Override
  public Hive getHive() throws HiveException {

    return Hive.get((HiveConf)conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void update(BaseSemanticAnalyzer sem) {
    this.inputs = sem.getInputs();
    this.outputs = sem.getOutputs();

    if (inputs != null && !inputs.isEmpty()) {
      try {
        getCols(sem);
      } catch (HiveException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  @Override
  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  @Override
  public String getIpAddress() {
    return ipAddr;
  }

  @Override
  public void setIpAddress(String ipAddress) {
    this.ipAddr = ipAddress;
  }

  @Override
  public String getCommand() {
    return command;
  }

  @Override
  public void setCommand(String command) {
    this.command = command;
  }

  public boolean hasColumns() {
    return ((tab2Cols != null && !tab2Cols.isEmpty())
        || (part2Cols != null && !part2Cols.isEmpty()));
  }

  public Map<Table, List<String>> getTab2Cols() {
    return tab2Cols;
  }

  public Map<Partition, List<String>> getPart2Cols() {
    return part2Cols;
  }

  private void getCols(BaseSemanticAnalyzer sem) throws HiveException {
    SessionState ss = SessionState.get();
    HiveOperation op = ss.getHiveOperation();
    if (op.equals(HiveOperation.CREATETABLE_AS_SELECT)
        || op.equals(HiveOperation.QUERY)) {
      tab2Cols = new HashMap<Table, List<String>>();
      part2Cols = new HashMap<Partition, List<String>>();

      //determine if partition level privileges should be checked for input tables
      Map<String, Boolean> tableUsePartLevelAuth = new HashMap<String, Boolean>();
      for (ReadEntity read : inputs) {
        if (read.isDummy() || read.getType() == org.apache.hadoop.hive.ql.hooks.Entity.Type.DATABASE) {
          continue;
        }
        Table tbl = read.getTable();
        if ((read.getPartition() != null) || (tbl != null && tbl.isPartitioned())) {
          String tblName = tbl.getTableName();
          if (tableUsePartLevelAuth.get(tblName) == null) {
            boolean usePartLevelPriv = (tbl.getParameters().get(
                "PARTITION_LEVEL_PRIVILEGE") != null && ("TRUE"
                .equalsIgnoreCase(tbl.getParameters().get(
                    "PARTITION_LEVEL_PRIVILEGE"))));
            if (usePartLevelPriv) {
              tableUsePartLevelAuth.put(tblName, Boolean.TRUE);
            } else {
              tableUsePartLevelAuth.put(tblName, Boolean.FALSE);
            }
          }
        }
      }
      getTablePartitionUsedColumns(sem, tab2Cols, part2Cols, tableUsePartLevelAuth);
    }
  }

  private void getTablePartitionUsedColumns(BaseSemanticAnalyzer sem,
      Map<Table, List<String>> tab2Cols, Map<Partition, List<String>> part2Cols,
      Map<String, Boolean> tableUsePartLevelAuth) throws HiveException {
    // for a select or create-as-select query, populate the partition to column
    // (par2Cols) or
    // table to columns mapping (tab2Cols)
    SemanticAnalyzer querySem = (SemanticAnalyzer) sem;
    ParseContext parseCtx = querySem.getParseContext();
    Map<TableScanOperator, Table> tsoTopMap = parseCtx.getTopToTable();

    for (Map.Entry<String, Operator<? extends OperatorDesc>> topOpMap : querySem
        .getParseContext().getTopOps().entrySet()) {
      Operator<? extends OperatorDesc> topOp = topOpMap.getValue();
      if (topOp instanceof TableScanOperator
          && tsoTopMap.containsKey(topOp)) {
        TableScanOperator tableScanOp = (TableScanOperator) topOp;
        Table tbl = tsoTopMap.get(tableScanOp);
        List<Integer> neededColumnIds = tableScanOp.getNeededColumnIDs();
        List<FieldSchema> columns = tbl.getCols();
        List<String> cols = new ArrayList<String>();
        for (int i = 0; i < neededColumnIds.size(); i++) {
          cols.add(columns.get(neededColumnIds.get(i)).getName());
        }
        //map may not contain all sources, since input list may have been optimized out
        //or non-existent tho such sources may still be referenced by the TableScanOperator
        //if it's null then the partition probably doesn't exist so let's use table permission
        if (tbl.isPartitioned() &&
            Boolean.TRUE.equals(tableUsePartLevelAuth.get(tbl.getTableName()))) {
          String alias_id = topOpMap.getKey();

          PrunedPartitionList partsList = PartitionPruner.prune(tableScanOp,
              parseCtx, alias_id);
          Set<Partition> parts = partsList.getPartitions();
          for (Partition part : parts) {
            List<String> existingCols = part2Cols.get(part);
            if (existingCols == null) {
              existingCols = new ArrayList<String>();
            }
            existingCols.addAll(cols);
            part2Cols.put(part, existingCols);
          }
        } else {
          List<String> existingCols = tab2Cols.get(tbl);
          if (existingCols == null) {
            existingCols = new ArrayList<String>();
          }
          existingCols.addAll(cols);
          tab2Cols.put(tbl, existingCols);
        }
      }
    }
  }
}
