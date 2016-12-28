/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

/**
 * @author Andrey Lomakin
 * @since 12.06.13
 */
public class OStorageTransaction {
  private final OTransaction clientTx;

  private boolean                                   preCommitted                 = false;
  private Iterable<ORecordOperation>                preCommittedEntries          = null;
  private TreeMap<String, OTransactionIndexChanges> preCommittedIndexesToCommit  = null;
  private List<Lock[]>                              preCommittedIndexKeyLockList = null;
  private List<ORecordOperation>                    preCommittedResult           = null;

  public OStorageTransaction(OTransaction clientTx) {
    this.clientTx = clientTx;
  }

  public OTransaction getClientTx() {
    return clientTx;
  }

  public boolean isPreCommitted() {
    return preCommitted;
  }

  public void setPreCommitted(boolean preCommitted) {
    this.preCommitted = preCommitted;
  }

  public Iterable<ORecordOperation> getPreCommittedEntries() {
    return preCommittedEntries;
  }

  public void setPreCommittedEntries(Iterable<ORecordOperation> preCommittedEntries) {
    this.preCommittedEntries = preCommittedEntries;
  }

  public TreeMap<String, OTransactionIndexChanges> getPreCommittedIndexesToCommit() {
    return preCommittedIndexesToCommit;
  }

  public void setPreCommittedIndexesToCommit(TreeMap<String, OTransactionIndexChanges> preCommittedIndexesToCommit) {
    this.preCommittedIndexesToCommit = preCommittedIndexesToCommit;
  }

  public List<Lock[]> getPreCommittedIndexKeyLockList() {
    return preCommittedIndexKeyLockList;
  }

  public void setPreCommittedIndexKeyLockList(List<Lock[]> preCommittedIndexKeyLockList) {
    this.preCommittedIndexKeyLockList = preCommittedIndexKeyLockList;
  }

  public List<ORecordOperation> getPreCommittedResult() {
    return preCommittedResult;
  }

  public void setPreCommittedResult(List<ORecordOperation> preCommittedResult) {
    this.preCommittedResult = preCommittedResult;
  }
}
