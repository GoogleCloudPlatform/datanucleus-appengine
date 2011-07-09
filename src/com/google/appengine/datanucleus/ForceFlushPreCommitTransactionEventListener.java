/*
 * Copyright (C) 2010 Max Ross.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus;

import org.datanucleus.TransactionEventListener;
import org.datanucleus.store.ObjectProvider;

/**
 * If an element is being added to a list outside of a txn
 * there is a chance that the insert of the element won't be triggered
 * in response to the update of its parent.  If the insert doesn't happen
 * then the parent won't know it needs to repersist itself to capture
 * the key of the newly added element.  So, we register a transaction
 * listener with the expectation that datanucleus is going to begin
 * and then immediately end a txn when the persistence manager is closed
 * in order to get all outstanding updates to flush.  Right before the
 * txn commits we're going to force the parent to repersist itself.
 * Dang this is hard.
 * 
 * This is basically wrong. If in non-tx context then the List is a wrapper
 * which informs the owning object of a change and is marked dirty. This plugin then
 * can take appropriate action. Hacks like this should be removed and address the
 * real problem. Where is a testcase that demonstrates the need for this? Andy 23/06/2011
 *
 * @author Max Ross <max.ross@gmail.com>
 */
class ForceFlushPreCommitTransactionEventListener implements TransactionEventListener {

  static final String ALREADY_PERSISTED_RELATION_KEYS_KEY =
      ForceFlushPreCommitTransactionEventListener.class.getName() + ".already_persisted_relation_keys";

  private final ObjectProvider op;

  ForceFlushPreCommitTransactionEventListener(ObjectProvider op) {
    this.op = op;
  }

  public void transactionStarted() {
  }

  public void transactionEnded() {
  }

  public void transactionFlushed() {
  }

  public void transactionPreCommit() {
    if (op.getObject() == null) {
      // Maybe the ObjectProvider was already processed and disconnected
      return;
    }

    DatastoreManager storeMgr = (DatastoreManager) op.getExecutionContext().getStoreManager();
    // make sure we only force the re-persist once
    if (storeMgr.storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS) &&
        op.getAssociatedValue(ALREADY_PERSISTED_RELATION_KEYS_KEY) == null) {
      op.setAssociatedValue(ALREADY_PERSISTED_RELATION_KEYS_KEY, true);
      for (int pos : op.getClassMetaData().getAllMemberPositions()) {
        op.makeDirty(pos);
      }
      op.flush();
    }
  }

  public void transactionCommitted() {
  }

  public void transactionPreRollBack() {
  }

  public void transactionRolledBack() {
  }
}