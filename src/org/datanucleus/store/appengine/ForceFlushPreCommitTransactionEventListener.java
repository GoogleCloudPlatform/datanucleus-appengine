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
package org.datanucleus.store.appengine;

import org.datanucleus.StateManager;

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
 * @author Max Ross <max.ross@gmail.com>
 */
class ForceFlushPreCommitTransactionEventListener extends BaseTransactionEventListener {

  private static final String TXN_EVENT_LISTENER_KEY =
      DatastoreFKListStore.class.getName() + ".txn_event_listener";

  private final StateManager sm;

  ForceFlushPreCommitTransactionEventListener(StateManager sm) {
    this.sm = sm;
    // make sure we only force the repersist once
    sm.setAssociatedValue(TXN_EVENT_LISTENER_KEY, null);
  }

  @Override
  public void transactionPreCommit() {
    if (sm.getAssociatedValue(TXN_EVENT_LISTENER_KEY) == null) {
      for (int pos : sm.getClassMetaData().getAllMemberPositions()) {
        sm.makeDirty(pos);
      }
      sm.flush();
    }
  }
}