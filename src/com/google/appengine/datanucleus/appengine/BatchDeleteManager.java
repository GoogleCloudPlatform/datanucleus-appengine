/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package com.google.appengine.datanucleus.appengine;

import com.google.appengine.api.datastore.Key;

import java.util.List;

/**
 * Manages batch deletes that are initiated via standard JDO and JPA
 * calls.
 *
 * @author Max Ross <maxr@google.com>
 */
public class BatchDeleteManager extends BatchManager<BatchDeleteManager.BatchDeleteState> {

  static final class BatchDeleteState {
    private final DatastoreTransaction txn;
    private final Key key;

    BatchDeleteState(DatastoreTransaction txn, Key key) {
      this.txn = txn;
      this.key = key;
    }
  }

  String getOperation() {
    return "delete";
  }

  void processBatchState(DatastorePersistenceHandler handler, List<BatchDeleteState> batchDeleteStateList) {
    DatastoreTransaction txn = batchDeleteStateList.get(0).txn;
    List<Key> keyList = Utils.newArrayList();
    for (BatchDeleteState bds : batchDeleteStateList) {
      if (bds.txn != txn) {
        throw new IllegalStateException("Batch delete cannot involve multiple txns.");
      }
      keyList.add(bds.key);
    }
    handler.delete(txn, keyList);
  }

}