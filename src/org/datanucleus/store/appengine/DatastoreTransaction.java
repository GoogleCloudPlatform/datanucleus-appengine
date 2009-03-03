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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.exceptions.NucleusDataStoreException;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Set;

/**
 * The orm's view of a datastore transaction.  Delegates
 * to a {@link Transaction} and also functions as a txn-level
 * cache.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreTransaction {

  private final Transaction txn;

  private final Map<Key, Entity> putEntities = Utils.newHashMap();

  private final Set<Key> deletedKeys = Utils.newHashSet();

  DatastoreTransaction(Transaction txn) {
    if (txn == null) {
      throw new NullPointerException("txn cannot be null");
    }
    this.txn = txn;
  }

  private void clear() {
    putEntities.clear();
    deletedKeys.clear();
  }

  void commit() {
    try {
      txn.commit();
    } catch (ConcurrentModificationException e) {
      throw new NucleusDataStoreException("Datastore threw a ConcurrentModificationException", e);      
    }
    clear();
  }

  void rollback() {
    txn.rollback();
    clear();
  }

  Transaction getInnerTxn() {
    return txn;
  }

  void addPutEntity(Entity entity) {
    // Make a copy in case someone changes
    // the provided entity after we add it to our cache.
    putEntities.put(entity.getKey(), makeCopy(entity));
  }

  private Entity makeCopy(Entity entity) {
    // We don't check key when we look for changes so it's
    // ok that the copy doesn't have its key set.
    Entity copy = new Entity(entity.getKind());
    DatastoreFieldManager.copyProperties(entity, copy);
    return copy;
  }

  void addDeletedKey(Key key) {
    deletedKeys.add(key);
  }

  Map<Key, Entity> getPutEntities() {
    return putEntities;
  }

  Set<Key> getDeletedKeys() {
    return deletedKeys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DatastoreTransaction that = (DatastoreTransaction) o;

    if (!txn.equals(that.txn)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return txn.hashCode();
  }
}
