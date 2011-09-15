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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.DatastoreAttributes;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Index;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyRange;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Max Ross <maxr@google.com>
 */
public class BaseDatastoreServiceDelegate implements DatastoreService {

  private final DatastoreService delegate;

  public BaseDatastoreServiceDelegate(DatastoreService delegate) {
    this.delegate = delegate;
  }

  public Entity get(Key key) throws EntityNotFoundException {
    return delegate.get(key);
  }

  public Entity get(Transaction transaction, Key key) throws EntityNotFoundException {
    return delegate.get(transaction, key);
  }

  public Map<Key, Entity> get(Iterable<Key> keyIterable) {
    return delegate.get(keyIterable);
  }

  public Map<Key, Entity> get(Transaction transaction, Iterable<Key> keyIterable) {
    return delegate.get(transaction, keyIterable);
  }

  public Key put(Entity entity) {
    return delegate.put(entity);
  }

  public Key put(Transaction transaction, Entity entity) {
    return delegate.put(transaction, entity);
  }

  public List<Key> put(Iterable<Entity> entityIterable) {
    return delegate.put(entityIterable);
  }

  public List<Key> put(Transaction transaction, Iterable<Entity> entityIterable) {
    return delegate.put(transaction, entityIterable);
  }

  public void delete(Key... keys) {
    delegate.delete(keys);
  }

  public void delete(Transaction transaction, Key... keys) {
    delegate.delete(transaction, keys);
  }

  public void delete(Iterable<Key> keyIterable) {
    delegate.delete(keyIterable);
  }

  public void delete(Transaction transaction, Iterable<Key> keyIterable) {
    delegate.delete(transaction, keyIterable);
  }

  public PreparedQuery prepare(Query query) {
    return delegate.prepare(query);
  }

  public PreparedQuery prepare(Transaction transaction, Query query) {
    return delegate.prepare(transaction, query);
  }

  public Transaction beginTransaction() {
    return delegate.beginTransaction();
  }

  public Transaction beginTransaction(TransactionOptions txnOpts) {
    return delegate.beginTransaction(txnOpts);
  }

  public Transaction getCurrentTransaction() {
    return delegate.getCurrentTransaction();
  }

  public Transaction getCurrentTransaction(Transaction transaction) {
    return delegate.getCurrentTransaction(transaction);
  }

  public Collection<Transaction> getActiveTransactions() {
    return delegate.getActiveTransactions();
  }

  public KeyRange allocateIds(String s, long l) {
    return delegate.allocateIds(s, l);
  }

  public KeyRange allocateIds(Key key, String s, long l) {
    return delegate.allocateIds(key, s, l);
  }

  public KeyRangeState allocateIdRange(KeyRange keyRange) {
    return delegate.allocateIdRange(keyRange);
  }

  public DatastoreAttributes getDatastoreAttributes() {
    return delegate.getDatastoreAttributes();
  }

  public Map<Index, Index.IndexState> getIndexes() {
    return delegate.getIndexes();
  }
}
