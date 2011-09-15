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
 * This testing helper class allows a test to perform an integration test while
 * simultaneously recording calls.  This is useful when used in conjunction with
 * a mocking utility such as EasyMock.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class DatastoreServiceRecordingImpl implements DatastoreService {

  private final DatastoreService recorder;
  private final DatastoreService delegate;
  private final Transaction txnRecorder;
  private final TxnIdAnswer txnIdAnswer;

  public DatastoreServiceRecordingImpl(DatastoreService recorder, DatastoreService delegate, Transaction txnRecorder,
      TxnIdAnswer txnIdAnswer) {
    this.recorder = recorder;
    this.delegate = delegate;
    this.txnRecorder = txnRecorder;
    this.txnIdAnswer = txnIdAnswer;
  }

  public Entity get(Key key) throws EntityNotFoundException {
    recorder.get(key);
    return delegate.get(key);
  }

  public Entity get(Transaction txn, Key key) throws EntityNotFoundException {
    recorder.get(txn, key);
    return delegate.get(txn, key);
  }

  public Map<Key, Entity> get(Iterable<Key> keys) {
    recorder.get(keys);
    return delegate.get(keys);
  }

  public Map<Key, Entity> get(Transaction txn, Iterable<Key> keys) {
    recorder.get(txn, keys);
    return delegate.get(txn, keys);
  }

  public Key put(Entity entity) {
    recorder.put(entity);
    return delegate.put(entity);
  }

  public Key put(Transaction txn, Entity entity) {
    recorder.put(txn, entity);
    return delegate.put(txn, entity);
  }

  public List<Key> put(Iterable<Entity> entities) {
    recorder.put(entities);
    return delegate.put(entities);
  }

  public List<Key> put(Transaction txn, Iterable<Entity> entities) {
    recorder.put(txn, entities);
    return delegate.put(txn, entities);
  }

  public void delete(Key... keys) {
    recorder.delete(keys);
    delegate.delete(keys);
  }

  public void delete(Transaction txn, Key... keys) {
    recorder.delete(txn, keys);
    delegate.delete(txn, keys);
  }

  public void delete(Iterable<Key> keys) {
    recorder.delete(keys);
    delegate.delete(keys);
  }

  public void delete(Transaction txn, Iterable<Key> keys) {
    recorder.delete(txn, keys);
    delegate.delete(txn, keys);
  }

  public Transaction beginTransaction() {
    Transaction txn =  delegate.beginTransaction();
    txnIdAnswer.setExpectedTxnId(txn.getId());
    recorder.beginTransaction();
    return new TransactionRecordingImpl(txn, txnRecorder);
  }

  public Transaction beginTransaction(TransactionOptions transactionOptions) {
    Transaction txn =  delegate.beginTransaction(transactionOptions);
    txnIdAnswer.setExpectedTxnId(txn.getId());
    recorder.beginTransaction(transactionOptions);
    return new TransactionRecordingImpl(txn, txnRecorder);
  }

  public Transaction getCurrentTransaction() {
    Transaction t1 = recorder.getCurrentTransaction();
    Transaction t2 = delegate.getCurrentTransaction();
    if (t1 == null || t2 == null) {
      return null;
    }
    return new TransactionRecordingImpl(t1, t2);
  }

  public Transaction getCurrentTransaction(Transaction txn) {
    Transaction t1 = recorder.getCurrentTransaction(txn);
    Transaction t2 = delegate.getCurrentTransaction(txn);
    if (t1 == null || t2 == null) {
      return null;
    }
    return new TransactionRecordingImpl(t1, t2);
  }

  public Collection<Transaction> getActiveTransactions() {
    recorder.getActiveTransactions();
    return delegate.getActiveTransactions();
  }

  public PreparedQuery prepare(Query query) {
    recorder.prepare(query);
    return delegate.prepare(query);
  }

  public PreparedQuery prepare(Transaction transaction, Query query) {
    recorder.prepare(transaction, query);
    return delegate.prepare(transaction, query);
  }

  public KeyRange allocateIds(String kind, long num) {
    recorder.allocateIds(kind, num);
    return delegate.allocateIds(kind, num);
  }

  public KeyRange allocateIds(Key parent, String kind, long num) {
    recorder.allocateIds(parent, kind, num);
    return delegate.allocateIds(parent, kind, num);
  }

  public KeyRangeState allocateIdRange(KeyRange keyRange) {
    recorder.allocateIdRange(keyRange);
    return delegate.allocateIdRange(keyRange);
  }

  public DatastoreAttributes getDatastoreAttributes() {
    recorder.getDatastoreAttributes();
    return delegate.getDatastoreAttributes();
  }

  public Map<Index, Index.IndexState> getIndexes() {
    recorder.getIndexes();
    return delegate.getIndexes();
  }
}
