// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.PreparedQuery;
import com.google.apphosting.api.datastore.Query;
import com.google.apphosting.api.datastore.Transaction;

import java.util.Collection;
import java.util.Map;

/**
 * This testing helper class allows a test to perform an integration test while
 * simultaneously recording calls.  This is useful when used in conjunction with
 * a mocking utility such as EasyMock.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 */
class DatastoreServiceRecordingImpl implements DatastoreService {

  private final DatastoreService recorder;
  private final DatastoreService delegate;
  private final Transaction txnRecorder;

  public DatastoreServiceRecordingImpl(DatastoreService recorder, DatastoreService delegate,
      Transaction txnRecorder) {
    this.recorder = recorder;
    this.delegate = delegate;
    this.txnRecorder = txnRecorder;
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

  public void put(Iterable<Entity> entities) {
    recorder.put(entities);
    delegate.put(entities);
  }

  public void put(Transaction txn, Iterable<Entity> entities) {
    recorder.put(txn, entities);
    delegate.put(txn, entities);
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
    recorder.beginTransaction();
    Transaction txn =  delegate.beginTransaction();
    return new TransactionRecordingImpl(txn, txnRecorder);
  }

  public Transaction getCurrentTransaction() {
    recorder.getCurrentTransaction();
    return delegate.getCurrentTransaction();
  }

  public Transaction getCurrentTransaction(Transaction txn) {
    recorder.getCurrentTransaction(txn);
    return delegate.getCurrentTransaction(txn);
  }

  public Collection<Transaction> getActiveTransactions() {
    recorder.getActiveTransactions();
    return delegate.getActiveTransactions();
  }

  public PreparedQuery prepare(Query query) {
    recorder.prepare(query);
    return delegate.prepare(query);
  }
}
