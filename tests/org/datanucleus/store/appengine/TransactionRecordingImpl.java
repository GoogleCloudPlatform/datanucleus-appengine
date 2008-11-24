// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Transaction;

/**
 * @author Max Ross <maxr@google.com>
 */
class TransactionRecordingImpl implements Transaction {

  private final Transaction delegate;
  private final Transaction recorder;

  TransactionRecordingImpl(Transaction recorder, Transaction delegate) {
    this.recorder = recorder;
    this.delegate = delegate;
  }

  public void commit() {
    recorder.commit();
    delegate.commit();
  }

  public void rollback() {
    recorder.rollback();
    delegate.rollback();
  }

  public String getId() {
    recorder.getId();
    return delegate.getId();
  }
}
