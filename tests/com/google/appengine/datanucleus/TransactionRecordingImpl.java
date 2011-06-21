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

import com.google.appengine.api.datastore.Transaction;

import java.util.concurrent.Future;

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

  public boolean isActive() {
    recorder.isActive();
    return delegate.isActive();
  }

  public String getApp() {
    recorder.getApp();
    return delegate.getApp();
  }

  public Future<Void> commitAsync() {
    recorder.commitAsync();
    return delegate.commitAsync();
  }

  public Future<Void> rollbackAsync() {
    recorder.rollbackAsync();
    return delegate.rollbackAsync();
  }
}
