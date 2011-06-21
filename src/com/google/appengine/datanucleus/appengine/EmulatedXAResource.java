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

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * This emulated XAResource only supports a small subset of XA functionality.
 * There's no underlying transaction, just some simple state management.
 * Instances of this class are instantiated and used when the datasource has
 * been configured as nontransactional and the user is not explicitly doing
 * any transaction management.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 * @author Max Ross <maxr@google.com>
 */
class EmulatedXAResource implements XAResource {

  private enum State {NEW, ACTIVE, INACTIVE}

  private State state = State.NEW;

  private final KeyRegistry keyRegistry = new KeyRegistry();

  public void start(Xid xid, int flags) throws XAException {
    if (state != State.NEW) {
      throw new XAException("Nested transactions are not supported");
    }
    state = State.ACTIVE;
  }

  public void commit(Xid arg0, boolean arg1) throws XAException {
    if (state != State.ACTIVE) {
      throw new XAException("A transaction has not been started, cannot commit");
    }
    keyRegistry.clear();
    state = State.INACTIVE;
  }

  public void rollback(Xid xid) throws XAException {
    if (state != State.ACTIVE) {
      throw new XAException("A transaction has not been started, cannot rollback");
    }
    keyRegistry.clear();
    state = State.INACTIVE;
  }

  public void end(Xid xid, int flags) throws XAException {
    // TODO (earmbrust): Should we throw an unsupported error?
  }

  public void forget(Xid xid) throws XAException {
    // TODO (earmbrust): Should we throw an unsupported error?
  }

  public int getTransactionTimeout() throws XAException {
    return 0;
  }

  public boolean isSameRM(XAResource xares) throws XAException {
    // We only support a single datastore, so this should always be true.
    return true;
  }

  public int prepare(Xid xid) throws XAException {
    return XA_OK;
  }

  public Xid[] recover(int flag) throws XAException {
    throw new XAException("Unsupported operation");
  }

  public boolean setTransactionTimeout(int seconds) throws XAException {
    return false;
  }
  
  KeyRegistry getKeyRegistry() {
    return keyRegistry;
  }

  /**
   * @return The current transaction, or {@code null} if the datasource does
   * not support transactions.
   */
  DatastoreTransaction getCurrentTransaction() {
    return null;
  }
}
