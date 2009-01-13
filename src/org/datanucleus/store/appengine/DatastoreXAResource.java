// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Transaction;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/**
 * Extension to {@link EmulatedXAResource} that manages a transaction against the datastore.
 * Currently only supports a single, non-distributed transaction.  Instances of this class are
 * instantiated and used when the datasource has been configured as transactional or the user is
 * explicitly doing transaction management.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 * @author Max Ross <maxr@google.com>
 */
class DatastoreXAResource extends EmulatedXAResource {

  /**
   * The datastore service we'll use to perform datastore operations.
   */
  private final DatastoreService datastoreService;

  /**
   * The current datastore transaction.
   */
  private Transaction currentTxn;

  public DatastoreXAResource(DatastoreService datastoreService) {
    this.datastoreService = datastoreService;
  }

  Transaction getCurrentTransaction() {
    return currentTxn;
  }

  @Override
  public void start(Xid xid, int flags) throws XAException {
    super.start(xid, flags);
    // A transaction will only be started if non-transactional reads/writes
    // are turned off.
    if (currentTxn == null) {
      currentTxn = datastoreService.beginTransaction();
    } else {
      throw new XAException("Nested transactions are not supported");
    }
  }

  @Override
  public void commit(Xid arg0, boolean arg1) throws XAException {
    super.commit(arg0, arg1);
    if (currentTxn != null) {
      currentTxn.commit();
      currentTxn = null;
    } else {
      throw new XAException("A transaction has not been started, cannot commit");
    }
  }

  @Override
  public void rollback(Xid xid) throws XAException {
    super.rollback(xid);
    if (currentTxn != null) {
      currentTxn.rollback();
      currentTxn = null;
    } else {
      throw new XAException("A transaction has not been started, cannot roll back");
    }
  }
}
