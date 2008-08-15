package org.datanucleus.store.appengine;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Transaction;

/**
 * This emulated XAResource only supports a small subset of XA functionality.
 * Currently it only supports single, non-distributed transactions.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 */
class EmulatedXAResource implements XAResource {
  final DatastoreService datastoreService;
  private Transaction currentTxn;

  public EmulatedXAResource(DatastoreService datastoreService) {
    this.datastoreService = datastoreService;
  }

  Transaction getCurrentTransaction() {
    return currentTxn;
  }

  public void start(Xid xid, int flags) throws XAException {
    // A transaction will only be started if non-transactional reads/writes
    // are turned off.
    if (currentTxn == null) {
      currentTxn = datastoreService.beginTransaction();
    } else {
      throw new XAException("Nested transactions are not supported");
    }
  }

  public void commit(Xid arg0, boolean arg1) throws XAException {
    if (currentTxn != null) {
      currentTxn.commit();
      currentTxn = null;
    } else {
      throw new XAException("A transaction has not been started, cannot commit");
    }
  }

  public void rollback(Xid xid) throws XAException {
    if (currentTxn != null) {
      currentTxn.rollback();
      currentTxn = null;
    } else {
      throw new XAException("A transaction has not been started, cannot roll back");
    }
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
}
