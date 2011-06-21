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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.util.NucleusLogger;

import java.sql.SQLException;

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
  private DatastoreTransaction currentTxn;

  public DatastoreXAResource(DatastoreService datastoreService) {
    this.datastoreService = datastoreService;
  }

  @Override
  DatastoreTransaction getCurrentTransaction() {
    return currentTxn;
  }

  @Override
  public void start(Xid xid, int flags) throws XAException {
    super.start(xid, flags);
    // A transaction will only be started if non-transactional reads/writes
    // are turned off.
    if (currentTxn == null) {
      Transaction datastoreTxn = datastoreService.getCurrentTransaction(null);
      // Typically the transaction will have been established when the user
      // calls pm.currentTransaction().begin() or em.getTransaction().begin(),
      // but if the datasource is non-transactional and the user is not
      // demarcating transactions, the transaction can be started without
      // going through the pm or em, which sidesteps our logic to aggressively
      // start the transaction.  In this case we'll just start the transaction
      // ourselves.  This isn't a problem for transactional tasks because
      // the user isn't actually managing transactions, it's just DataNucleus
      // doing it under the hood in order to force things to flush.
      if (datastoreTxn == null) {
        datastoreTxn = datastoreService.beginTransaction();
      }
      currentTxn = new DatastoreTransaction(datastoreTxn);
    } else {
      throw new XAException("Nested transactions are not supported");
    }
  }

  @Override
  public void commit(Xid arg0, boolean arg1) throws XAException {
    super.commit(arg0, arg1);
    if (currentTxn != null) {
      try {
        currentTxn.commit();
      } catch (SQLException e) {
        XAException xa = new XAException(e.getMessage());
        xa.initCause(e);
        throw xa;
      }
      NucleusLogger.DATASTORE.debug(
          "Committed datastore transaction: " + currentTxn.getInnerTxn().getId());
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
      NucleusLogger.DATASTORE.debug(
          "Rolled back datastore transaction: " + currentTxn.getInnerTxn().getId());
      currentTxn = null;
    } else {
      throw new XAException("A transaction has not been started, cannot roll back");
    }
  }
}
