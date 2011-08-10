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

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/**
 * Extension to {@link EmulatedXAResource} that manages a (autoCreate) transaction against the datastore.
 * Currently only supports a single, non-distributed transaction.  Instances of this class are
 * instantiated and used when the datasource has been configured as "autoCreateTransaction" or the user is
 * explicitly doing transaction management.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 * @author Max Ross <maxr@google.com>
 */
class DatastoreXAResource extends EmulatedXAResource {

  /** The datastore service we'll use to perform datastore operations. */
  private final DatastoreService datastoreService;

  /** The current datastore transaction. */
  private DatastoreTransaction currentTxn;

  public DatastoreXAResource(DatastoreService datastoreService) {
    NucleusLogger.GENERAL.info(">> DatastoreXAResource.ctr datastoreService=" + datastoreService);
    this.datastoreService = datastoreService;
  }

  @Override
  DatastoreTransaction getCurrentTransaction() {
    return currentTxn;
  }

  @Override
  DatastoreService getDatastoreService() {
    return datastoreService;
  }

  @Override
  public void start(Xid xid, int flags) throws XAException {
    super.start(xid, flags);
    NucleusLogger.GENERAL.info(">> DatastoreXAResource.start");
    if (currentTxn == null) {
      // DatastoreService transaction will have been started by DatastoreManager.transactionStarted()
      Transaction datastoreTxn = datastoreService.getCurrentTransaction(null);
      currentTxn = new DatastoreTransaction(datastoreTxn);
    } else {
      throw new XAException("Transaction has already been started and nested transactions are not supported");
    }
  }

  @Override
  public void commit(Xid xid, boolean onePhase) throws XAException {
    super.commit(xid, onePhase);
    NucleusLogger.GENERAL.info(">> DatastoreXAResource.commit");
    if (currentTxn != null) {
      currentTxn.commit();
      NucleusLogger.DATASTORE.debug("Committed datastore transaction: " + currentTxn.getInnerTxn().getId());
      currentTxn = null;
    } else {
      throw new XAException("Transaction has not been started, cannot commit");
    }
  }

  @Override
  public void rollback(Xid xid) throws XAException {
    super.rollback(xid);
    NucleusLogger.GENERAL.info(">> DatastoreXAResource.rollback");
    if (currentTxn != null) {
      currentTxn.rollback();
      NucleusLogger.DATASTORE.debug("Rolled back datastore transaction: " + currentTxn.getInnerTxn().getId());
      currentTxn = null;
    } else {
      throw new XAException("Transaction has not been started, cannot roll back");
    }
  }
}
