/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOTransaction;

import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;

import org.datanucleus.util.NucleusLogger;

import javax.jdo.PersistenceManager;

/**
 * TODO Remove this. StoreManager has callback methods
 * transactionStarted/transactionCommitted/transactionRolledBack so just use
 * those to get such a hook.
 * 
 * Datastore-specific extension that aggressively starts transactions.
 * DataNucleus delays the start of the transaction until the first
 * read or write, but since task queue tasks auto-enlist in the current
 * transaction when a current transaction exists, it's important that
 * a datastore transaction actually be active once the user has called
 * pm.currentTransaction().begin().
 *
 * @author Max Ross <max.ross@gmail.com>
 */
class DatastoreJDOTransaction extends JDOTransaction {

  private final DatastoreServiceConfig config;

  public DatastoreJDOTransaction(PersistenceManager pm, DatastoreManager storeMgr, org.datanucleus.Transaction tx) {
    super((JDOPersistenceManager) pm, tx);
    config = storeMgr.getDefaultDatastoreServiceConfigForWrites();
  }

  @Override
  public void begin() {
    super.begin();
    Transaction txn = DatastoreServiceFactoryInternal.getDatastoreService(config).beginTransaction();
    NucleusLogger.DATASTORE.debug("Started new datastore transaction: " + txn.getId());
  }

  @Override
  public void commit() {
    super.commit();
    Transaction txn = DatastoreServiceFactoryInternal.getDatastoreService(config).getCurrentTransaction(null);
    if (txn == null) {
      // this is ok, it means the txn was committed via the connection
    } else {
      // ordinarily the txn gets committed in DatastoreXAResource.commit(), but
      // if the begin/commit block doesn't perform any reads or writes then
      // DatastoreXAResource.commit() won't be called.  In order to avoid
      // leaving transactions open we do the commit here.
      txn.commit();
    }
  }

  @Override
  public void rollback() {
    super.rollback();
    Transaction txn = DatastoreServiceFactoryInternal.getDatastoreService(config).getCurrentTransaction(null);
    if (txn == null) {
      // this is ok, it means the txn was rolled back via the connection
    } else {
      // ordinarily the txn gets aborted in DatastoreXAResource.commit(), but
      // if the begin/abort block doesn't perform any reads or writes then
      // DatastoreXAResource.rollback() won't be called.  In order to avoid
      // leaving transactions open we do the rollback here.
      txn.rollback();
    }
  }
}
