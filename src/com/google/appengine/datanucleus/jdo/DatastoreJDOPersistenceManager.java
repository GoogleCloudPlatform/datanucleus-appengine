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
package com.google.appengine.datanucleus.jdo;

import org.datanucleus.Transaction;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;

import com.google.appengine.datanucleus.BatchDeleteManager;
import com.google.appengine.datanucleus.BatchManager;
import com.google.appengine.datanucleus.BatchPutManager;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastorePersistenceHandler;

import java.util.Collection;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJDOPersistenceManager extends JDOPersistenceManager {

  public DatastoreJDOPersistenceManager(JDOPersistenceManagerFactory apmf, String userName, String password) {
    super(apmf, userName, password);
    setTransaction(om.getTransaction());
  }

  private BatchPutManager getBatchPutManager() {
    DatastoreManager dm = (DatastoreManager) getObjectManager().getStoreManager();
    return dm.getBatchPutManager();
  }

  private BatchDeleteManager getBatchDeleteManager() {
    DatastoreManager dm = (DatastoreManager) getObjectManager().getStoreManager();
    return dm.getBatchDeleteManager();
  }

  /**
   * We override this so we can execute a batch put at the datastore level.
   * The batch put will only execute for Transient pcs.  Making this work
   * for updates is too tricky because an update can be triggered by many
   * different events: committing a txn, closing a persistence manager,
   * modifying a field value, etc.  Ideally we could capture all mutations
   * and then divide them up intelligently at time of commit/pm.close(), but
   * this would probably violate a lot of assumptions around when the mutations
   * are actually executed.
   */
  @Override
  public Collection makePersistentAll(final Collection pcs) {
    // TODO Remove this since DN has PersistenceHandler.batchStart/batchEnd methods
    return new BatchManagerWrapper().call(getBatchPutManager(), new Callable<Collection>() {
      public Collection call() {
        return DatastoreJDOPersistenceManager.super.makePersistentAll(pcs);
      }
    });
  }

  /**
   * We override this so we can execute a batch delete at the datastore level.
   */
  @Override
  public void deletePersistentAll(final Collection pcs) {
    // TODO Remove this since DN has PersistenceHandler.batchStart/batchEnd methods
    new BatchManagerWrapper().call(getBatchDeleteManager(), new Callable<Void>() {
      public Void call() {
        DatastoreJDOPersistenceManager.super.deletePersistentAll(pcs);
        return null;
      }
    });
  }

  /**
   * Invokes the provided {@link Callable} in {@link BatchManager} operations.
   */
  private final class BatchManagerWrapper {
    private <T> T call(BatchManager batchMgr, Callable<T> callable) {
      batchMgr.start();
      try {
        return callable.call();
      } finally {
        batchMgr.finish(
            (DatastorePersistenceHandler) getObjectManager().getStoreManager().getPersistenceHandler());
      }
    }
  }

  @Override
  protected void setTransaction(Transaction tx) {
    DatastoreManager storeMgr = (DatastoreManager) getObjectManager().getStoreManager();
    if (storeMgr.connectionFactoryIsTransactional()) {
      // We need to install our own transaction object here.
      jdotx = new DatastoreJDOTransaction(this, storeMgr, tx);
    } else {
      super.setTransaction(tx);
    }
  }

  /**
   * Our version doesn't throw Exception.  yuh.
   */
  private interface Callable<T> {
    T call();
  }
}
