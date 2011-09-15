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
import com.google.appengine.api.datastore.DatastoreServiceConfig;

import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.Transaction;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.util.NucleusLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAResource;

/**
 * Factory for connections to the datastore. There are two connection factories for a DatastoreManager.
 * <ul>
 * <li>Transactional : a connection (DatastoreService) is obtained at the start of the transaction and we
 * call "beginTransaction" on it. It is closed at the end of the transaction after we call "commit"/"rollback".</li>
 * <li>Nontransactional : a connection (DatastoreService) is obtained on the first operation, and is retained
 * until PM/EM.close(). All operations are atomic, since we don't call "beginTransaction", hence no need to call
 * "commit"/"rollback"</li>
 * </ul>
 *
 * <p>
 * By default, when the user invokes transaction.begin() in user-space this will start a DatastoreTransaction.
 * If they have the persistence property <i>datanucleus.appengine.autoCreateDatastoreTxns</i> set to false
 * then this means it will NOT start a transaction. Why anyone would want to do this is unknown to me
 * but anyway it's there.
 * </p>
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreConnectionFactoryImpl extends AbstractConnectionFactory {

  public static final String AUTO_CREATE_TXNS_PROPERTY =
      "datanucleus.appengine.autoCreateDatastoreTxns";

  private final boolean isAutoCreateTransaction;

  /**
   * Constructs a connection factory for the datastore.
   * Provides ManagedConnections to communicate with the datastore.
   *
   * @param storeMgr The store manager
   * @param resourceType Name of the resource ("appengine", "appengine-nontx")
   */
  public DatastoreConnectionFactoryImpl(StoreManager storeMgr, String resourceType) {
    super(storeMgr, resourceType);

    PersistenceConfiguration conf = storeMgr.getNucleusContext().getPersistenceConfiguration();
    if (conf.getProperty(DatastoreConnectionFactoryImpl.AUTO_CREATE_TXNS_PROPERTY) == null) {
        // User hasn't configured the "auto-create" property, so set it
        conf.setProperty(DatastoreConnectionFactoryImpl.AUTO_CREATE_TXNS_PROPERTY, Boolean.TRUE.toString());
    }
    this.isAutoCreateTransaction = conf.getBooleanProperty(AUTO_CREATE_TXNS_PROPERTY);
  }

  public void close() {}

  /**
   * {@inheritDoc}
   */
  public ManagedConnection getConnection(Object poolKey, Transaction txn, Map options) {
    return storeMgr.getConnectionManager().allocateConnection(this, poolKey, txn, options);
  }

  /**
   * {@inheritDoc}
   */
  public ManagedConnection createManagedConnection(Object poolKey, Map transactionOptions) {
    return new DatastoreManagedConnection(storeMgr, isAutoCreateTransaction());
  }

  boolean isAutoCreateTransaction() {
    return isAutoCreateTransaction;
  }

  static class DatastoreManagedConnection implements ManagedConnection {
    private boolean managed = false;
    private boolean locked = false;
    private final List<ManagedConnectionResourceListener> listeners =
        new ArrayList<ManagedConnectionResourceListener>();
    private final XAResource datastoreXAResource;

    DatastoreManagedConnection(StoreManager storeMgr, boolean autoCreateTransaction) {
      DatastoreManager datastoreManager = (DatastoreManager) storeMgr;
      DatastoreServiceConfig config = datastoreManager.getDefaultDatastoreServiceConfigForWrites();
      DatastoreService datastoreService = DatastoreServiceFactoryInternal.getDatastoreService(config);
      if (NucleusLogger.CONNECTION.isDebugEnabled()) {
        if (datastoreService instanceof WrappedDatastoreService) {
          NucleusLogger.CONNECTION.debug("Created ManagedConnection using DatastoreService = " + 
              ((WrappedDatastoreService)datastoreService).getDelegate());
        } else {
          NucleusLogger.CONNECTION.debug("Created ManagedConnection using DatastoreService = " + datastoreService);
        }
      }
      if (autoCreateTransaction) {
        datastoreXAResource = new DatastoreXAResource(
            datastoreService, datastoreManager.getDefaultDatastoreTransactionOptions());
      } else {
        datastoreXAResource = new EmulatedXAResource(datastoreService);
      }
    }

    public Object getConnection() {
      // Return the DatastoreService
      return ((EmulatedXAResource)datastoreXAResource).getDatastoreService();
    }

    public XAResource getXAResource() {
      return datastoreXAResource;
    }

    public void release() {
      if (!managed) {        
        close();
      }
    }

    public void close() {
      for (ManagedConnectionResourceListener listener : listeners) {
        listener.managedConnectionPreClose();
      }
      // nothing to actually close
      for (ManagedConnectionResourceListener listener : listeners) {
        listener.managedConnectionPostClose();
      }
    }

    public void setManagedResource() {
      managed = true;
    }

    public boolean isLocked() {
      return locked;
    }

    public void lock() {
      locked = true;
    }

    public void unlock() {
      locked = false;
    }

    public void transactionFlushed() {
      for (ManagedConnectionResourceListener listener : listeners) {
        listener.transactionFlushed();
      }
    }

    public void transactionPreClose() {
      for (ManagedConnectionResourceListener listener : listeners) {
        listener.transactionPreClose();
      }
    }

    public void addListener(ManagedConnectionResourceListener listener) {
      listeners.add(listener);
    }

    public void removeListener(ManagedConnectionResourceListener listener) {
      listeners.remove(listener);
    }
  }
}
