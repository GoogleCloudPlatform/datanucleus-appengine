// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.common.collect.Lists;

import org.datanucleus.ConnectionFactory;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ManagedConnectionResourceListener;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;

import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAResource;

/**
 * Factory for connections to the datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreConnectionFactoryImpl implements ConnectionFactory {

  /**
   * If the map passed as an argument to {@link #createManagedConnection}
   * contains a key with this name, the returned connection will not be
   * associated with a datastore transaction.
   */
  static final String NO_TXN_PROPERTY = DatastoreConnectionFactoryImpl.class.getName() + ".no_txn"; 

  private OMFContext omfContext;

  /**
   * Constructs a connection faotory for the datastore
   *
   * @param omfContext The OMFContext
   * @param resourceType The resource type of the connection.
   */
  public DatastoreConnectionFactoryImpl(OMFContext omfContext, String resourceType) {
    // TODO(maxr): Find out what resourceType is
    this.omfContext = omfContext;
  }

  /**
   * {@inheritDoc}
   */
  public ManagedConnection getConnection(ObjectManager om, Map options) {
    return omfContext.getConnectionManager().allocateConnection(this, om, options);
  }

  /**
   * {@inheritDoc}
   */
  public ManagedConnection createManagedConnection(ObjectManager om, Map transactionOptions) {
    EmulatedXAResource resource;
    if (transactionOptions != null && transactionOptions.containsKey(NO_TXN_PROPERTY)) {
      resource = new EmulatedXAResource();
    } else {
      resource =
        new DatastoreXAResource(DatastoreServiceFactoryInternal.getDatastoreService());
    }
    return new DatastoreManagedConnection(resource);
  }

  static class DatastoreManagedConnection implements ManagedConnection {
    private boolean managed = false;
    private boolean locked = false;
    private final List<ManagedConnectionResourceListener> listeners = Lists.newArrayList();
    private final DatastoreService datastoreService =
        DatastoreServiceFactoryInternal.getDatastoreService();
    private final XAResource datastoreXAResource;

    DatastoreManagedConnection(XAResource datastoreXAResource) {
      this.datastoreXAResource = datastoreXAResource;
    }

    public Object getConnection() {
      return datastoreService;
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
      // nothing to close
      for (ManagedConnectionResourceListener listener : listeners) {
        listener.managedConnectionPreClose();
      }
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

    public void flush() {
      // Nothing to flush
      for (ManagedConnectionResourceListener listener : listeners) {
        listener.managedConnectionFlushed();
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
