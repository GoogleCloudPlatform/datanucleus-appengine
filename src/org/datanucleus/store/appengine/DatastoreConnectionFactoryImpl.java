// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
  public ManagedConnection getConnection(ObjectManager om, Map o) {
    @SuppressWarnings("unchecked")
    Map<Object, Object> options = o;
    Map<Object, Object> addedOptions = Maps.newHashMap();
    if (options != null) {
      addedOptions.putAll(options);
    }
    return omfContext.getConnectionManager().allocateConnection(this, om,
        addedOptions);
  }

  /**
   * {@inheritDoc}
   */
  public ManagedConnection createManagedConnection(ObjectManager om, Map transactionOptions) {
    return new DatastoreManagedConnection();
  }

  static class DatastoreManagedConnection implements ManagedConnection {
    private boolean managed = false;
    private boolean locked = false;
    private final List<ManagedConnectionResourceListener> listeners = Lists.newArrayList();
    private final DatastoreService datastoreService =
        DatastoreServiceFactoryInternal.getDatastoreService();
    private final XAResource emulatedXAResource = new EmulatedXAResource(datastoreService);

    public Object getConnection() {
      return datastoreService;
    }

    public XAResource getXAResource() {
      return emulatedXAResource;
    }

    public void release() {
      close();
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
