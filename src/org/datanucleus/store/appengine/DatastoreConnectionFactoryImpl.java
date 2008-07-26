// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ConnectionFactory;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.ManagedConnectionResourceListener;
import org.datanucleus.OMFContext;

import javax.transaction.xa.XAResource;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.DatastoreService;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreConnectionFactoryImpl implements ConnectionFactory {

  private OMFContext omfContext;

  public DatastoreConnectionFactoryImpl(OMFContext omfContext, String resourceType) {
    this.omfContext = omfContext;
  }

  @SuppressWarnings("unchecked")
  public ManagedConnection getConnection(ObjectManager om, Map options) {
    Map addedOptions = new HashMap();
    if (options != null) {
        addedOptions.putAll(options);
    }
    return omfContext.getConnectionManager().allocateConnection(this, om, addedOptions);
  }

  public ManagedConnection createManagedConnection(ObjectManager om, Map transactionOptions) {
    return new DatastoreManagedConnection();
  }

  static class DatastoreManagedConnection implements ManagedConnection {
    private boolean locked;
    private final List<ManagedConnectionResourceListener> listeners =
        new ArrayList<ManagedConnectionResourceListener>();
    private final DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();

    public Object getConnection() {
      return datastoreService;
    }

    public XAResource getXAResource() {
      // we don't support XA
      return null;
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
