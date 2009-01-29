// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;

import org.datanucleus.ConnectionFactory;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ManagedConnectionResourceListener;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAResource;

/**
 * Factory for connections to the datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreConnectionFactoryImpl implements ConnectionFactory {

  public static final String AUTO_CREATE_TXNS_PROPERTY =
      "datanucleus.appengine.autoCreateDatastoreTxns";

  private final OMFContext omfContext;
  private final boolean isTransactional;

  private final DatastoreService datastoreService =
      DatastoreServiceFactoryInternal.getDatastoreService();

  /**
   * Constructs a connection factory for the datastore.
   * This connection factory either creates connections that are all
   * transactional or connections that are all nontransactional.
   * Transactional connections manage an underlying datastore transaction.
   * Nontransactional connections do not manage an underlying datastore
   * transaction.   The type of connection that this factory provides
   * is controlled via the {@link #AUTO_CREATE_TXNS_PROPERTY} property, which
   * can be specified in jdoconfig.xml (for JDO) or persistence.xml (for JPA).
   * Note that the default value for this property is {@code true}.
   *
   * JDO Example:
   * <pre>
   * <jdoconfig xmlns="http://java.sun.com/xml/ns/jdo/jdoconfig"
   * xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
   * xsi:noNamespaceSchemaLocation="http://java.sun.com/xml/ns/jdo/jdoconfig">
   *
   *  <persistence-manager-factory name="transactional">
   *     <property name="javax.jdo.PersistenceManagerFactoryClass" value="org.datanucleus.jdo.JDOPersistenceManagerFactory"/>
   *     <property name="javax.jdo.option.ConnectionURL" value="appengine"/>
   *  </persistence-manager-factory>
   *
   *  <persistence-manager-factory name="nontransactional">
   *     <property name="javax.jdo.PersistenceManagerFactoryClass" value="org.datanucleus.jdo.JDOPersistenceManagerFactory"/>
   *     <property name="javax.jdo.option.ConnectionURL" value="appengine"/>
   *     <property name="datanucleus.appengine.autoCreateDatastoreTxns" value="false"/>
   *  </persistence-manager-factory>
   *
   * </jdoconfig>
   * </pre>
   *
   * JPA Example:
   * <pre>
   * <persistence-unit name="transactional">
   *     <properties>
   *         <property name="datanucleus.ConnectionURL" value="appengine"/>
   *     </properties>
   * </persistence-unit>
   *
   * <persistence-unit name="nontransactional">
   *     <properties>
   *         <property name="datanucleus.ConnectionURL" value="appengine"/>
   *         <property name="datanucleus.appengine.autoCreateDatastoreTxns" value="false"/>
   *     </properties>
   * </persistence-unit>
   * </pre>
   *
   * @param omfContext The OMFContext
   * @param resourceType The resource type of the connection, either tx or
   * notx.  We ignore this parameter because it isn't a valid indication of
   * whether or not this connection factory creates transactional connections.
   */
  public DatastoreConnectionFactoryImpl(OMFContext omfContext, String resourceType) {
    this.omfContext = omfContext;
    this.isTransactional = omfContext.getPersistenceConfiguration().getBooleanProperty(AUTO_CREATE_TXNS_PROPERTY);
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
    return new DatastoreManagedConnection(datastoreService, newXAResource());
  }

  boolean isTransactional() {
    return isTransactional;
  }

  private XAResource newXAResource() {
    return isTransactional() ?
        new DatastoreXAResource(datastoreService) : new EmulatedXAResource();
  }

  static class DatastoreManagedConnection implements ManagedConnection {
    private boolean managed = false;
    private boolean locked = false;
    private final List<ManagedConnectionResourceListener> listeners =
        new ArrayList<ManagedConnectionResourceListener>();
    protected final DatastoreService datastoreService;
    private final XAResource datastoreXAResource;

    DatastoreManagedConnection(DatastoreService datastoreService, XAResource datastoreXAResource) {
      this.datastoreService = datastoreService;
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
