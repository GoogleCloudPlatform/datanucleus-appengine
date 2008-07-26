// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.store.StorePersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.StateManager;
import org.datanucleus.ObjectManager;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;

import javax.naming.directory.DirContext;
import javax.naming.NamingException;
import javax.jdo.spi.PersistenceCapable;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.EntityNotFoundException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastorePersistenceHandler implements StorePersistenceHandler {

  private final DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
  private final DatastoreManager storeMgr;

  public DatastorePersistenceHandler(StoreManager storeMgr) {
    this.storeMgr = (DatastoreManager) storeMgr;
  }

  public void close() {
  }

  public void insertObject(StateManager sm) {
    throw new UnsupportedOperationException();
  }

  public void fetchObject(StateManager sm, int fieldNumbers[]) {
    throw new UnsupportedOperationException();
  }

  public void updateObject(StateManager sm, int fieldNumbers[]) {
    throw new UnsupportedOperationException();
  }

  public void deleteObject(StateManager sm) {
    Key key = (Key) sm.getInternalObjectId();
    datastoreService.delete(key);
  }

  public void locateObject(StateManager sm) {
  }

  public Object findObject(ObjectManager om, Object id) {
    ClassLoaderResolver clr = om.getClassLoaderResolver();
    String className = storeMgr.getClassNameForObjectID(id, clr, om);
    AbstractClassMetaData cmd = om.getMetaDataManager().getMetaDataForClass(className, clr);
    if (cmd.getIdentityType() == IdentityType.APPLICATION) {
        // Generate a template object with these PK field values
        Class pcClass = clr.classForName(className, (id instanceof OID) ? null : id.getClass().getClassLoader());
        StateManager sm = StateManagerFactory.newStateManagerForHollow(om, pcClass, id);
        locateObject(sm);
        return sm.getObject();
    }
    throw new NucleusException();
  }
}
