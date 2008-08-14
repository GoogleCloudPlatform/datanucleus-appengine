// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.StorePersistenceHandler;

import javax.jdo.identity.StringIdentity;
import javax.jdo.spi.PersistenceCapable;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastorePersistenceHandler implements StorePersistenceHandler {

  // TODO(maxr): Get rid of the config arg.
  private final DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
  private final DatastoreManager storeMgr;

  public DatastorePersistenceHandler(StoreManager storeMgr) {
    this.storeMgr = (DatastoreManager) storeMgr;
  }

  public void close() {
  }

  public void insertObject(StateManager sm) {
    // Check if read-only so update not permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
    DatastoreService datastore = (DatastoreService) mconn.getConnection();

    int[] fieldNumbers = sm.getClassMetaData().getAllMemberPositions();
    // TODO(maxr): Hook into mechanism so that kind is not tied to fqn.
    // TODO(maxr): Figure out how to deal with ancestors.
    Entity entity = new Entity(sm.getClassMetaData().getFullClassName());
    sm.provideFields(fieldNumbers, new DatastoreFieldManager(sm, entity));
    datastore.put(entity);

    AbstractClassMetaData acmd = sm.getClassMetaData();
    // Set the generated key back on the pojo.
    // TODO(maxr): Ask Andy if this is a reasonable way to do this
    sm.setObjectField(
        (PersistenceCapable) sm.getObject(),
        acmd.getPKMemberPositions()[0],
        null,
        KeyFactory.encodeKey(entity.getKey()));
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementInsertCount();
    }
  }

  public void fetchObject(StateManager sm, int fieldNumbers[]) {
    ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
    DatastoreService datastore = (DatastoreService) mconn.getConnection();
    String value = (String) sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
    Entity entity;
    try {
      entity = datastore.get(KeyFactory.decodeKey(value));
    } catch (EntityNotFoundException e) {
      throw new NucleusObjectNotFoundException(
          "Could not retrieve entity of type " + sm.getClassMetaData().getFullClassName()
              + " with key " + value);
    }
    sm.replaceFields(fieldNumbers, new DatastoreFieldManager(sm, entity));
    if (storeMgr.getRuntimeManager() != null){
      storeMgr.getRuntimeManager().incrementFetchCount();
    }
  }

  /**
   * TODO (earmbrust): Find a way to get rid of the fetch before the update.
   */
  public void updateObject(StateManager sm, int fieldNumbers[]) {
    // Check if read-only so update not permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
    DatastoreService datastore = (DatastoreService) mconn.getConnection();
    StringIdentity ident = (StringIdentity) sm.getInternalObjectId();

    try {
      Entity entity = datastore.get(KeyFactory.decodeKey(ident.getKey()));
      sm.provideFields(fieldNumbers, new DatastoreFieldManager(sm, entity));
      datastore.put(entity);
    } catch (EntityNotFoundException e) {
      throw new NucleusObjectNotFoundException(
          "Could not retrieve entity of type " + sm.getClassMetaData().getFullClassName()
              + " with key " + ident.getKey());
    }

    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementUpdateCount();
    }
  }

  public void deleteObject(StateManager sm) {
    StringIdentity ident = (StringIdentity) sm.getInternalObjectId();
    datastoreService.delete(KeyFactory.decodeKey(ident.getKey()));
  }

  public void locateObject(StateManager sm) {
  }

  public Object findObject(ObjectManager om, Object id) {
    ClassLoaderResolver clr = om.getClassLoaderResolver();
    String className = storeMgr.getClassNameForObjectID(id, clr, om);
    // Generate a template object with these PK field values
    Class pcClass = clr.classForName(className,
        (id instanceof OID) ? null : id.getClass().getClassLoader());
    StateManager sm = StateManagerFactory.newStateManagerForHollow(om, pcClass, id);
    locateObject(sm);
    return sm.getObject();
  }
}
