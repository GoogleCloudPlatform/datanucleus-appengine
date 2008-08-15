// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import javax.jdo.identity.StringIdentity;
import javax.jdo.spi.PersistenceCapable;

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

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Transaction;

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

  private boolean isNontransactionalRead(StateManager sm) {
    org.datanucleus.Transaction txn = sm.getObjectManager().getTransaction();
    return txn.getNontransactionalRead();
  }

  private boolean isNontransactionalWrite(StateManager sm) {
    org.datanucleus.Transaction txn = sm.getObjectManager().getTransaction();
    return txn.getNontransactionalWrite();
  }

  private Transaction getCurrentTransaction(StateManager sm) {
    ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
    return ((EmulatedXAResource) mconn.getXAResource()).getCurrentTransaction();
  }

  private Entity get(StateManager sm, Key key) {
    try {
      if (isNontransactionalRead(sm)) {
        return datastoreService.get(key);
      } else {
        return datastoreService.get(getCurrentTransaction(sm), key);
      }
    } catch (EntityNotFoundException e) {
      throw new NucleusObjectNotFoundException(
          "Could not retrieve entity of type " + sm.getClassMetaData().getName()
          + " with key " + KeyFactory.encodeKey(key));
    }
  }

  private void put(StateManager sm, Entity entity) {
    if (isNontransactionalWrite(sm)) {
      datastoreService.put(entity);
    } else {
      datastoreService.put(getCurrentTransaction(sm), entity);
    }
  }

  private void delete(StateManager sm, Key key) {
    if (isNontransactionalWrite(sm)) {
      datastoreService.delete(key);
    } else {
      datastoreService.delete(getCurrentTransaction(sm), key);
    }
  }

  public void insertObject(StateManager sm) {
    // Check if read-only so update not permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    int[] fieldNumbers = sm.getClassMetaData().getAllMemberPositions();
    // TODO(maxr): Hook into mechanism so that kind is not tied to fqn.
    // TODO(maxr): Figure out how to deal with ancestors.
    Entity entity = new Entity(sm.getClassMetaData().getFullClassName());
    sm.provideFields(fieldNumbers, new DatastoreFieldManager(sm, entity));
    // TODO(earmbrust): Allow for non-transactional read/write.
    put(sm, entity);

    AbstractClassMetaData acmd = sm.getClassMetaData();
    // Set the generated key back on the pojo.
    // TODO(maxr): Ask Andy if this is a reasonable way to do this
    sm.setObjectField((PersistenceCapable) sm.getObject(), acmd
        .getPKMemberPositions()[0], null, KeyFactory.encodeKey(entity
        .getKey()));
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementInsertCount();
    }
  }

  public void fetchObject(StateManager sm, int fieldNumbers[]) {
    String value = (String) sm.provideField(sm.getClassMetaData()
        .getPKMemberPositions()[0]);
    Entity entity = get(sm, KeyFactory.decodeKey(value));
    sm.replaceFields(fieldNumbers, new DatastoreFieldManager(sm, entity));
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementFetchCount();
    }
  }

  /**
   * TODO (earmbrust): Find a way to get rid of the fetch before the update.
   */
  public void updateObject(StateManager sm, int fieldNumbers[]) {
    // Check if read-only so update not permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    StringIdentity ident = (StringIdentity) sm.getInternalObjectId();
    Entity entity = get(sm, KeyFactory.decodeKey(ident.getKey()));
    sm.provideFields(fieldNumbers, new DatastoreFieldManager(sm, entity));
    put(sm, entity);

    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementUpdateCount();
    }
  }

  public void deleteObject(StateManager sm) {
    StringIdentity ident = (StringIdentity) sm.getInternalObjectId();
    delete(sm, KeyFactory.decodeKey(ident.getKey()));
  }

  public void locateObject(StateManager sm) {
  }

  public Object findObject(ObjectManager om, Object id) {
    ClassLoaderResolver clr = om.getClassLoaderResolver();
    String className = storeMgr.getClassNameForObjectID(id, clr, om);
    // Generate a template object with these PK field values
    Class pcClass = clr.classForName(className, (id instanceof OID) ? null : id
        .getClass().getClassLoader());
    StateManager sm = StateManagerFactory.newStateManagerForHollow(om, pcClass,
        id);
    locateObject(sm);
    return sm.getObject();
  }
}
