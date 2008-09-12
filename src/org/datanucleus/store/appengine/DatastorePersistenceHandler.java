// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Transaction;

import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.StorePersistenceHandler;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.IdentifierFactory;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastorePersistenceHandler implements StorePersistenceHandler {

  private final DatastoreService datastoreService =
      DatastoreServiceFactoryInternal.getDatastoreService();
  private final DatastoreManager storeMgr;

  public DatastorePersistenceHandler(StoreManager storeMgr) {
    this.storeMgr = (DatastoreManager) storeMgr;
  }

  private enum VersionBehavior { INCREMENT, NO_INCREMENT }

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

  private Entity getWithoutTxn(Key key) throws EntityNotFoundException {
    return datastoreService.get(key);
  }

  private Entity get(StateManager sm, Key key) {
    try {
      if (isNontransactionalRead(sm)) {
        return getWithoutTxn(key);
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
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    String kind = determineKind(sm);

    // For inserts we let the field manager create the Entity and then
    // retrieve it afterwards.  We do this because the entity isn't
    // 'fixed' until after provideFields has been called.
    DatastoreFieldManager fieldMgr = new DatastoreFieldManager(sm, kind);
    AbstractClassMetaData acmd = sm.getClassMetaData();
    sm.provideFields(acmd.getAllMemberPositions(), fieldMgr);
    Entity entity = fieldMgr.getEntity();
    handleVersioningBeforeWrite(sm, entity, VersionBehavior.INCREMENT);
    // TODO(earmbrust): Allow for non-transactional read/write.
    put(sm, entity);

    // Set the generated key back on the pojo.
    sm.setPostStoreNewObjectId(KeyFactory.encodeKey(entity.getKey()));
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementInsertCount();
    }
  }

  private String determineKind(StateManager sm) {
    AbstractClassMetaData acmd = sm.getClassMetaData();
    if (acmd.getTable() != null) {
      // User specified a table name as part of the mapping so use that as the
      // kind.
      return acmd.getTable();
    }
    // No table name provided so use the identifier factory to convert the
    // class name into the kind.
    ClassLoaderResolver clr = sm.getObjectManager().getClassLoaderResolver();
    return getIdentifierFactory(sm).newDatastoreContainerIdentifier(clr, acmd).getIdentifier();
  }

  IdentifierFactory getIdentifierFactory(StateManager sm) {
    return ((MappedStoreManager)sm.getObjectManager().getStoreManager()).getIdentifierFactory();
  }

  private NucleusOptimisticException newNucleusOptimisticException(
      AbstractClassMetaData cmd, Entity entity, String details) {
    return new NucleusOptimisticException(
        "Optimistic concurrency exception updating " + cmd.getFullClassName()
            + " with pk " + KeyFactory.encodeKey(entity.getKey()) + ".  " + details);
  }

  private void handleVersioningBeforeWrite(StateManager sm, Entity entity,
      VersionBehavior versionBehavior) {
    AbstractClassMetaData cmd = sm.getClassMetaData();
    if (cmd.hasVersionStrategy()) {
      VersionMetaData vmd = cmd.getVersionMetaData();
      Object curVersion = sm.getObjectManager().getApiAdapter().getVersion(sm);
      if (curVersion != null) {
        // Fetch the latest and greatest version of the entity from the datastore
        // to see if anyone has made a change underneath us.  We need to execute
        // the fetch outside a txn to guarantee that we see the latest version.
        Entity refreshedEntity;
        try {
          refreshedEntity = getWithoutTxn(entity.getKey());
        } catch (EntityNotFoundException e) {
          // someone deleted out from under us
          throw newNucleusOptimisticException(
              cmd, entity, "The underlying entity had already been deleted.");
        }
        if (!getVersionFromEntity(sm, vmd, refreshedEntity).equals(curVersion)) {
          throw newNucleusOptimisticException(
              cmd, entity, "The underlying entity had already been updated.");
        }
      }
      Object nextVersion = cmd.getVersionMetaData().getNextVersion(curVersion);

      sm.setTransactionalVersion(nextVersion);
      String versionPropertyName = getVersionPropertyName(sm, vmd);
      entity.setProperty(versionPropertyName, nextVersion);

      // Version field - update the version on the object
      if (versionBehavior == VersionBehavior.INCREMENT && vmd.getFieldName() != null) {
        AbstractMemberMetaData verfmd =
            ((AbstractClassMetaData)vmd.getParent()).getMetaDataForMember(vmd.getFieldName());
        sm.replaceField(verfmd.getAbsoluteFieldNumber(), nextVersion, false);
      }
    }
  }

  private Object getVersionFromEntity(StateManager sm, VersionMetaData vmd, Entity entity) {
    return entity.getProperty(getVersionPropertyName(sm, vmd));
  }

  private String getVersionPropertyName(StateManager sm, VersionMetaData vmd) {
    ColumnMetaData[] columnMetaData = vmd.getColumnMetaData();
    if (columnMetaData == null || columnMetaData.length == 0) {
      return getIdentifierFactory(sm).newVersionFieldIdentifier().getIdentifier();
    }
    if (columnMetaData.length != 1) {
      throw new IllegalArgumentException(
          "Please specify 0 or 1 column name for the version property.");
    }
    return columnMetaData[0].getName();
  }

  private String getPk(StateManager sm) {
    return (String) sm.getObjectManager().getApiAdapter()
        .getTargetKeyForSingleFieldIdentity(sm.getInternalObjectId());
  }

  public void fetchObject(StateManager sm, int fieldNumbers[]) {
    String pk = getPk(sm);
    // We always fetch the entire object, so if the state manager
    // already has an associated Entity we know that associated
    // Entity has all the fields.
    Entity entity = (Entity) sm.getAssociatedValue(pk);
    if (entity == null) {
      entity = get(sm, KeyFactory.decodeKey(pk));
      sm.setAssociatedValue(pk, entity);
    }
    sm.replaceFields(fieldNumbers, new DatastoreFieldManager(sm, entity));

    AbstractClassMetaData cmd = sm.getClassMetaData();
    if (cmd.hasVersionStrategy()) {
      sm.setTransactionalVersion(getVersionFromEntity(sm, cmd.getVersionMetaData(), entity));
    }
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementFetchCount();
    }
  }

  public void updateObject(StateManager sm, int fieldNumbers[]) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    Entity entity = (Entity) sm.getAssociatedValue(getPk(sm));
    sm.provideFields(fieldNumbers, new DatastoreFieldManager(sm, entity));
    handleVersioningBeforeWrite(sm, entity, VersionBehavior.INCREMENT);
    put(sm, entity);

    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementUpdateCount();
    }
  }

  public void deleteObject(StateManager sm) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    Entity entity = (Entity) sm.getAssociatedValue(getPk(sm));
    handleVersioningBeforeWrite(sm, entity, VersionBehavior.NO_INCREMENT);
    delete(sm, KeyFactory.decodeKey(getPk(sm)));
  }

  public void locateObject(StateManager sm) {
    // get throws NucleusObjectNotFoundException if the entity isn't found,
    // which is what we want.
    get(sm, KeyFactory.decodeKey(getPk(sm)));
  }

  /**
   * Implementation of this operation is optional and is intended for
   * datastores that instantiate the model objects themselves (as opposed
   * to letting datanucleus do it).  The App Engine datastore let's
   * datanucleus instantiate the model objects so we just return null.
   */
  public Object findObject(ObjectManager om, Object id) {
    return null;
  }

  public void close() {
  }
}
