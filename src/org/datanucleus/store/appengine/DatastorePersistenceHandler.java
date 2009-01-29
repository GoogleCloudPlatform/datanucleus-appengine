// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.StorePersistenceHandler;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.util.NucleusLogger;

import java.util.Map;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastorePersistenceHandler implements StorePersistenceHandler {

  private final DatastoreService datastoreService =
      DatastoreServiceFactoryInternal.getDatastoreService();
  private final DatastoreManager storeMgr;

  /**
   * Constructor
   *
   * @param storeMgr The StoreManager to use.
   */
  public DatastorePersistenceHandler(StoreManager storeMgr) {
    this.storeMgr = (DatastoreManager) storeMgr;
  }

  private enum VersionBehavior { INCREMENT, NO_INCREMENT }

  /**
   * Get the active transaction.  Depending on the connection factory
   * associated with the store manager, this may establish a transaction if one
   * is not currently active.  This method will return null if the connection
   * factory associated with the store manager is nontransactional
   */
  private Transaction getCurrentTransaction(ObjectManager om) {
    ManagedConnection mconn = storeMgr.getConnection(om);
    return ((EmulatedXAResource) mconn.getXAResource()).getCurrentTransaction();
  }

  private Entity getWithoutTxn(Key key) throws EntityNotFoundException {
    return datastoreService.get(key);
  }

  private Entity get(StateManager sm, Key key) {
    NucleusLogger.DATASTORE.debug("Getting entity with key " + key);
    Entity entity;
    try {
      Transaction txn = getCurrentTransaction(sm.getObjectManager());
      if (txn == null) {
        entity = datastoreService.get(key);
      } else {
        entity = datastoreService.get(txn, key);
      }
      setAssociatedEntity(sm, txn, entity);
      return entity;
    } catch (EntityNotFoundException e) {
      throw new NucleusObjectNotFoundException(
          "Could not retrieve entity of type " + sm.getClassMetaData().getName()
          + " with key " + KeyFactory.encodeKey(key));
    }
  }

  private void put(StateManager sm, Entity entity) {
    if (NucleusLogger.DATASTORE.isDebugEnabled()) {
      NucleusLogger.DATASTORE.debug("Putting entity with key " + entity.getKey());
      for (Map.Entry<String, Object> entry : entity.getProperties().entrySet()) {
        NucleusLogger.DATASTORE.debug("  " + entry.getKey() + " : " + entry.getValue());
      }
    }
    Transaction txn = getCurrentTransaction(sm.getObjectManager());
    if (txn == null) {
      datastoreService.put(entity);
    } else {
      datastoreService.put(txn, entity);
    }
    setAssociatedEntity(sm, txn, entity);
  }

  private void delete(StateManager sm, Key key) {
    NucleusLogger.DATASTORE.debug("Deleting entity with key " + key);
    Transaction txn = getCurrentTransaction(sm.getObjectManager());
    if (txn == null) {
      datastoreService.delete(key);
    } else {
      datastoreService.delete(txn, key);
    }
  }

  /**
   * Token used to make sure we don't try to insert the pc associated with a
   * state manager more than once.  This can happen in the case of a
   * bi-directional one-to-one where we add a child to an existing parent and
   * call merge() on the parent.  In this scenario we receive a call
   * to {@link #insertObject(StateManager)} with the state
   * manager for the new child.  When we invoke
   * {@link StateManager#provideFields(int[], FieldManager)}
   * we will recurse back to the parent field on the child (remember,
   * bidirectional relationship), which will then recurse back to the child.
   * Since the child has not yet been inserted, insertObject will be invoked
   * _again_ and we end up creating 2 instances of the child in the datastore,
   * which is not good.  There are probably better ways to solve this problem,
   * but for now this looks ok.  We're making the assumption that state
   * managers are only accessed by a single thread at a time.
   */
  private static final Object INSERTION_TOKEN = new Object();

  public void insertObject(StateManager sm) {
    if (sm.getAssociatedValue(INSERTION_TOKEN) != null) {
      // already inserting the pc associated with this state manager
      return;
    }
    // set the token so if we recurse down to the same state manager we know
    // we're already inserting
    sm.setAssociatedValue(INSERTION_TOKEN, INSERTION_TOKEN);

    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    String kind = EntityUtils.determineKind(sm.getClassMetaData(), sm.getObjectManager());

    // For inserts we let the field manager create the Entity and then
    // retrieve it afterwards.  We do this because the entity isn't
    // 'fixed' until after provideFields has been called.
    DatastoreFieldManager fieldMgr = new DatastoreFieldManager(sm, kind, storeMgr);
    AbstractClassMetaData acmd = sm.getClassMetaData();
    sm.provideFields(acmd.getAllMemberPositions(), fieldMgr);
    InsertMappingConsumer consumer = fieldMgr.getInsertMappingConsumer();

    Object assignedAncestorPk = fieldMgr.establishEntityGroup(consumer);
    Entity entity = fieldMgr.getEntity();
    fieldMgr.handleHiddenFields(consumer);
    handleVersioningBeforeWrite(sm, entity, VersionBehavior.INCREMENT);

    // TODO(earmbrust): Allow for non-transactional read/write.

    // Save the parent entity first so we can have a key to use as a parent
    // on owned child entities.
    put(sm, entity);
    // Set the generated key back on the pojo.  If the pk field is a Key just
    // set it on the field directly.  If the pk field is a String, convert the Key
    // to a String.  If the pk field is anything else, we've got a problem.
    // Assumes we only have a single pk member position
    Class<?> pkType =
        acmd.getMetaDataForManagedMemberAtPosition(acmd.getPKMemberPositions()[0]).getType();

    Object newObjectId;
    if (pkType.equals(Key.class)) {
      newObjectId = entity.getKey();
    } else if (pkType.equals(String.class)) {
      newObjectId = KeyFactory.encodeKey(entity.getKey());
    } else {
      throw new IllegalStateException(
          "Primary key for type " + sm.getClassMetaData().getName()
              + " is of unexpected type " + pkType.getName()
              + " (must be String or " + Key.class.getName() + ")");
    }
    sm.setPostStoreNewObjectId(newObjectId);
    if (assignedAncestorPk != null) {
      // we automatically assigned an ancestor to the entity so make sure
      // that makes it back on to the pojo
      setPostStoreNewAncestor(sm, fieldMgr.getAncestorMemberMetaData(), assignedAncestorPk);
    }

    storeRelations(fieldMgr, sm, entity);

    // now safe to clear the insertion token
    sm.setAssociatedValue(INSERTION_TOKEN, null);

    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementInsertCount();
    }
  }

  private void setPostStoreNewAncestor(
      StateManager sm, AbstractMemberMetaData ancestorMemberMetaData, Object assignedAncestorPk) {
    sm.replaceField(ancestorMemberMetaData.getAbsoluteFieldNumber(), assignedAncestorPk, true);
  }

  private void storeRelations(DatastoreFieldManager fieldMgr, StateManager sm, Entity entity) {
    if (fieldMgr.storeRelations()) {
      // Return value of true means that storing the relations resulted in
      // changes to the parent object.  That means we need to re-save the parent.
      // TODO(maxr): Look into exposing a datastore mechanism for assigning a
      // Key and nothing else.  This would allow us to put children that need
      // the parent Key before we put the parent, and then we'd only need to
      // put the parent once at the end.
      ClassLoaderResolver clr = sm.getObjectManager().getClassLoaderResolver();
      sm.provideFields(sm.getClassMetaData().getRelationMemberPositions(clr), fieldMgr);
      put(sm, entity);
    }
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
        NucleusLogger.DATASTORE.debug("Getting entity with key " + entity.getKey());
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
        if (!EntityUtils.getVersionFromEntity(
            getIdentifierFactory(sm), vmd, refreshedEntity).equals(curVersion)) {
          throw newNucleusOptimisticException(
              cmd, entity, "The underlying entity had already been updated.");
        }
      }
      Object nextVersion = cmd.getVersionMetaData().getNextVersion(curVersion);

      sm.setTransactionalVersion(nextVersion);
      String versionPropertyName =
          EntityUtils.getVersionPropertyName(getIdentifierFactory(sm), vmd);
      entity.setProperty(versionPropertyName, nextVersion);

      // Version field - update the version on the object
      if (versionBehavior == VersionBehavior.INCREMENT && vmd.getFieldName() != null) {
        AbstractMemberMetaData verfmd =
            ((AbstractClassMetaData)vmd.getParent()).getMetaDataForMember(vmd.getFieldName());
        sm.replaceField(verfmd.getAbsoluteFieldNumber(), nextVersion, false);
      }
    }
  }

  /**
   * Get the primary key of the object associated with the provided
   * state manager.  Can return {@code null}.
   */
  private Object getPk(StateManager sm) {
    return sm.getObjectManager().getApiAdapter()
        .getTargetKeyForSingleFieldIdentity(sm.getInternalObjectId());
  }

  private Key getPkAsKey(StateManager sm) {
    Object pk = getPk(sm);
    if (pk == null) {
      throw new IllegalStateException(
          "Primary key for object of type " + sm.getClassMetaData().getName() + " is null.");
    } else if (pk instanceof Key) {
      return (Key) pk;
    } else if (pk instanceof String) {
      return KeyFactory.decodeKey((String) pk);
    } else {
      throw new IllegalStateException(
          "Primary key for object of type " + sm.getClassMetaData().getName()
              + " is of unexpected type " + pk.getClass().getName()
              + " (must be String or " + Key.class.getName() + ")");
    }
  }

  private void setAssociatedEntity(StateManager sm, Transaction txn, Entity entity) {
    // A StateManager can be used across multiple transactions, so we really
    // want to associate the entity not just with a StateManager but a
    // StateManager and a transaction.
    sm.setAssociatedValue(txn, entity);
  }

  Entity getAssociatedEntityForCurrentTransaction(StateManager sm) {
    return (Entity) sm.getAssociatedValue(getCurrentTransaction(sm.getObjectManager()));
  }

  public void fetchObject(StateManager sm, int fieldNumbers[]) {
    if (fieldNumbers == null || fieldNumbers.length == 0) {
      return;
    }

    // We always fetch the entire object, so if the state manager
    // already has an associated Entity we know that associated
    // Entity has all the fields.
    Entity entity = getAssociatedEntityForCurrentTransaction(sm);
    if (entity == null) {
      Key pk = getPkAsKey(sm);
      entity = get(sm, pk);
    }
    sm.replaceFields(fieldNumbers, new DatastoreFieldManager(sm, storeMgr, entity));

    AbstractClassMetaData cmd = sm.getClassMetaData();
    if (cmd.hasVersionStrategy()) {
      sm.setTransactionalVersion(
          EntityUtils.getVersionFromEntity(
              getIdentifierFactory(sm), cmd.getVersionMetaData(), entity));
    }
    runPostFetchMappingCallbacks(sm, fieldNumbers);

    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementFetchCount();
    }
  }

  private void runPostFetchMappingCallbacks(StateManager sm, int[] fieldNumbers) {
    AbstractMemberMetaData[] fmds = new AbstractMemberMetaData[fieldNumbers.length];
    for (int i = 0; i < fmds.length; i++) {
      fmds[i] =
          sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
    }

    ClassLoaderResolver clr = sm.getObjectManager().getClassLoaderResolver();
    DatastoreClass dc = storeMgr.getDatastoreClass(sm.getObject().getClass().getName(), clr);
    FetchMappingConsumer consumer = new FetchMappingConsumer(sm.getClassMetaData());
    dc.provideMappingsForMembers(consumer, fmds, true);
    dc.provideDatastoreIdMappings(consumer);
    dc.providePrimaryKeyMappings(consumer);
    for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
      callback.postFetch(sm);
    }
  }

  public void updateObject(StateManager sm, int fieldNumbers[]) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    Entity entity = getAssociatedEntityForCurrentTransaction(sm);
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = getPkAsKey(sm);
      entity = get(sm, key);
    }
    DatastoreFieldManager fieldMgr = new DatastoreFieldManager(sm, storeMgr, entity);
    sm.provideFields(fieldNumbers, fieldMgr);
    handleVersioningBeforeWrite(sm, entity, VersionBehavior.INCREMENT);
    put(sm, entity);

    storeRelations(fieldMgr, sm, entity);

    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementUpdateCount();
    }
  }

  public void deleteObject(StateManager sm) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    Entity entity = getAssociatedEntityForCurrentTransaction(sm);
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = getPkAsKey(sm);
      entity = get(sm, key);
    }

    ClassLoaderResolver clr = sm.getObjectManager().getClassLoaderResolver();
    DatastoreClass dc = storeMgr.getDatastoreClass(sm.getObject().getClass().getName(), clr);

    // first handle any dependent deletes
    DependentDeleteRequest req = new DependentDeleteRequest(dc, clr);
    req.execute(sm, entity);

    // now delete ourselves
    DatastoreFieldManager fieldMgr = new DatastoreFieldManager(sm, storeMgr, entity);
    AbstractClassMetaData acmd = sm.getClassMetaData();
    sm.provideFields(acmd.getAllMemberPositions(), fieldMgr);

    handleVersioningBeforeWrite(sm, entity, VersionBehavior.NO_INCREMENT);
    delete(sm, getPkAsKey(sm));
  }

  public void locateObject(StateManager sm) {
    // get throws NucleusObjectNotFoundException if the entity isn't found,
    // which is what we want.
    get(sm, getPkAsKey(sm));
  }

  /**
   * Implementation of this operation is optional and is intended for
   * datastores that instantiate the model objects themselves (as opposed
   * to letting datanucleus do it).  The App Engine datastore lets
   * datanucleus instantiate the model objects so we just return null.
   */
  public Object findObject(ObjectManager om, Object id) {
    return null;
  }

  public void close() {
  }
}
