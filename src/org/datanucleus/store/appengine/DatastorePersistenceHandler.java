// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

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

  /**
   * Magic property we use to signal to downstream writers that
   * they should write the entity associated with this property.
   * This has to do with the datastore constraint of only allowing
   * a single write per entity in a txn.  See
   * {@link DatastoreFieldManager#handleIndexFields()} for more info.
   */
  static final Object ENTITY_WRITE_DELAYED = "___entity_write_delayed___";

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
  public DatastoreTransaction getCurrentTransaction(ObjectManager om) {
    ManagedConnection mconn = storeMgr.getConnection(om);
    return ((EmulatedXAResource) mconn.getXAResource()).getCurrentTransaction();
  }

  private Entity getWithoutTxn(Key key) throws EntityNotFoundException {
    return datastoreService.get(key);
  }

  Entity get(DatastoreTransaction txn, Key key) {
    NucleusLogger.DATASTORE.debug("Getting entity of kind " + key.getKind() + " with key " + key);
    Entity entity;
    try {
      if (txn == null) {
        entity = datastoreService.get(key);
      } else {
        entity = datastoreService.get(txn.getInnerTxn(), key);
      }
      return entity;
    } catch (EntityNotFoundException e) {
      throw new NucleusObjectNotFoundException(
          "Could not retrieve entity of kind " + key.getKind() + " with key " + key);
    }
  }

  private Entity get(StateManager sm, Key key) {
    DatastoreTransaction txn = getCurrentTransaction(sm.getObjectManager());
    Entity entity = get(txn, key);
    setAssociatedEntity(sm, txn, entity);
    return entity;
  }

  private void put(StateManager sm, Entity entity) {
    DatastoreTransaction txn = put(sm.getObjectManager(), entity);
    setAssociatedEntity(sm, txn, entity);
  }

  DatastoreTransaction put(ObjectManager om, Entity entity) {
    if (NucleusLogger.DATASTORE.isDebugEnabled()) {
      NucleusLogger.DATASTORE.debug("Putting entity of kind " + entity.getKind() +
                                    " with key " + entity.getKey());
      for (Map.Entry<String, Object> entry : entity.getProperties().entrySet()) {
        NucleusLogger.DATASTORE.debug("  " + entry.getKey() + " : " + entry.getValue());
      }
    }
    DatastoreTransaction txn = getCurrentTransaction(om);
    if (txn == null) {
      datastoreService.put(entity);
    } else if (txn.getDeletedKeys().contains(entity.getKey())) {
      // entity was already deleted - just skip it
      // I'm a bit worried about swallowing user errors but we'll
      // see what bubbles up when we launch.  In theory we could
      // keep the entity that the user was deleting along with the key
      // and check to see if they changed anything between the
      // delete and the put.
    } else {
      Entity previouslyPut = txn.getPutEntities().get(entity.getKey());
      // It's ok to put if we haven't put this entity before or we have
      // and something has changed.  The reason we want to reput if something has
      // changed is that this will generate a datastore error, and we want users
      // to get this error because it means they have done something wrong.

      // TODO(maxr) Throw this exception ourselves with lots of good error detail.
      if (previouslyPut == null || !previouslyPut.getProperties().equals(entity.getProperties()))
        datastoreService.put(txn.getInnerTxn(), entity);
        txn.addPutEntity(entity);
    }
    return txn;
  }

  private void delete(ObjectManager om, Key key) {
    NucleusLogger.DATASTORE.debug("Deleting entity of kind " + key.getKind() + " with key " + key);
    DatastoreTransaction txn = getCurrentTransaction(om);
    if (txn == null) {
      datastoreService.delete(key);
    } else {
      datastoreService.delete(txn.getInnerTxn(), key);
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
    storeMgr.validateMetaDataForClass(sm.getClassMetaData(), sm.getObjectManager().getClassLoaderResolver());
    try {
      // Make sure writes are permitted
      storeMgr.assertReadOnlyForUpdateOfObject(sm);

      String kind = EntityUtils.determineKind(sm.getClassMetaData(), sm.getObjectManager());

      // For inserts we let the field manager create the Entity and then
      // retrieve it afterwards.  We do this because the entity isn't
      // 'fixed' until after provideFields has been called.
      DatastoreFieldManager fieldMgr = new DatastoreFieldManager(sm, kind, storeMgr);
      AbstractClassMetaData acmd = sm.getClassMetaData();
      sm.provideFields(acmd.getAllMemberPositions(), fieldMgr);

      Object assignedParentPk = fieldMgr.establishEntityGroup();
      Entity entity = fieldMgr.getEntity();
      if (fieldMgr.handleIndexFields()) {
        // signal to downstream writers that they should
        // insert this entity when they are invoked
        sm.setAssociatedValue(ENTITY_WRITE_DELAYED, entity);
      } else {
        handleVersioningBeforeWrite(sm, entity, VersionBehavior.INCREMENT, "updating");

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
          if (storeMgr.hasEncodedPKField(acmd)) {
            newObjectId = KeyFactory.keyToString(entity.getKey());
          } else {
            newObjectId = entity.getKey().getName();
          }
        } else if (pkType.equals(Long.class)) {
          newObjectId = entity.getKey().getId();
        } else {
          throw new IllegalStateException(
              "Primary key for type " + sm.getClassMetaData().getName()
                  + " is of unexpected type " + pkType.getName()
                  + " (must be String, Long, or " + Key.class.getName() + ")");
        }
        sm.setPostStoreNewObjectId(newObjectId);
        if (assignedParentPk != null) {
          // we automatically assigned a parent to the entity so make sure
          // that makes it back on to the pojo
          setPostStoreNewParent(sm, fieldMgr.getParentMemberMetaData(), assignedParentPk);
        }
        Integer pkIdPos = fieldMgr.getPkIdPos(); 
        if (pkIdPos != null) {
          setPostStorePkId(sm, pkIdPos, entity);
        }
        fieldMgr.storeRelations();

        if (storeMgr.getRuntimeManager() != null) {
          storeMgr.getRuntimeManager().incrementInsertCount();
        }
      }
    } finally {
      sm.setAssociatedValue(INSERTION_TOKEN, null);
    }
  }

  private void setPostStorePkId(StateManager sm, int fieldNumber, Entity entity) {
    sm.replaceField(fieldNumber, entity.getKey().getId(), true);
  }

  private void setPostStoreNewParent(
      StateManager sm, AbstractMemberMetaData parentMemberMetaData, Object assignedParentPk) {
    sm.replaceField(parentMemberMetaData.getAbsoluteFieldNumber(), assignedParentPk, true);
  }

  IdentifierFactory getIdentifierFactory(StateManager sm) {
    return ((MappedStoreManager)sm.getObjectManager().getStoreManager()).getIdentifierFactory();
  }

  private NucleusOptimisticException newNucleusOptimisticException(
      AbstractClassMetaData cmd, Entity entity, String op, String details) {
    return new NucleusOptimisticException(
        "Optimistic concurrency exception " + op + " " + cmd.getFullClassName()
            + " with pk " + entity.getKey() + ".  " + details);
  }

  private void handleVersioningBeforeWrite(StateManager sm, Entity entity,
      VersionBehavior versionBehavior, String op) {
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
              cmd, entity, op, "The underlying entity had already been deleted.");
        }
        if (!EntityUtils.getVersionFromEntity(
            getIdentifierFactory(sm), vmd, refreshedEntity).equals(curVersion)) {
          throw newNucleusOptimisticException(
              cmd, entity, op, "The underlying entity had already been updated.");
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
      if (storeMgr.hasEncodedPKField(sm.getClassMetaData())) {
        return KeyFactory.stringToKey((String) pk);
      } else {
        String kind = EntityUtils.determineKind(sm.getClassMetaData(), sm.getObjectManager());
        return KeyFactory.createKey(kind, (String) pk);
      }
    } else if (pk instanceof Long) {
      String kind = EntityUtils.determineKind(sm.getClassMetaData(), sm.getObjectManager());
      return KeyFactory.createKey(kind, (Long) pk);
    } else {
      throw new IllegalStateException(
          "Primary key for object of type " + sm.getClassMetaData().getName()
              + " is of unexpected type " + pk.getClass().getName()
              + " (must be String, Long, or " + Key.class.getName() + ")");
    }
  }

  public void setAssociatedEntity(StateManager sm, DatastoreTransaction txn, Entity entity) {
    // A StateManager can be used across multiple transactions, so we
    // want to associate the entity not just with a StateManager but a
    // StateManager and a transaction.
    sm.setAssociatedValue(txn, entity);
  }

  Entity getAssociatedEntityForCurrentTransaction(StateManager sm) {
    DatastoreTransaction txn = getCurrentTransaction(sm.getObjectManager());
    return (Entity) sm.getAssociatedValue(txn);
  }

  public void fetchObject(StateManager sm, int fieldNumbers[]) {
    if (fieldNumbers == null || fieldNumbers.length == 0) {
      return;
    }

    storeMgr.validateMetaDataForClass(
        sm.getClassMetaData(), sm.getObjectManager().getClassLoaderResolver());

    // We always fetch the entire object, so if the state manager
    // already has an associated Entity we know that associated
    // Entity has all the fields.
    Entity entity = getAssociatedEntityForCurrentTransaction(sm);
    if (entity == null) {
      Key pk = getPkAsKey(sm);
      entity = get(sm, pk);
    }
    sm.replaceFields(fieldNumbers, new DatastoreFieldManager(sm, storeMgr, entity, fieldNumbers));

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

    storeMgr.validateMetaDataForClass(
        sm.getClassMetaData(), sm.getObjectManager().getClassLoaderResolver());

    Entity entity = getAssociatedEntityForCurrentTransaction(sm);
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = getPkAsKey(sm);
      entity = get(sm, key);
    }
    DatastoreFieldManager fieldMgr = new DatastoreFieldManager(sm, storeMgr, entity, fieldNumbers);
    sm.provideFields(fieldNumbers, fieldMgr);
    handleVersioningBeforeWrite(sm, entity, VersionBehavior.INCREMENT, "updating");
    put(sm, entity);

    fieldMgr.storeRelations();

    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementUpdateCount();
    }
  }

  public void deleteObject(StateManager sm) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(sm);

    storeMgr.validateMetaDataForClass(
        sm.getClassMetaData(), sm.getObjectManager().getClassLoaderResolver());

    Entity entity = getAssociatedEntityForCurrentTransaction(sm);
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = getPkAsKey(sm);
      entity = get(sm, key);
    }

    ClassLoaderResolver clr = sm.getObjectManager().getClassLoaderResolver();
    DatastoreClass dc = storeMgr.getDatastoreClass(sm.getObject().getClass().getName(), clr);

    DatastoreTransaction txn = getCurrentTransaction(sm.getObjectManager());
    if (txn != null) {
      if (txn.getDeletedKeys().contains(entity.getKey())) {
        // need to check this _before_ we execute the dependent delete request
        // because that might set off a chain of cascades that could bring us
        // back here or to an update for the same entity, and we need to make
        // sure those recursive calls can see that we're already deleting
        // this entity.
        return;
      }
      txn.addDeletedKey(entity.getKey());
    }

    // first handle any dependent deletes
    DependentDeleteRequest req = new DependentDeleteRequest(dc, clr);
    req.execute(sm, entity);

    // now delete ourselves
    DatastoreFieldManager fieldMgr = new DatastoreFieldManager(sm, storeMgr, entity);
    AbstractClassMetaData acmd = sm.getClassMetaData();
    // stay away from the pk fields - this method sets fields to null and that's
    // not allowed for pks
    sm.provideFields(acmd.getNonPKMemberPositions(), fieldMgr);

    handleVersioningBeforeWrite(sm, entity, VersionBehavior.NO_INCREMENT, "deleting");
    delete(sm.getObjectManager(), getPkAsKey(sm));
  }

  public void locateObject(StateManager sm) {
    storeMgr.validateMetaDataForClass(
        sm.getClassMetaData(), sm.getObjectManager().getClassLoaderResolver());
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
