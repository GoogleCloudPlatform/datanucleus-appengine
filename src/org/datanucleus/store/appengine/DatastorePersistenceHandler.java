/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
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

import java.util.Collections;
import java.util.List;
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
      throw DatastoreExceptionTranslator.wrapEntityNotFoundException(e, key);
    }
  }

  private Entity get(StateManager sm, Key key) {
    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(sm.getObjectManager());
    Entity entity = get(txn, key);
    setAssociatedEntity(sm, txn, entity);
    return entity;
  }

  private void put(List<PutState> putStateList) {
    if (putStateList.isEmpty()) {
      return;
    }
    DatastoreTransaction txn = null;
    ObjectManager om = null;
    List<Entity> entityList = Utils.newArrayList();
    for (PutState putState : putStateList) {
      if (txn == null) {
        txn = EntityUtils.getCurrentTransaction(putState.sm.getObjectManager());
      }
      if (om == null) {
        om = putState.sm.getObjectManager();
      }
      entityList.add(putState.entity);
    }
    put(om, entityList);
    for (PutState putState : putStateList) {
      setAssociatedEntity(putState.sm, txn, putState.entity);
    }
  }

  private void put(StateManager sm, Entity entity) {
    DatastoreTransaction txn = put(sm.getObjectManager(), entity);
    setAssociatedEntity(sm, txn, entity);
  }

  DatastoreTransaction put(ObjectManager om, Entity entity) {
    return put(om, Collections.singletonList(entity));
  }

  private DatastoreTransaction put(ObjectManager om, List<Entity> entities) {
    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(om);
    List<Entity> putMe = Utils.newArrayList();
    for (Entity entity : entities) {
      if (NucleusLogger.DATASTORE.isDebugEnabled()) {
        NucleusLogger.DATASTORE.debug("Putting entity of kind " + entity.getKind() +
                                      " with key " + entity.getKey());
        for (Map.Entry<String, Object> entry : entity.getProperties().entrySet()) {
          NucleusLogger.DATASTORE.debug("  " + entry.getKey() + " : " + entry.getValue());
        }
      }
      if (txn == null) {
        putMe.add(entity);
      } else {
        if (txn.getDeletedKeys().contains(entity.getKey())) {
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
          if (previouslyPut == null || !previouslyPut.getProperties().equals(entity.getProperties())) {
            putMe.add(entity);
          }
        }
      }
    }
    if (!putMe.isEmpty()) {
      if (txn == null) {
        if (putMe.size() == 1) {
          datastoreService.put(putMe.get(0));
        } else {
          datastoreService.put(putMe);
        }
      } else {
        Transaction innerTxn = txn.getInnerTxn();
        if (putMe.size() == 1) {
          datastoreService.put(innerTxn, putMe.get(0));
        } else {
          datastoreService.put(innerTxn, putMe);
        }
        txn.addPutEntities(putMe);
      }
    }
    return txn;
  }

  void delete(DatastoreTransaction txn, List<Key> keys) {
    NucleusLogger.DATASTORE.debug("Deleting entities with keys " + keys);
    if (txn == null) {
      if (keys.size() == 1) {
        datastoreService.delete(keys.get(0));
      } else {
        datastoreService.delete(keys);
      }
    } else {
      Transaction innerTxn = txn.getInnerTxn();
      if (keys.size() == 1) {
        datastoreService.delete(innerTxn, keys.get(0));
      } else {
        datastoreService.delete(innerTxn, keys);
      }
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
    // If we're in the middle of a batch operation just register
    // the statemanager that needs the insertion
    if (storeMgr.getBatchInsertManager().batchOperationInProgress()) {
      storeMgr.getBatchInsertManager().add(sm);
      return;
    }
    insertObjects(Collections.singletonList(sm));
  }

  /**
   * If we're inserting multiple objects we want to use the low-level batch put
   * mechanism.  This method pre-processes all the state managers, gathering
   * up all the info needed to do the put, does the put for all the state
   * managers at once, and then does post processing on all the state managers.
   */
  void insertObjects(List<StateManager> stateManagersToInsert) {
    try {
      List<PutState> putStateList = insertPreProcess(stateManagersToInsert);
      // Save the parent entities first so we can have a key to use as a parent
      // on owned child entities.
      put(putStateList);

      insertPostProcess(putStateList);
    } finally {
      for (StateManager sm : stateManagersToInsert) {
        sm.setAssociatedValue(INSERTION_TOKEN, null);
      }
    }
  }

  private void insertPostProcess(List<PutState> putStateList) {
    // Post-processing for all puts
    for (PutState putState : putStateList) {
      // Set the generated key back on the pojo.  If the pk field is a Key just
      // set it on the field directly.  If the pk field is a String, convert the Key
      // to a String.  If the pk field is anything else, we've got a problem.
      // Assumes we only have a single pk member position
      Class<?> pkType =
          putState.acmd.getMetaDataForManagedMemberAtAbsolutePosition(
              putState.acmd.getPKMemberPositions()[0]).getType();

      Object newObjectId;
      if (pkType.equals(Key.class)) {
        newObjectId = putState.entity.getKey();
      } else if (pkType.equals(String.class)) {
        if (DatastoreManager.hasEncodedPKField(putState.acmd)) {
          newObjectId = KeyFactory.keyToString(putState.entity.getKey());
        } else {
          newObjectId = putState.entity.getKey().getName();
        }
      } else if (pkType.equals(Long.class)) {
        newObjectId = putState.entity.getKey().getId();
      } else {
        throw new IllegalStateException(
            "Primary key for type " + putState.sm.getClassMetaData().getName()
                + " is of unexpected type " + pkType.getName()
                + " (must be String, Long, or " + Key.class.getName() + ")");
      }
      putState.sm.setPostStoreNewObjectId(newObjectId);
      if (putState.assignedParentPk != null) {
        // we automatically assigned a parent to the entity so make sure
        // that makes it back on to the pojo
        setPostStoreNewParent(
            putState.sm, putState.fieldMgr.getParentMemberMetaData(), putState.assignedParentPk);
      }
      Integer pkIdPos = putState.fieldMgr.getPkIdPos();
      if (pkIdPos != null) {
        setPostStorePkId(putState.sm, pkIdPos, putState.entity);
      }
      putState.fieldMgr.storeRelations();

      if (storeMgr.getRuntimeManager() != null) {
        storeMgr.getRuntimeManager().incrementInsertCount();
      }
    }
  }

  private List<PutState> insertPreProcess(List<StateManager> stateManagersToInsert) {
    List<PutState> putStateList = Utils.newArrayList();
    for (StateManager sm : stateManagersToInsert) {
      if (sm.getAssociatedValue(INSERTION_TOKEN) != null) {
        // already inserting the pc associated with this state manager
        continue;
      }
      // set the token so if we recurse down to the same state manager we know
      // we're already inserting
      sm.setAssociatedValue(INSERTION_TOKEN, INSERTION_TOKEN);
      storeMgr.validateMetaDataForClass(
          sm.getClassMetaData(), sm.getObjectManager().getClassLoaderResolver());
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
        continue;
      } else {
        handleVersioningBeforeWrite(sm, entity, VersionBehavior.INCREMENT, "updating");
      }
      // Add the state for this put to the list.
      putStateList.add(new PutState(sm, fieldMgr, acmd, assignedParentPk, entity));
    }
    return putStateList;
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
          refreshedEntity = datastoreService.get(entity.getKey());
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
  private static Object getPk(StateManager sm) {
    return sm.getObjectManager().getApiAdapter()
        .getTargetKeyForSingleFieldIdentity(sm.getInternalObjectId());
  }

  private static Key getPkAsKey(StateManager sm) {
    Object pk = getPk(sm);
    if (pk == null) {
      throw new IllegalStateException(
          "Primary key for object of type " + sm.getClassMetaData().getName() + " is null.");
    }
    return EntityUtils.getPkAsKey(pk, sm.getClassMetaData(), sm.getObjectManager());
  }

  public void setAssociatedEntity(StateManager sm, DatastoreTransaction txn, Entity entity) {
    // A StateManager can be used across multiple transactions, so we
    // want to associate the entity not just with a StateManager but a
    // StateManager and a transaction.
    sm.setAssociatedValue(txn, entity);
  }

  Entity getAssociatedEntityForCurrentTransaction(StateManager sm) {
    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(sm.getObjectManager());
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

    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(sm.getObjectManager());
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
    DatastoreFieldManager fieldMgr = new DatastoreDeleteFieldManager(sm, storeMgr, entity);
    AbstractClassMetaData acmd = sm.getClassMetaData();
    // stay away from the pk fields - this method sets fields to null and that's
    // not allowed for pks
    sm.provideFields(acmd.getNonPKMemberPositions(), fieldMgr);

    handleVersioningBeforeWrite(sm, entity, VersionBehavior.NO_INCREMENT, "deleting");
    Key keyToDelete = getPkAsKey(sm);
    // If we're in the middle of a batch operation just register
    // the statemanager that needs the delete
    BatchDeleteManager bdm = storeMgr.getBatchDeleteManager();
    if (bdm.batchOperationInProgress()) {
      bdm.add(new BatchDeleteManager.BatchDeleteState(txn, keyToDelete));
      return;
    }
    delete(txn, Collections.singletonList(keyToDelete));
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

  /**
   * All the information needed to perform a put on an Entity.
   */
  private static final class PutState {
    private final StateManager sm;
    private final DatastoreFieldManager fieldMgr;
    private final AbstractClassMetaData acmd;
    private final Object assignedParentPk;
    private final Entity entity;

    private PutState(StateManager sm,
          DatastoreFieldManager fieldMgr, AbstractClassMetaData acmd, Object assignedParentPk,
          Entity entity) {
      this.sm = sm;
      this.fieldMgr = fieldMgr;
      this.acmd = acmd;
      this.assignedParentPk = assignedParentPk;
      this.entity = entity;
    }
  }
}
