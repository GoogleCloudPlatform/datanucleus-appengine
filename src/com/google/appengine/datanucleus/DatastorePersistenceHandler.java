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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.datanucleus.mapping.DatastoreTable;
import com.google.appengine.datanucleus.mapping.DependentDeleteRequest;
import com.google.appengine.datanucleus.mapping.FetchMappingConsumer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.PersistenceBatchType;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastorePersistenceHandler extends AbstractPersistenceHandler {

  protected static final Localiser GAE_LOCALISER = Localiser.getInstance(
      "com.google.appengine.datanucleus.Localisation", DatastoreManager.class.getClassLoader());

  /**
   * Magic property we use to signal to downstream writers that
   * they should write the entity associated with this property.
   * This has to do with the datastore constraint of only allowing
   * a single write per entity in a txn.  See
   * {@link DatastoreFieldManager#handleIndexFields()} for more info.
   */
  public static final Object ENTITY_WRITE_DELAYED = "___entity_write_delayed___";

  /**
   * Magic property that we use to signal that an associated child object
   * does not yet have a key.  This tells us that we can skip re-putting
   * the parent entity because there are still children that need to be
   * written first.
   */
  public static final String MISSING_RELATION_KEY = "___missing_relation_key___";

  private final DatastoreService datastoreServiceForReads;
  private final DatastoreService datastoreServiceForWrites;
  private final DatastoreManager storeMgr;

  /**
   * Constructor.
   * @param storeMgr The StoreManager to use.
   */
  public DatastorePersistenceHandler(StoreManager storeMgr) {
    this.storeMgr = (DatastoreManager) storeMgr;
    datastoreServiceForReads = DatastoreServiceFactoryInternal.getDatastoreService(
        this.storeMgr.getDefaultDatastoreServiceConfigForReads());
    datastoreServiceForWrites = DatastoreServiceFactoryInternal.getDatastoreService(
        this.storeMgr.getDefaultDatastoreServiceConfigForWrites());
  }

  public void close() {}

  private enum VersionBehavior { INCREMENT, NO_INCREMENT }

  @Override
  public boolean useReferentialIntegrity() {
    // This informs DataNucleus that the store requires ordered flushes, so the order of receiving dirty
    // requests is preserved when flushing them
    return true;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.AbstractPersistenceHandler#batchStart(org.datanucleus.store.ExecutionContext, org.datanucleus.store.PersistenceBatchType)
   */
  @Override
  public void batchStart(ExecutionContext ec, PersistenceBatchType batchType) {
    if (batchType == PersistenceBatchType.PERSIST) {
      storeMgr.getBatchPutManager().start();
    }
    else if (batchType == PersistenceBatchType.DELETE) {
      storeMgr.getBatchDeleteManager().start();
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.AbstractPersistenceHandler#batchEnd(org.datanucleus.store.ExecutionContext, org.datanucleus.store.PersistenceBatchType)
   */
  @Override
  public void batchEnd(ExecutionContext ec, PersistenceBatchType batchType) {
    if (batchType == PersistenceBatchType.PERSIST) {
      storeMgr.getBatchPutManager().finish(this);
    }
    else if (batchType == PersistenceBatchType.DELETE) {
      storeMgr.getBatchDeleteManager().finish(this);
    }
  }

  /**
   * Method to retrieve the Entity with the specified key from the datastore.
   * @param op ObjectProvider that we want to associate this Entity with
   * @param key The key
   * @return The Entity
   */
  private Entity getEntityFromDatastore(ObjectProvider op, Key key) {
    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(op.getExecutionContext());

    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_NATIVE.debug("Getting entity of kind " + key.getKind() + " with key " + key);
    }

    Entity entity;
    try {
      if (txn == null) {
        entity = datastoreServiceForReads.get(key);
      } else {
        entity = datastoreServiceForReads.get(txn.getInnerTxn(), key);
      }
    } catch (EntityNotFoundException e) {
      throw DatastoreExceptionTranslator.wrapEntityNotFoundException(e, key);
    }

    setAssociatedEntity(op, txn, entity);
    return entity;
  }

  private void put(List<PutState> putStateList) {
    if (putStateList.isEmpty()) {
      return;
    }

    DatastoreTransaction txn = null;
    ExecutionContext ec = null;
    AbstractClassMetaData acmd = null;
    List<Entity> entityList = Utils.newArrayList();
    for (PutState putState : putStateList) {
      if (txn == null) {
        txn = EntityUtils.getCurrentTransaction(putState.op.getExecutionContext());
      }
      if (ec == null) {
        ec = putState.op.getExecutionContext();
      }
      if (acmd == null) {
        acmd = putState.op.getClassMetaData();
      }
      entityList.add(putState.entity);
    }

    put(ec, acmd, entityList);
    for (PutState putState : putStateList) {
      setAssociatedEntity(putState.op, txn, putState.entity);
    }
  }

  private void put(ObjectProvider op, Entity entity) {
    DatastoreTransaction txn = put(op.getExecutionContext(), op.getClassMetaData(), entity);
    setAssociatedEntity(op, txn, entity);
  }

  public DatastoreTransaction put(ExecutionContext ec, AbstractClassMetaData acmd, Entity entity) {
    return put(ec, acmd, Collections.singletonList(entity));
  }

  private DatastoreTransaction put(ExecutionContext ec, AbstractClassMetaData acmd, List<Entity> entities) {
    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(ec);
    List<Entity> putMe = Utils.newArrayList();
    for (Entity entity : entities) {
      if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
        StringBuffer str = new StringBuffer();
        for (Map.Entry<String, Object> entry : entity.getProperties().entrySet()) {
          str.append(entry.getKey() + "[" + entry.getValue() + "]");
          str.append(", ");
        }
        NucleusLogger.DATASTORE_NATIVE.debug("Putting entity of kind " + entity.getKind() + 
            " with key " + entity.getKey() + " as {" + str.toString() + "}");
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
      // validate the entity
      // for now it checks for unwanted null values
      for (Entity entity : putMe) {
        validate(ec, acmd, entity);
      }
      
      if (txn == null) {
        if (putMe.size() == 1) {
          datastoreServiceForWrites.put(putMe.get(0));
        } else {
          datastoreServiceForWrites.put(putMe);
        }
      } else {
        Transaction innerTxn = txn.getInnerTxn();
        if (putMe.size() == 1) {
          datastoreServiceForWrites.put(innerTxn, putMe.get(0));
        } else {
          datastoreServiceForWrites.put(innerTxn, putMe);
        }
        txn.addPutEntities(putMe);
      }
    }
    return txn;
  }

  void delete(DatastoreTransaction txn, List<Key> keys) {
    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_NATIVE.debug("Deleting entities with keys " + StringUtils.collectionToString(keys));
    }

    if (txn == null) {
      if (keys.size() == 1) {
        datastoreServiceForWrites.delete(keys.get(0));
      } else {
        datastoreServiceForWrites.delete(keys);
      }
    } else {
      Transaction innerTxn = txn.getInnerTxn();
      if (keys.size() == 1) {
        datastoreServiceForWrites.delete(innerTxn, keys.get(0));
      } else {
        datastoreServiceForWrites.delete(innerTxn, keys);
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

  public void insertObject(ObjectProvider op) {
    NucleusLogger.GENERAL.info(">> PersistenceHandler.insertObject FOR " + op);
    // If we're in the middle of a batch operation just register the ObjectProvider that needs the insertion
    if (storeMgr.getBatchPutManager().batchOperationInProgress()) {
      storeMgr.getBatchPutManager().add(op);
      return;
    }
    insertObjectsInternal(Collections.singletonList(op));
  }

  /**
   * If we're inserting multiple objects we want to use the low-level batch put
   * mechanism.  This method pre-processes all the state managers, gathering
   * up all the info needed to do the put, does the put for all the state
   * managers at once, and then does post processing on all the state managers.
   */
  void insertObjectsInternal(List<ObjectProvider> opsToInsert) {
    try {
      List<PutState> putStateList = insertPreProcess(opsToInsert);
      // Save the parent entities first so we can have a key to use as a parent
      // on owned child entities.
      put(putStateList);

      insertPostProcess(putStateList);
    } finally {
      for (ObjectProvider sm : opsToInsert) {
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

      // TODO Only do this when the datastore is allocating the identity (i.e "identity" strategy)
      {
        Object newObjectId;
        if (pkType.equals(Key.class)) {
          newObjectId = putState.entity.getKey();
        } else if (pkType.equals(String.class)) {
          if (MetaDataUtils.hasEncodedPKField(putState.acmd)) {
            newObjectId = KeyFactory.keyToString(putState.entity.getKey());
          } else {
            newObjectId = putState.entity.getKey().getName();
          }
        } else if (pkType.equals(Long.class)) {
          newObjectId = putState.entity.getKey().getId();
        } else {
          // TODO Why is this checking here? Ought to be checked in MetaDataValidator!
          throw new IllegalStateException(
              "Primary key for type " + putState.op.getClassMetaData().getName()
              + " is of unexpected type " + pkType.getName()
              + " (must be String, Long, or " + Key.class.getName() + ")");
        }
        putState.op.setPostStoreNewObjectId(newObjectId);
        if (putState.assignedParentPk != null) {
          // we automatically assigned a parent to the entity so make sure
          // that makes it back on to the pojo
          setPostStoreNewParent(
              putState.op, putState.fieldMgr.getParentMemberMetaData(), putState.assignedParentPk);
        }
        Integer pkIdPos = putState.fieldMgr.getPkIdPos();
        if (pkIdPos != null) {
          setPostStorePkId(putState.op, pkIdPos, putState.entity);
        }
      }

      storeRelations(putState.fieldMgr, putState.op, putState.entity);

      if (putState.entity.getParent() != null) {
        // We inserted a new child.  Register the parent key so we know we need
        // to update the parent.
        KeyRegistry keyReg = KeyRegistry.getKeyRegistry(putState.op.getExecutionContext());
        keyReg.registerModifiedParent(putState.entity.getParent());
      }
      if (storeMgr.getRuntimeManager() != null) {
        storeMgr.getRuntimeManager().incrementInsertCount();
      }
    }
  }

  private List<PutState> insertPreProcess(List<ObjectProvider> opsToInsert) {
    List<PutState> putStateList = Utils.newArrayList();
    for (ObjectProvider op : opsToInsert) {
      if (op.getAssociatedValue(INSERTION_TOKEN) != null) {
        // already inserting the pc associated with this state manager
        continue;
      }
      // set the token so if we recurse down to the same state manager we know we're already inserting
      op.setAssociatedValue(INSERTION_TOKEN, INSERTION_TOKEN);

      storeMgr.validateMetaDataForClass(op.getClassMetaData());

      // Make sure writes are permitted
      storeMgr.assertReadOnlyForUpdateOfObject(op);

      String kind = EntityUtils.determineKind(op.getClassMetaData(), op.getExecutionContext());

      // For inserts we let the field manager create the Entity and then
      // retrieve it afterwards.  We do this because the entity isn't
      // 'fixed' until after provideFields has been called.
      StoreFieldManager fieldMgr = new StoreFieldManager(
          op, kind, storeMgr, StoreFieldManager.Operation.INSERT);
      AbstractClassMetaData acmd = op.getClassMetaData();
      op.provideFields(acmd.getAllMemberPositions(), fieldMgr);

      Object assignedParentPk = fieldMgr.establishEntityGroup();
      Entity entity = fieldMgr.getEntity();
      if (fieldMgr.handleIndexFields()) {
        // signal to downstream writers that they should
        // insert this entity when they are invoked
        op.setAssociatedValue(ENTITY_WRITE_DELAYED, entity);
        continue;
      } else {
        handleVersioningBeforeWrite(op, entity, VersionBehavior.INCREMENT, "updating");
        handleDiscriminatorBeforeInsert(op, entity);
      }
      // Add the state for this put to the list.
      putStateList.add(new PutState(op, fieldMgr, acmd, assignedParentPk, entity));
    }
    return putStateList;
  }

  private void setPostStorePkId(ObjectProvider op, int fieldNumber, Entity entity) {
    op.replaceFieldMakeDirty(fieldNumber, entity.getKey().getId());
  }

  private void setPostStoreNewParent(
      ObjectProvider op, AbstractMemberMetaData parentMemberMetaData, Object assignedParentPk) {
    op.replaceFieldMakeDirty(parentMemberMetaData.getAbsoluteFieldNumber(), assignedParentPk);
  }

  IdentifierFactory getIdentifierFactory(ObjectProvider op) {
    return ((MappedStoreManager)op.getExecutionContext().getStoreManager()).getIdentifierFactory();
  }

  private NucleusOptimisticException newNucleusOptimisticException(
      AbstractClassMetaData cmd, Entity entity, String op, String details) {
    return new NucleusOptimisticException(
        "Optimistic concurrency exception " + op + " " + cmd.getFullClassName()
            + " with pk " + entity.getKey() + ".  " + details);
  }

  private void handleVersioningBeforeWrite(ObjectProvider sm, Entity entity,
      VersionBehavior versionBehavior, String op) {
    AbstractClassMetaData cmd = sm.getClassMetaData();
    VersionMetaData vmd = cmd.getVersionMetaDataForClass();
    if (cmd.isVersioned()) {
      String versionPropertyName = EntityUtils.getVersionPropertyName(getIdentifierFactory(sm), vmd);
      Object curVersion = sm.getExecutionContext().getApiAdapter().getVersion(sm);
      if (curVersion != null) {
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
          NucleusLogger.DATASTORE_NATIVE.debug("Getting entity with key " + entity.getKey());
        }
        // Fetch the latest and greatest version of the entity from the datastore
        // to see if anyone has made a change underneath us.  We need to execute
        // the fetch outside a txn to guarantee that we see the latest version.
        Entity refreshedEntity;
        try {
          refreshedEntity = datastoreServiceForReads.get(entity.getKey());
        } catch (EntityNotFoundException e) {
          // someone deleted out from under us
          throw newNucleusOptimisticException(
              cmd, entity, op, "The underlying entity had already been deleted.");
        }
        Object datastoreVersion = refreshedEntity.getProperty(versionPropertyName);
        if (vmd.getVersionStrategy() == VersionStrategy.DATE_TIME) {
          datastoreVersion = new Timestamp((Long) datastoreVersion);
        }
        if (!datastoreVersion.equals(curVersion)) {
          throw newNucleusOptimisticException(cmd, entity, op, "The underlying entity had already been updated.");
        }
      }

      Object nextVersion = VersionHelper.getNextVersion(vmd.getVersionStrategy(), curVersion);
      sm.setTransactionalVersion(nextVersion);
      if (vmd.getVersionStrategy() == VersionStrategy.DATE_TIME) {
        EntityUtils.setEntityProperty(entity, vmd, versionPropertyName, ((Timestamp)nextVersion).getTime());
      } else {
        EntityUtils.setEntityProperty(entity, vmd, versionPropertyName, nextVersion);
      }

      // Version field - update the version on the object
      if (versionBehavior == VersionBehavior.INCREMENT && vmd.getFieldName() != null) {
        AbstractMemberMetaData verfmd =
            ((AbstractClassMetaData)vmd.getParent()).getMetaDataForMember(vmd.getFieldName());
        if (nextVersion instanceof Number) {
          // Version can be long, Long, int, Integer, short, Short, or
          // Timestamp, but Timestamp is not yet supported.  DataNuc always
          // returns a Long if the VersionStrategy is VERSION_NUMBER so we need
          // to convert for other types.
          Number nextNumber = (Number) nextVersion;
          if (verfmd.getType().equals(Long.class) || verfmd.getType().equals(Long.TYPE)) {
            nextVersion = nextNumber.longValue();
          } else if (verfmd.getType().equals(Integer.class) || verfmd.getType().equals(Integer.TYPE)) {
            nextVersion = nextNumber.intValue();
          } else if (verfmd.getType().equals(Short.class) || verfmd.getType().equals(Short.TYPE)) {
            nextVersion = nextNumber.shortValue();
          }
        }
        sm.replaceField(verfmd.getAbsoluteFieldNumber(), nextVersion);
      }
    }
  }

  private void handleDiscriminatorBeforeInsert(ObjectProvider op, Entity entity) {
    if (op.getClassMetaData().hasDiscriminatorStrategy()) {
      DiscriminatorMetaData dismd = op.getClassMetaData().getDiscriminatorMetaDataRoot();
      EntityUtils.setEntityProperty(entity, dismd, 
          EntityUtils.getDiscriminatorPropertyName(getIdentifierFactory(op), dismd), 
          op.getClassMetaData().getDiscriminatorValue());
    }
  }
  
  /**
   * Get the primary key of the object associated with the provided
   * state manager.  Can return {@code null}.
   */
  private static Object getPk(ObjectProvider op) {
    return op.getExecutionContext().getApiAdapter()
        .getTargetKeyForSingleFieldIdentity(op.getInternalObjectId());
  }

  private static Key getPkAsKey(ObjectProvider op) {
    Object pk = getPk(op);
    if (pk == null) {
      throw new IllegalStateException(
          "Primary key for object of type " + op.getClassMetaData().getName() + " is null.");
    }
    return EntityUtils.getPkAsKey(pk, op.getClassMetaData(), op.getExecutionContext());
  }

  public void setAssociatedEntity(ObjectProvider op, DatastoreTransaction txn, Entity entity) {
    // A StateManager can be used across multiple transactions, so we
    // want to associate the entity not just with a StateManager but a
    // StateManager and a transaction.
    op.setAssociatedValue(txn, entity);
  }

  public Entity getAssociatedEntityForCurrentTransaction(ObjectProvider op) {
    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(op.getExecutionContext());
    return (Entity) op.getAssociatedValue(txn);
  }

  public void fetchObject(ObjectProvider op, int fieldNumbers[]) {
    if (fieldNumbers == null || fieldNumbers.length == 0) {
      return;
    }

    AbstractClassMetaData cmd = op.getClassMetaData();
    storeMgr.validateMetaDataForClass(cmd);

    // We always fetch the entire object, so if the state manager
    // already has an associated Entity we know that associated
    // Entity has all the fields.
    Entity entity = getAssociatedEntityForCurrentTransaction(op);
    if (entity == null) {
      Key pk = getPkAsKey(op);
      entity = getEntityFromDatastore(op, pk); // Throws NucleusObjectNotFoundException if necessary
    }

    if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled()) {
      // Debug information about what we are retrieving
      StringBuffer str = new StringBuffer("Fetching object \"");
      str.append(op.toPrintableID()).append("\" (id=");
      str.append(op.getExecutionContext().getApiAdapter().getObjectId(op)).append(")").append(" fields [");
      for (int i=0;i<fieldNumbers.length;i++) {
        if (i > 0) {
          str.append(",");
        }
        str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
      }
      str.append("]");
      NucleusLogger.DATASTORE_RETRIEVE.debug(str);
    }

    long startTime = System.currentTimeMillis();
    if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_RETRIEVE.debug(GAE_LOCALISER.msg("AppEngine.Fetch.Start", 
        op.toPrintableID(), op.getInternalObjectId()));
    }

    // TODO Update fieldNumbers to include non-DFG that we think are required.

    op.replaceFields(fieldNumbers, new FetchFieldManager(op, storeMgr, entity, fieldNumbers));

    // Refresh version - is this needed? should have been set on retrieval anyway
    VersionMetaData vmd = cmd.getVersionMetaDataForClass();
    if (cmd.isVersioned()) {
      Object versionValue = entity.getProperty(EntityUtils.getVersionPropertyName(getIdentifierFactory(op), vmd));
      if (vmd.getVersionStrategy() == VersionStrategy.DATE_TIME) {
        versionValue = new Timestamp((Long)versionValue);
      }
      op.setVersion(versionValue);
    }

    // Run post-fetch mapping callbacks. What is this actually achieving?
    AbstractMemberMetaData[] fmds = new AbstractMemberMetaData[fieldNumbers.length];
    for (int i = 0; i < fmds.length; i++) {
      fmds[i] = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
    }
    ClassLoaderResolver clr = op.getExecutionContext().getClassLoaderResolver();
    DatastoreClass dc = storeMgr.getDatastoreClass(op.getObject().getClass().getName(), clr);
    FetchMappingConsumer consumer = new FetchMappingConsumer(op.getClassMetaData());
    dc.provideMappingsForMembers(consumer, fmds, true);
    dc.provideDatastoreIdMappings(consumer);
    dc.providePrimaryKeyMappings(consumer);
    for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
      callback.postFetch(op);
    }

    if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_RETRIEVE.debug(GAE_LOCALISER.msg("AppEngine.ExecutionTime",
            (System.currentTimeMillis() - startTime)));
    }
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementFetchCount();
    }
  }

  public void updateObject(ObjectProvider op, int fieldNumbers[]) {
    if (op.getLifecycleState().isDeleted()) {
// TODO Was this in 1.1   if (op.isDeleted((PersistenceCapable) op.getObject())) {
      // don't perform updates on objects that are already deleted - this will cause them to be recreated
      // NOTE : SHOULD NEVER HAVE AN UPDATE OF A DELETED OBJECT. DEFINE THE TESTCASE THAT DOES THIS
      return;
    }

    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(op);
    NucleusLogger.GENERAL.info(">> PersistenceHandler.updateObject FOR " + op);

    AbstractClassMetaData cmd = op.getClassMetaData();
    storeMgr.validateMetaDataForClass(cmd);

    long startTime = System.currentTimeMillis();
    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      StringBuffer fieldStr = new StringBuffer();
      for (int i=0;i<fieldNumbers.length;i++) {
        if (i > 0) {
          fieldStr.append(",");
        }
        fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
      }
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.Update.Start", 
        op.toPrintableID(), op.getInternalObjectId(), fieldStr.toString()));
    }

    Entity entity = getAssociatedEntityForCurrentTransaction(op);
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = getPkAsKey(op);
      entity = getEntityFromDatastore(op, key);
    }
    StoreFieldManager fieldMgr = new StoreFieldManager(
        op, storeMgr, entity, fieldNumbers, StoreFieldManager.Operation.UPDATE);
    op.provideFields(fieldNumbers, fieldMgr);
    handleVersioningBeforeWrite(op, entity, VersionBehavior.INCREMENT, "updating");

    NucleusLogger.GENERAL.info(">> PersistenceHandler.updateObject PUT of " + op);
    put(op, entity);

    NucleusLogger.GENERAL.info(">> PersistenceHandler.updateObject storeRelations of " + op);
    storeRelations(fieldMgr, op, entity);

    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.ExecutionTime", 
        (System.currentTimeMillis() - startTime)));
    }
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementUpdateCount();
    }
  }

  private void storeRelations(StoreFieldManager fieldMgr, ObjectProvider op, Entity entity) {
    if (fieldMgr.storeRelations() &&
        storeMgr.storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
      // Return value of true means that storing the relations resulted in
      // changes that need to be reflected on the current object.
      // That means we need to re-save.
      ClassLoaderResolver clr = op.getExecutionContext().getClassLoaderResolver();
      boolean missingRelationKey = false;
      try {
        fieldMgr.setRepersistingForChildKeys(true);
        AbstractClassMetaData acmd = op.getClassMetaData();
        for (int field : acmd.getRelationMemberPositions(clr, storeMgr.getMetaDataManager())) {
          op.provideFields(new int[] {field}, fieldMgr);
          if (op.getAssociatedValue(MISSING_RELATION_KEY) != null) {
            missingRelationKey = true;
          }
          op.setAssociatedValue(MISSING_RELATION_KEY, null);
        }
      } finally {
        fieldMgr.setRepersistingForChildKeys(false);
      }
      // if none of the relation fields are missing relation keys then there is
      // no need for any additional puts to write the relation keys
      // we'll set a flag on the statemanager to let downstream writers know
      // and then we'll do the put ourselves
      if (!missingRelationKey) {
        op.setAssociatedValue(ForceFlushPreCommitTransactionEventListener.ALREADY_PERSISTED_RELATION_KEYS_KEY, true);
        put(op, entity);
      }
    }
  }

  public void deleteObject(ObjectProvider op) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(op);

    ExecutionContext ec = op.getExecutionContext();
    storeMgr.validateMetaDataForClass(op.getClassMetaData());

    long startTime = System.currentTimeMillis();
    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.Delete.Start", 
        op.toPrintableID(), op.getInternalObjectId()));
    }

    Entity entity = getAssociatedEntityForCurrentTransaction(op);
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = getPkAsKey(op);
      entity = getEntityFromDatastore(op, key);
    }

    ClassLoaderResolver clr = ec.getClassLoaderResolver();
    DatastoreClass dc = storeMgr.getDatastoreClass(op.getObject().getClass().getName(), clr);

    DatastoreTransaction txn = EntityUtils.getCurrentTransaction(ec);
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
    } else if (Boolean.TRUE.equals(op.getAssociatedValue("deleted"))) {
      // same issue as above except we may not be in a txn
      return;
    }
    op.setAssociatedValue("deleted", true);

    // Check the version is valid to delete; any updates since read?
    handleVersioningBeforeWrite(op, entity, VersionBehavior.NO_INCREMENT, "deleting");

    // first handle any dependent deletes that need deleting before we delete this object
    DependentDeleteRequest req = new DependentDeleteRequest(dc, op.getClassMetaData(), clr);
    Set relatedObjectsToDelete = req.execute(op, entity);

    // Null out all non-PK fields in the datastore. Why not just delete it?
    DatastoreFieldManager fieldMgr = new DatastoreDeleteFieldManager(op, storeMgr, entity);
    AbstractClassMetaData acmd = op.getClassMetaData();
    op.provideFields(acmd.getNonPKMemberPositions(), fieldMgr);

    Key keyToDelete = getPkAsKey(op);

    // If we're in the middle of a batch operation just register the statemanager that needs the delete
    BatchDeleteManager bdm = storeMgr.getBatchDeleteManager();
    if (bdm.batchOperationInProgress()) {
      bdm.add(new BatchDeleteManager.BatchDeleteState(txn, keyToDelete));

      if (relatedObjectsToDelete != null && !relatedObjectsToDelete.isEmpty()) {
        // Delete any related objects that need deleting after the delete of this object
        Iterator iter = relatedObjectsToDelete.iterator();
        while (iter.hasNext()) {
          Object relatedObject = iter.next();
          ec.deleteObjectInternal(relatedObject);
        }
      }

      if (storeMgr.getRuntimeManager() != null) {
        storeMgr.getRuntimeManager().incrementDeleteCount();
      }

      return;
    }

    // Delete this object
    delete(txn, Collections.singletonList(keyToDelete));

    if (relatedObjectsToDelete != null && !relatedObjectsToDelete.isEmpty()) {
      // Delete any related objects that need deleting after the delete of this object
      Iterator iter = relatedObjectsToDelete.iterator();
      while (iter.hasNext()) {
        Object relatedObject = iter.next();
        ec.deleteObjectInternal(relatedObject);
      }
    }

    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.ExecutionTime", 
        (System.currentTimeMillis() - startTime)));
    }
    if (storeMgr.getRuntimeManager() != null) {
      storeMgr.getRuntimeManager().incrementDeleteCount();
    }
  }

  public void locateObject(ObjectProvider op) {
    storeMgr.validateMetaDataForClass(op.getClassMetaData());

    // get throws NucleusObjectNotFoundException if the entity isn't found, which is what we want.
    getEntityFromDatastore(op, getPkAsKey(op));
  }

  /**
   * Implementation of this operation is optional and is intended for
   * datastores that instantiate the model objects themselves (as opposed
   * to letting datanucleus do it).  The App Engine datastore lets
   * datanucleus instantiate the model objects so we just return null.
   */
  public Object findObject(ExecutionContext ec, Object id) {
    return null;
  }

  private void validate(ExecutionContext ec, AbstractClassMetaData acmd, Entity entity) {
    DatastoreTable table = storeMgr.getDatastoreClass(
        acmd.getFullClassName(), ec.getClassLoaderResolver());

    for (Entry<String, Object> prop : entity.getProperties().entrySet()) {
      DatastoreField field = table.getDatastoreField(prop.getKey());
      // If the property is a relation and datanucleus thinks it won't
      // be stored in a database field, we don't have a database field.
      // And the are some issues with the @Version field where we don't
      // have a database field either.
      // TODO investigate into the app engine plugin not honoring the column attributes for version fields.
      if (field != null && !field.isNullable() && prop.getValue() == null) {
        throw new NucleusDataStoreException("non-null property " + prop.getKey() + " of " + entity.getKind() +
            " is null");
      }
    }
  }
  /**
   * All the information needed to perform a put on an Entity.
   */
  private static final class PutState {
    private final ObjectProvider op;
    private final StoreFieldManager fieldMgr;
    private final AbstractClassMetaData acmd;
    private final Object assignedParentPk;
    private final Entity entity;

    private PutState(ObjectProvider op,
          StoreFieldManager fieldMgr, AbstractClassMetaData acmd, Object assignedParentPk,
          Entity entity) {
      this.op = op;
      this.fieldMgr = fieldMgr;
      this.acmd = acmd;
      this.assignedParentPk = assignedParentPk;
      this.entity = entity;
    }
  }
}
