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
import com.google.appengine.datanucleus.mapping.DependentDeleteRequest;
import com.google.appengine.datanucleus.mapping.FetchMappingConsumer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
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
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private final DatastoreManager storeMgr;

  // TODO Remove this and make more use of the DatastoreService from the ManagedConnection
  private final DatastoreService datastoreServiceForReads;

  private final Map<ExecutionContext, BatchPutManager> batchPutManagerByExecutionContext = new HashMap();

  private final Map<ExecutionContext, BatchDeleteManager> batchDeleteManagerByExecutionContext = new HashMap();

  /**
   * Constructor.
   * @param storeMgr The StoreManager to use.
   */
  public DatastorePersistenceHandler(StoreManager storeMgr) {
    this.storeMgr = (DatastoreManager) storeMgr;
    datastoreServiceForReads = DatastoreServiceFactoryInternal.getDatastoreService(
        this.storeMgr.getDefaultDatastoreServiceConfigForReads());
  }

  public void close() {}

  @Override
  public boolean useReferentialIntegrity() {
    // This informs DataNucleus that the store requires ordered flushes, so the order of receiving dirty
    // requests is preserved when flushing them
    return true;
  }

  protected BatchPutManager getBatchPutManager(ExecutionContext ec) {
    BatchPutManager putMgr = batchPutManagerByExecutionContext.get(ec);
    if (putMgr == null) {
      putMgr = new BatchPutManager();
      batchPutManagerByExecutionContext.put(ec, putMgr);
    }
    return putMgr;
  }

  protected BatchDeleteManager getBatchDeleteManager(ExecutionContext ec) {
    BatchDeleteManager deleteMgr = batchDeleteManagerByExecutionContext.get(ec);
    if (deleteMgr == null) {
      deleteMgr = new BatchDeleteManager(ec);
      batchDeleteManagerByExecutionContext.put(ec, deleteMgr);
    }
    return deleteMgr;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.AbstractPersistenceHandler#batchStart(org.datanucleus.store.ExecutionContext, org.datanucleus.store.PersistenceBatchType)
   */
  @Override
  public void batchStart(ExecutionContext ec, PersistenceBatchType batchType) {
    if (batchType == PersistenceBatchType.PERSIST) {
      getBatchPutManager(ec).start();
    }
    else if (batchType == PersistenceBatchType.DELETE) {
      getBatchDeleteManager(ec).start();
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.AbstractPersistenceHandler#batchEnd(org.datanucleus.store.ExecutionContext, org.datanucleus.store.PersistenceBatchType)
   */
  @Override
  public void batchEnd(ExecutionContext ec, PersistenceBatchType batchType) {
    if (batchType == PersistenceBatchType.PERSIST) {
      getBatchPutManager(ec).finish(this);
      batchPutManagerByExecutionContext.remove(ec);
    }
    else if (batchType == PersistenceBatchType.DELETE) {
      getBatchDeleteManager(ec).finish(this);
      batchDeleteManagerByExecutionContext.remove(ec);
    }
  }

  /**
   * Token used to make sure we don't try to insert the pc associated with an ObjectProvider more than once.
   * This can happen in the case of a bi-directional one-to-one where we add a child to an existing parent and
   * call merge() on the parent.  In this scenario we receive a call to {@link #insertObject(StateManager)} with 
   * the state manager for the new child. When we invoke {@link StateManager#provideFields(int[], FieldManager)}
   * we will recurse back to the parent field on the child (remember, bidirectional relationship), which will 
   * then recurse back to the child. Since the child has not yet been inserted, insertObject will be invoked
   * _again_ and we end up creating 2 instances of the child in the datastore, which is not good.
   * There are probably better ways to solve this problem, but for now this looks ok.  We're making the assumption 
   * that state managers are only accessed by a single thread at a time.
   */
  private static final Object INSERTION_TOKEN = new Object();

  /**
   * Method to insert the specified managed object into the datastore.
   * @param op ObjectProvider for the managed object
   */
  public void insertObject(ObjectProvider op) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(op);

    storeMgr.validateMetaDataForClass(op.getClassMetaData());
    NucleusLogger.GENERAL.info(">> PersistenceHandler.insertObject FOR " + op);

    // If we're in the middle of a batch operation just register the ObjectProvider that needs the insertion
    BatchPutManager batchPutMgr = getBatchPutManager(op.getExecutionContext());
    if (batchPutMgr.batchOperationInProgress()) {
      batchPutMgr.add(op);
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
      List<PutState> putStateList = Utils.newArrayList();
      for (ObjectProvider op : opsToInsert) {
        if (op.getAssociatedValue(INSERTION_TOKEN) != null) {
          // already inserting the pc associated with this state manager
          continue;
        }

        // set the token so if we recurse down to the same state manager we know we're already inserting
        op.setAssociatedValue(INSERTION_TOKEN, INSERTION_TOKEN);

        // For inserts we let the field manager create the Entity and then retrieve it afterwards.
        // We do this because the entity isn't ready to put until after provideFields has been called.
        String kind = EntityUtils.determineKind(op.getClassMetaData(), op.getExecutionContext());
        StoreFieldManager fieldMgr = new StoreFieldManager(op, kind, StoreFieldManager.Operation.INSERT);
        op.provideFields(op.getClassMetaData().getAllMemberPositions(), fieldMgr);

        Object assignedParentPk = fieldMgr.establishEntityGroup();

        Entity entity = fieldMgr.getEntity();
        if (fieldMgr.handleIndexFields()) {
          // signal to downstream writers that they should insert this entity when they are invoked
          op.setAssociatedValue(ENTITY_WRITE_DELAYED, entity);
          continue;
        } else {
          // Set version
          handleVersioningBeforeWrite(op, entity, true, "inserting");

          if (op.getClassMetaData().hasDiscriminatorStrategy()) {
            // Set discriminator
            DiscriminatorMetaData dismd = op.getClassMetaData().getDiscriminatorMetaDataRoot();
            EntityUtils.setEntityProperty(entity, dismd, 
                EntityUtils.getDiscriminatorPropertyName(storeMgr.getIdentifierFactory(), dismd), 
                op.getClassMetaData().getDiscriminatorValue());
          }
        }

        // Add the state for this put to the list.
        putStateList.add(new PutState(op, fieldMgr, assignedParentPk, entity));
      }

      // Save the parent entities first so we can have a key to use as a parent on owned child entities.
      if (!putStateList.isEmpty()) {
        DatastoreTransaction txn = null;
        ExecutionContext ec = null;
        AbstractClassMetaData acmd = null;
        List<Entity> entityList = Utils.newArrayList();
        for (PutState putState : putStateList) {
          if (txn == null) {
            txn = DatastoreManager.getDatastoreTransaction(putState.op.getExecutionContext());
          }
          if (ec == null) {
            ec = putState.op.getExecutionContext();
          }
          if (acmd == null) {
            acmd = putState.op.getClassMetaData();
          }
          entityList.add(putState.entity);
        }

        EntityUtils.putEntitiesIntoDatastore(ec, entityList);
        for (PutState putState : putStateList) {
          putState.op.setAssociatedValue(txn, putState.entity);
        }
      }

      // Post-processing for all puts
      for (PutState putState : putStateList) {
        AbstractClassMetaData cmd = putState.op.getClassMetaData();

        // Set the generated key back on the pojo.  If the pk field is a Key just set it on the field directly. 
        // If the pk field is a String, convert the Key to a String, similarly for long.
        // Assumes we only have a single pk member position
        Object newId = null;
        Class pkType = null;
        boolean identityStrategyUsed = false;
        if (cmd.getIdentityType() == IdentityType.APPLICATION) {
          int[] pkFields = cmd.getPKMemberPositions();
          // TODO Allow for multiple PK fields
          AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFields[0]);
          if (pkMmd.getValueStrategy() == IdentityStrategy.IDENTITY) {
            identityStrategyUsed = true;
            pkType = cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[0]).getType();
          }
        } else if (cmd.getIdentityType() == IdentityType.DATASTORE &&
            cmd.getIdentityMetaData().getValueStrategy() == IdentityStrategy.IDENTITY) {
          pkType = Long.class;
          ColumnMetaData colmd = cmd.getIdentityMetaData().getColumnMetaData();
          if (colmd != null && 
              ("varchar".equalsIgnoreCase(colmd.getJdbcType()) || "char".equalsIgnoreCase(colmd.getJdbcType()))) {
            pkType = String.class;
          }
        }

        if (identityStrategyUsed) {
          // Update the identity of the object with the datastore-assigned id
          if (pkType.equals(Key.class)) {
            newId = putState.entity.getKey();
          } else if (pkType.equals(String.class)) {
            if (MetaDataUtils.hasEncodedPKField(cmd)) {
              newId = KeyFactory.keyToString(putState.entity.getKey());
            } else {
              newId = putState.entity.getKey().getName();
            }
          } else if (pkType.equals(Long.class) || pkType.equals(long.class)) {
            newId = putState.entity.getKey().getId();
          }

          putState.op.setPostStoreNewObjectId(newId);
        }

        if (putState.assignedParentPk != null) {
          // we automatically assigned a parent to the entity so make sure that makes it back on to the pojo
          putState.op.replaceFieldMakeDirty(putState.fieldMgr.getParentMemberMetaData().getAbsoluteFieldNumber(), 
              putState.assignedParentPk);
        }

        storeRelations(putState.fieldMgr, putState.op, putState.entity);

        if (putState.entity.getParent() != null) {
          // We inserted a new child.  Register the parent key so we know we need to update the parent.
          KeyRegistry keyReg = KeyRegistry.getKeyRegistry(putState.op.getExecutionContext());
          keyReg.registerModifiedParent(putState.entity.getParent());
        }

        if (storeMgr.getRuntimeManager() != null) {
          storeMgr.getRuntimeManager().incrementInsertCount();
        }
      }
    } finally {
      for (ObjectProvider op : opsToInsert) {
        op.setAssociatedValue(INSERTION_TOKEN, null);
      }
    }
  }

  /**
   * Method to fetch the specified fields of the managed object from the datastore.
   * @param op ObjectProvider of the object whose fields need fetching
   * @param fieldNumbers Fields to fetch
   */
  public void fetchObject(ObjectProvider op, int fieldNumbers[]) {
    if (fieldNumbers == null || fieldNumbers.length == 0) {
      return;
    }

    AbstractClassMetaData cmd = op.getClassMetaData();
    storeMgr.validateMetaDataForClass(cmd);

    // We always fetch the entire object, so if the state manager
    // already has an associated Entity we know that associated
    // Entity has all the fields.
    Entity entity = (Entity) op.getAssociatedValue(DatastoreManager.getDatastoreTransaction(op.getExecutionContext()));
    if (entity == null) {
      Key pk = EntityUtils.getPkAsKey(op);
      entity = EntityUtils.getEntityFromDatastore(datastoreServiceForReads, op, pk); // Throws NucleusObjectNotFoundException if necessary
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

    op.replaceFields(fieldNumbers, new FetchFieldManager(op, entity, fieldNumbers));

    // Refresh version - is this needed? should have been set on retrieval anyway
    VersionMetaData vmd = cmd.getVersionMetaDataForClass();
    if (cmd.isVersioned()) {
      Object versionValue = entity.getProperty(EntityUtils.getVersionPropertyName(storeMgr.getIdentifierFactory(), vmd));
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

  /**
   * Method to update the specified fields of the managed object in the datastore.
   * @param op ObjectProvider of the managed object
   * @param fieldNumbers Fields to be updated in the datastore
   */
  public void updateObject(ObjectProvider op, int fieldNumbers[]) {
    if (op.getLifecycleState().isDeleted()) {
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

    Entity entity = (Entity) op.getAssociatedValue(DatastoreManager.getDatastoreTransaction(op.getExecutionContext()));
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = EntityUtils.getPkAsKey(op);
      entity = EntityUtils.getEntityFromDatastore(datastoreServiceForReads, op, key);
    }

    StoreFieldManager fieldMgr = new StoreFieldManager(op, entity, fieldNumbers, StoreFieldManager.Operation.UPDATE);
    op.provideFields(fieldNumbers, fieldMgr);
    handleVersioningBeforeWrite(op, entity, true, "updating");

    NucleusLogger.GENERAL.info(">> PersistenceHandler.updateObject PUT of " + op);
    DatastoreTransaction txn = EntityUtils.putEntityIntoDatastore(op.getExecutionContext(), entity);
    op.setAssociatedValue(txn, entity);

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
    if (fieldMgr.storeRelations(KeyRegistry.getKeyRegistry(op.getExecutionContext())) &&
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
        DatastoreTransaction txn = EntityUtils.putEntityIntoDatastore(op.getExecutionContext(), entity);
        op.setAssociatedValue(txn, entity);
      }
    }
  }

  /**
   * Method to delete the specified managed object from the datastore.
   * @param op ObjectProvider of the managed object
   */
  public void deleteObject(ObjectProvider op) {
    // Make sure writes are permitted
    storeMgr.assertReadOnlyForUpdateOfObject(op);

    storeMgr.validateMetaDataForClass(op.getClassMetaData());

    long startTime = System.currentTimeMillis();
    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.Delete.Start", 
        op.toPrintableID(), op.getInternalObjectId()));
    }

    ExecutionContext ec = op.getExecutionContext();
    Entity entity = (Entity) op.getAssociatedValue(DatastoreManager.getDatastoreTransaction(ec));
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = EntityUtils.getPkAsKey(op);
      entity = EntityUtils.getEntityFromDatastore(datastoreServiceForReads, op, key);
    }

    DatastoreTransaction txn = DatastoreManager.getDatastoreTransaction(ec);
    if (txn != null) {
      if (txn.getDeletedKeys().contains(entity.getKey())) {
        // need to check this _before_ we execute the dependent delete request
        // because that might set off a chain of cascades that could bring us
        // back here or to an update for the same entity, and we need to make
        // sure those recursive calls can see that we're already deleting this entity.
        return;
      }
      txn.addDeletedKey(entity.getKey());
    } else if (Boolean.TRUE.equals(op.getAssociatedValue("deleted"))) {
      // same issue as above except we may not be in a txn
      return;
    }
    op.setAssociatedValue("deleted", true);

    // Check the version is valid to delete; any updates since read?
    handleVersioningBeforeWrite(op, entity, false, "deleting");

    // first handle any dependent deletes that need deleting before we delete this object
    ClassLoaderResolver clr = ec.getClassLoaderResolver();
    DatastoreClass dc = storeMgr.getDatastoreClass(op.getObject().getClass().getName(), clr);
    DependentDeleteRequest req = new DependentDeleteRequest(dc, op.getClassMetaData(), clr);
    Set relatedObjectsToDelete = req.execute(op, entity);

    Key keyToDelete = EntityUtils.getPkAsKey(op);

    // If we're in the middle of a batch operation just register the key that needs the delete
    BatchDeleteManager bdm = getBatchDeleteManager(ec);
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
    EntityUtils.deleteEntitiesFromDatastore(ec, Collections.singletonList(keyToDelete));

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

    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.ExecutionTime", 
        (System.currentTimeMillis() - startTime)));
    }
  }

  /**
   * Method to locate the specified managed object in the datastore.
   * @param op ObjectProvider for the managed object
   * @throws NucleusObjectNotFoundException if the object isn't found in the datastore
   */
  public void locateObject(ObjectProvider op) {
    storeMgr.validateMetaDataForClass(op.getClassMetaData());

    // get throws NucleusObjectNotFoundException if the entity isn't found, which is what we want.
    EntityUtils.getEntityFromDatastore(datastoreServiceForReads, op, EntityUtils.getPkAsKey(op));
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

  /**
   * Method to check optimistic versioning, and to set the version on the entity when required.
   * @param op ObjectProvider for the object
   * @param entity The entity being updated
   * @param versionBehavior Behaviour required for versioning here
   * @param operation Convenience string for messages
   */
  private void handleVersioningBeforeWrite(ObjectProvider op, Entity entity, boolean increment, String operation) {
    AbstractClassMetaData cmd = op.getClassMetaData();
    VersionMetaData vmd = cmd.getVersionMetaDataForClass();
    if (cmd.isVersioned()) {
      String versionPropertyName = EntityUtils.getVersionPropertyName(storeMgr.getIdentifierFactory(), vmd);
      Object curVersion = op.getExecutionContext().getApiAdapter().getVersion(op);
      if (curVersion != null) {
        // Fetch the latest and greatest version of the entity from the datastore
        // to see if anyone has made a change underneath us.  We need to execute
        // the fetch outside a txn to guarantee that we see the latest version.
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
          NucleusLogger.DATASTORE_NATIVE.debug("Getting entity with key " + entity.getKey());
        }
        Entity refreshedEntity;
        try {
          refreshedEntity = datastoreServiceForReads.get(entity.getKey());
        } catch (EntityNotFoundException e) {
          // someone deleted out from under us
          throw new NucleusOptimisticException(GAE_LOCALISER.msg("AppEngine.OptimisticError.EntityHasBeenDeleted", operation,
              cmd.getFullClassName(), entity.getKey()));
        }

        Object datastoreVersion = refreshedEntity.getProperty(versionPropertyName);
        if (vmd.getVersionStrategy() == VersionStrategy.DATE_TIME) {
          datastoreVersion = new Timestamp((Long) datastoreVersion);
        }

        if (!datastoreVersion.equals(curVersion)) {
          throw new NucleusOptimisticException(GAE_LOCALISER.msg("AppEngine.OptimisticError.EntityHasBeenUpdated", operation,
              cmd.getFullClassName(), entity.getKey()));
        }
      }

      Object nextVersion = VersionHelper.getNextVersion(vmd.getVersionStrategy(), curVersion);
      op.setTransactionalVersion(nextVersion);
      if (vmd.getVersionStrategy() == VersionStrategy.DATE_TIME) {
        EntityUtils.setEntityProperty(entity, vmd, versionPropertyName, ((Timestamp)nextVersion).getTime());
      } else {
        EntityUtils.setEntityProperty(entity, vmd, versionPropertyName, nextVersion);
      }

      // Version field - update the version on the object
      if (increment && vmd.getFieldName() != null) {
        AbstractMemberMetaData verfmd =
            ((AbstractClassMetaData)vmd.getParent()).getMetaDataForMember(vmd.getFieldName());
        if (nextVersion instanceof Number) {
          // Version can be long, Long, int, Integer, short, Short (or Timestamp).
          Number nextNumber = (Number) nextVersion;
          if (verfmd.getType().equals(Long.class) || verfmd.getType().equals(Long.TYPE)) {
            nextVersion = nextNumber.longValue();
          } else if (verfmd.getType().equals(Integer.class) || verfmd.getType().equals(Integer.TYPE)) {
            nextVersion = nextNumber.intValue();
          } else if (verfmd.getType().equals(Short.class) || verfmd.getType().equals(Short.TYPE)) {
            nextVersion = nextNumber.shortValue();
          }
        }
        op.replaceField(verfmd.getAbsoluteFieldNumber(), nextVersion);
      }
    }
  }

  /**
   * All the information needed to perform a put on an Entity.
   */
  private static final class PutState {
    private final ObjectProvider op;
    private final StoreFieldManager fieldMgr;
    private final Object assignedParentPk;
    private final Entity entity;

    private PutState(ObjectProvider op, StoreFieldManager fieldMgr, Object assignedParentPk, Entity entity) {
      this.op = op;
      this.fieldMgr = fieldMgr;
      this.assignedParentPk = assignedParentPk;
      this.entity = entity;
    }
  }
}
