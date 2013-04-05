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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.mapping.DatastoreTable;
import com.google.appengine.datanucleus.mapping.DependentDeleteRequest;
import com.google.appengine.datanucleus.mapping.FetchMappingConsumer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.ExecutionContext;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.PersistenceBatchType;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.mapping.ArrayMapping;
import org.datanucleus.store.mapped.mapping.CollectionMapping;
import org.datanucleus.store.mapped.mapping.IndexMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MapMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCO;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler for persistence requests for GAE/J datastore. Lifecycle management processes persists, updates, deletes
 * and field access and hands them off here to interface with the datastore.
 * No method in here should be called from anywhere other than DataNucleus core.
 * <h3>Persistence Process</h3>
 * Receive calls to the following from DataNucleus core for persistence events. All persistence events arrive in 
 * the store plugin in the order they are performed by the user. Optimistic operations are queued until flush().
 * <p>
 * <b>PersistenceHandler.insertObject</b><br/>
 * <ol>
 * <li>CREATE Entity belonging to the appropriate entity group to represent the object.</li>
 * 
 * <li>If the entity is “owned” the entity group must be established before the Entity is initially put(),
 * there is no way to adjust it after. So when persisting an Entity that is a child of some other Entity, you need to
 * figure out who its parent is before you can do this first put.
 * <ol>
 * <li>Key 'id' can be assigned by datastore (long, Long)</li>
 * <li>Key 'name' can be assigned by the application (or value-generator) (String)</li>
 * </ol></li>
 * 
 * <li>Create StoreFieldManager, and set properties in Entity for all fields which have values ready.
 * <ol>
 * <li>If “identity” not set on this related object, make note of and skip relation fields.</li>
 * <li>If related object is detached, note its field number</li>
 * <li>If “identity” set on this related object, and related object not persistent, flush the related object(s) to 
 * get their Key(s).</li>
 * </ol></li>
 * 
 * <li>PUT the Entity in datastore</li>
 * <li>Set any generated id back on the Entity</li>
 * <li>If fields noted in step 3.1
 * <ol>
 * <li>Reuse StoreFieldManager from above, and process fields noted earlier.
 * <ol>
 * <li>Attach any detached related objects</li>
 * <li>Persist and flush new related object(s), and add property(s) for relation fields to the Entity</li>
 * </ol></li>
 * <li>PUT the updated Entity in datastore</li>
 * </ol></li>
 * 
 * </ol>
 * </p>
 * 
 * <p>
 * <b>PersistenceHandler.updateObject</b><br/>
 * <ol>
 * <li>GET Entity that represents the object</li>
 * <li>Populate all updated fields that have values ready, forcing the flush of any relation fields that don't have 
 * their id present, and attach any detached related objects</li>
 * <li>PUT the updated Entity in datastore</li>
 * </ol>
 * </p>
 * <p>
 * <b>PersistenceHandler.deleteObject</b><br/>
 * <ol>
 * <li>GET Entity that represents the object</li>
 * <li>Handle any cascade deletion</li>
 * <li>DELETE the Entity from datastore</li>
 * </ol>
 * </p>
 * 
 * @author Max Ross <maxr@google.com>
 * @author Andy Jefferson
 */
public class DatastorePersistenceHandler extends AbstractPersistenceHandler {
  protected static final Localiser GAE_LOCALISER = Localiser.getInstance(
      "com.google.appengine.datanucleus.Localisation", DatastoreManager.class.getClassLoader());

  private final Map<ExecutionContext, BatchPutManager> batchPutManagerByExecutionContext = new ConcurrentHashMap();

  private final Map<ExecutionContext, BatchDeleteManager> batchDeleteManagerByExecutionContext = new ConcurrentHashMap();

  private final DatastoreManager datastoreMgr;

  /**
   * Constructor.
   * @param storeMgr The StoreManager to use.
   */
  public DatastorePersistenceHandler(StoreManager storeMgr) {
    super(storeMgr);
    this.datastoreMgr = (DatastoreManager) storeMgr;
  }

  public void close() {}

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
   * Method to insert the specified managed object into the datastore.
   * @param op ObjectProvider for the managed object
   */
  public void insertObject(ObjectProvider op) {
    // Make sure writes are permitted
    assertReadOnlyForUpdateOfObject(op);
    datastoreMgr.validateMetaDataForClass(op.getClassMetaData());

    // If we're in the middle of a batch operation just register the ObjectProvider that needs the insertion
    BatchPutManager batchPutMgr = getBatchPutManager(op.getExecutionContext());
    if (batchPutMgr.batchOperationInProgress()) {
      batchPutMgr.add(op);
      return;
    }

    insertObjectsInternal(Collections.singletonList(op));
  }

  /**
   * Method to perform the work of inserting the specified objects. If multiple are to be inserted then
   * performs it initially as a batch PUT. If some of these need subsequent work (e.g forcing the persist of 
   * children followed by a repersist to link to the children) then this is done one-by-one.
   */
  void insertObjectsInternal(List<ObjectProvider> opsToInsert) {
    if (opsToInsert == null || opsToInsert.isEmpty()) {
      return;
    }

    // All must be in same ExecutionContext
    ExecutionContext ec = opsToInsert.get(0).getExecutionContext();
    List<PutState> putStateList = Utils.newArrayList();
    for (ObjectProvider op : opsToInsert) {
      AbstractClassMetaData cmd = op.getClassMetaData();

      // Create the Entity, and populate all fields that can be populated (this will omit any owned child objects 
      // if we don't have the key of this object yet).
      StoreFieldManager fieldMgr =
        new StoreFieldManager(op, EntityUtils.determineKind(cmd, ec));
      op.provideFields(op.getClassMetaData().getAllMemberPositions(), fieldMgr);

      // Make sure the Entity parent is set (if any)
      Object assignedParentPk = fieldMgr.establishEntityGroup();
      Entity entity = fieldMgr.getEntity();

      if (!datastoreMgr.storageVersionAtLeast(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS)) {
        // Older storage versions : store list positions in the element
        DatastoreTable table = datastoreMgr.getDatastoreClass(op.getClassMetaData().getFullClassName(),
            ec.getClassLoaderResolver());
        Collection<JavaTypeMapping> orderMappings = table.getExternalOrderMappings().values();
        for (JavaTypeMapping orderMapping : orderMappings) {
          if (orderMapping instanceof IndexMapping) {
            Object orderValue = op.getAssociatedValue(orderMapping);
            if (orderValue != null) {
              // Set order index on the entity
              DatastoreField indexProp = orderMapping.getDatastoreMapping(0).getDatastoreField();
              entity.setProperty(indexProp.getIdentifier().toString(), orderValue); // Is this indexed in the datastore?
            } else {
              // Element has been persisted and has the owner set, but not positioned, so leave til user does it
            }
          }
        }
      }

      // Set version
      handleVersioningBeforeWrite(op, entity, true, "inserting");

      // Set discriminator
      if (op.getClassMetaData().hasDiscriminatorStrategy()) {
        DiscriminatorMetaData dismd = op.getClassMetaData().getDiscriminatorMetaDataRoot();
        EntityUtils.setEntityProperty(entity, dismd, 
            EntityUtils.getDiscriminatorPropertyName(datastoreMgr.getIdentifierFactory(), dismd), 
            op.getClassMetaData().getDiscriminatorValue());
      }

      // Add Multi-tenancy discriminator if applicable
      if (storeMgr.getStringProperty(PropertyNames.PROPERTY_TENANT_ID) != null) {
          if ("true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable"))) {
              // Don't bother with multitenancy for this class
          }
          else {
            String name = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.MULTITENANCY_COLUMN);
            EntityUtils.setEntityProperty(entity, cmd, name, storeMgr.getStringProperty(PropertyNames.PROPERTY_TENANT_ID));
          }
      }

      // Update parent PK field on pojo
      AbstractMemberMetaData parentPkMmd = datastoreMgr.getMetaDataForParentPK(cmd);
      if (assignedParentPk != null) {
        // we automatically assigned a parent to the entity so make sure that makes it back on to the pojo
        op.replaceField(parentPkMmd.getAbsoluteFieldNumber(), assignedParentPk);
      }

      // Add the "state" for this put to the list.
      putStateList.add(new PutState(op, fieldMgr, entity));
    }

    // PUT all entities in single call
    if (!putStateList.isEmpty()) {
      DatastoreTransaction txn = null;
      AbstractClassMetaData acmd = null;
      List<Entity> entityList = Utils.newArrayList();
      for (PutState putState : putStateList) {
        if (txn == null) {
          txn = datastoreMgr.getDatastoreTransaction(ec);
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
      if (cmd.pkIsDatastoreAttributed(storeMgr)) {
        if (cmd.getIdentityType() == IdentityType.APPLICATION) {
          // Assume only 1 PK field
          identityStrategyUsed = true;
          pkType = cmd.getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[0]).getType();
        } else if (cmd.getIdentityType() == IdentityType.DATASTORE) {
          identityStrategyUsed = true;
          pkType = Key.class;
          ColumnMetaData colmd = cmd.getIdentityMetaData().getColumnMetaData();
          if (colmd != null) {
            if ("varchar".equalsIgnoreCase(colmd.getJdbcType()) || "char".equalsIgnoreCase(colmd.getJdbcType())) {
              pkType = String.class;
            } else if ("integer".equalsIgnoreCase(colmd.getJdbcType()) || "numeric".equalsIgnoreCase(colmd.getJdbcType())) {
              pkType = Long.class;
            }
          }
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

      // Update relation fields (including cascade-persist etc)
      if (putState.fieldMgr.storeRelations(KeyRegistry.getKeyRegistry(ec))) {
        // PUT Entity into datastore with these changes
        EntityUtils.putEntityIntoDatastore(ec, putState.entity);
      }

      putState.op.replaceAllLoadedSCOFieldsWithWrappers();

      if (ec.getStatistics() != null) {
        ec.getStatistics().incrementInsertCount();
      }
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
      // This happens with JPAOneToOneTest/JDOOneToOneTest when deleting, called from DependentDeleteRequest
      return;
    }

    // Make sure writes are permitted
    assertReadOnlyForUpdateOfObject(op);
    datastoreMgr.validateMetaDataForClass(op.getClassMetaData());

    AbstractClassMetaData cmd = op.getClassMetaData();
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
          StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId(), fieldStr.toString()));
    }

    ExecutionContext ec = op.getExecutionContext();
    Entity entity = (Entity) op.getAssociatedValue(datastoreMgr.getDatastoreTransaction(ec));
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = EntityUtils.getPkAsKey(op);
      entity = EntityUtils.getEntityFromDatastore(datastoreMgr.getDatastoreServiceForReads(ec), op, key);
    }

    // Update the Entity with the specified fields
    StoreFieldManager fieldMgr = new StoreFieldManager(op, entity, fieldNumbers);
    op.provideFields(fieldNumbers, fieldMgr);

    // Check and update the version
    handleVersioningBeforeWrite(op, entity, true, "updating");

    // Update relation fields (including cascade-persist etc)
    fieldMgr.storeRelations(KeyRegistry.getKeyRegistry(op.getExecutionContext()));

    // PUT Entity into datastore
    DatastoreTransaction txn = EntityUtils.putEntityIntoDatastore(ec, entity);
    op.setAssociatedValue(txn, entity);

    op.replaceAllLoadedSCOFieldsWithWrappers();

    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.ExecutionTime", 
        (System.currentTimeMillis() - startTime)));
    }
    if (ec.getStatistics() != null) {
      ec.getStatistics().incrementUpdateCount();
    }
  }

  /**
   * Method to delete the specified managed object from the datastore.
   * @param op ObjectProvider of the managed object
   */
  public void deleteObject(ObjectProvider op) {
    // Make sure writes are permitted
    assertReadOnlyForUpdateOfObject(op);
    datastoreMgr.validateMetaDataForClass(op.getClassMetaData());

    long startTime = System.currentTimeMillis();
    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.Delete.Start", 
          StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId()));
    }

    ExecutionContext ec = op.getExecutionContext();
    Entity entity = (Entity) op.getAssociatedValue(datastoreMgr.getDatastoreTransaction(ec));
    if (entity == null) {
      // Corresponding entity hasn't been fetched yet, so get it.
      Key key = EntityUtils.getPkAsKey(op);
      entity = EntityUtils.getEntityFromDatastore(datastoreMgr.getDatastoreServiceForReads(ec), op, key);
    }

    DatastoreTransaction txn = datastoreMgr.getDatastoreTransaction(ec);
    if (txn != null) {
      txn.addDeletedKey(entity.getKey());
    }

    // Check the version is valid to delete; any updates since read?
    handleVersioningBeforeWrite(op, entity, false, "deleting");

    // first handle any dependent deletes that need deleting before we delete this object
    ClassLoaderResolver clr = ec.getClassLoaderResolver();
    DatastoreClass dc = datastoreMgr.getDatastoreClass(op.getObject().getClass().getName(), clr);
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
      if (ec.getStatistics() != null) {
        ec.getStatistics().incrementDeleteCount();
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
    if (ec.getStatistics() != null) {
      ec.getStatistics().incrementDeleteCount();
    }

    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled()) {
      NucleusLogger.DATASTORE_PERSIST.debug(GAE_LOCALISER.msg("AppEngine.ExecutionTime", 
        (System.currentTimeMillis() - startTime)));
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
    datastoreMgr.validateMetaDataForClass(cmd);

    // We always fetch the entire object, so if the state manager
    // already has an associated Entity we know that associated
    // Entity has all the fields.
    ExecutionContext ec = op.getExecutionContext();
    Entity entity = (Entity) op.getAssociatedValue(datastoreMgr.getDatastoreTransaction(ec));
    if (entity == null) {
      Key pk = EntityUtils.getPkAsKey(op);
      entity = EntityUtils.getEntityFromDatastore(datastoreMgr.getDatastoreServiceForReads(ec), op, pk); // Throws NucleusObjectNotFoundException if necessary
    }

    if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled()) {
      // Debug information about what we are retrieving
      StringBuffer str = new StringBuffer("Fetching object \"");
      str.append(StringUtils.toJVMIDString(op.getObject())).append("\" (id=");
      str.append(op.getInternalObjectId()).append(")").append(" fields [");
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
          StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId()));
    }

    op.replaceFields(fieldNumbers, new FetchFieldManager(op, entity, fieldNumbers));

    // Refresh version in case not yet set (e.g created HOLLOW object, and this is first fetch)
    VersionMetaData vmd = cmd.getVersionMetaDataForClass();
    if (cmd.isVersioned()) {
      Object versionValue = entity.getProperty(EntityUtils.getVersionPropertyName(datastoreMgr.getIdentifierFactory(), vmd));
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
    ClassLoaderResolver clr = ec.getClassLoaderResolver();
    DatastoreClass dc = datastoreMgr.getDatastoreClass(op.getObject().getClass().getName(), clr);
    FetchMappingConsumer consumer = new FetchMappingConsumer(op.getClassMetaData());
    dc.provideMappingsForMembers(consumer, fmds, true);
    dc.provideDatastoreIdMappings(consumer);
    dc.providePrimaryKeyMappings(consumer);
    for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
      // Arrays and Maps don't use backing stores
      if (callback instanceof ArrayMapping || callback instanceof MapMapping) {
        // Do nothing since arrays and maps are stored in the parent property and loaded above using FetchFieldManager
      } else if (callback instanceof CollectionMapping) {
        CollectionMapping m = (CollectionMapping)callback;
        Object val = op.provideField(m.getMemberMetaData().getAbsoluteFieldNumber());
        if (val == null || !(val instanceof SCO)) {
          // Not yet wrapped, so make sure we wrap it
          callback.postFetch(op);
        }
      } else {
        callback.postFetch(op);
      }
    }

    if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_RETRIEVE.debug(GAE_LOCALISER.msg("AppEngine.ExecutionTime",
            (System.currentTimeMillis() - startTime)));
    }
    if (ec.getStatistics() != null) {
      ec.getStatistics().incrementFetchCount();
    }
  }

  /**
   * Method to locate the specified managed objects in the datastore.
   * @param ops ObjectProviders for the managed objects
   * @throws NucleusObjectNotFoundException if any of the objects aren't found in the datastore
   */
  public void locateObjects(ObjectProvider[] ops) {
    if (ops == null) {
      return;
    }

    List<Key> keysToLocate = Utils.newArrayList();
    for (int i=0;i<ops.length;i++) {
      Key key = EntityUtils.getPkAsKey(ops[i]);
      keysToLocate.add(key);
    }
    EntityUtils.getEntitiesFromDatastore(datastoreMgr.getDatastoreServiceForReads(ops[0].getExecutionContext()),
        keysToLocate, ops[0].getExecutionContext());
  }

  /**
   * Method to locate the specified managed object in the datastore.
   * @param op ObjectProvider for the managed object
   * @throws NucleusObjectNotFoundException if the object isn't found in the datastore
   */
  public void locateObject(ObjectProvider op) {
    datastoreMgr.validateMetaDataForClass(op.getClassMetaData());
    EntityUtils.getEntityFromDatastore(datastoreMgr.getDatastoreServiceForReads(op.getExecutionContext()), op, 
        EntityUtils.getPkAsKey(op));
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
      ExecutionContext ec = op.getExecutionContext();
      String versionPropertyName = EntityUtils.getVersionPropertyName(datastoreMgr.getIdentifierFactory(), vmd);
      Object curVersion = op.getVersion();
      if (curVersion != null) {
        // Fetch the latest and greatest version of the entity from the datastore
        // to see if anyone has made a change underneath us.  We need to execute
        // the fetch outside a txn to guarantee that we see the latest version.
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
          NucleusLogger.DATASTORE_NATIVE.debug("Getting entity with key " + entity.getKey());
        }
        Entity refreshedEntity;
        try {
          if (ec.getStatistics() != null) {
            ec.getStatistics().incrementNumReads();
          }
          refreshedEntity = datastoreMgr.getDatastoreServiceForReads(op.getExecutionContext()).get(entity.getKey());
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
    private final Entity entity;

    private PutState(ObjectProvider op, StoreFieldManager fieldMgr, Entity entity) {
      this.op = op;
      this.fieldMgr = fieldMgr;
      this.entity = entity;
    }
  }
}
