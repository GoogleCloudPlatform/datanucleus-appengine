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
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NoPersistenceInformationException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;

import org.datanucleus.state.ObjectProviderFactory;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.mapped.mapping.EmbeddedMapping;
import org.datanucleus.store.mapped.mapping.IndexMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.spi.JDOImplHelper;

/**
 * FieldManager for converting app engine datastore entities into POJOs and vice-versa.
 *
 * Most of the complexity in this class is due to the fact that the datastore
 * automatically promotes certain types:
 * It promotes short/Short, int/Integer, and byte/Byte to long.
 * It also promotes float/Float to double.
 *
 * Also, the datastore does not support char/Character.  We've made the decision
 * to promote this to long as well.
 *
 * We handle the conversion in both directions.  At one point we let the
 * datastore api do the conversion from pojos to {@link Entity Entities} but we
 * this proved problematic in the case where we return entities that were
 * cached during insertion to avoid
 * issuing a get().  In this case we then end up trying to construct a pojo from
 * an {@link Entity} whose contents violate the datastore api invariants, and we
 * end up with cast exceptions.  So, we do the conversion ourselves, even though
 * this duplicates logic in the ORM and the datastore api.
 *
 * @author Max Ross <maxr@google.com>
 */
public abstract class DatastoreFieldManager extends AbstractFieldManager {

  private static final int[] NOT_USED = {0};

  private static final TypeConversionUtils TYPE_CONVERSION_UTILS = new TypeConversionUtils();

  // Stack used to maintain the current field manager state to use.  We push on
  // to this stack as we encounter embedded classes and then pop when we're done.
  protected final LinkedList<FieldManagerState> fieldManagerStateStack =
      new LinkedList<FieldManagerState>();

  // true if we instantiated the entity ourselves.
  protected final boolean createdWithoutEntity;

  protected final DatastoreManager storeManager;

  protected final DatastoreRelationFieldManager relationFieldManager;

  protected ObjectProvider op;

  // Not final because we will reallocate if we hit a parent pk field and the
  // key of the current value does not have a parent, or if the pk gets set.
  protected Entity datastoreEntity;

  // We'll assign this if we have a parent member and we store a value into it.
  protected AbstractMemberMetaData parentMemberMetaData;

  protected boolean parentAlreadySet = false;
  protected boolean keyAlreadySet = false;
  protected Integer pkIdPos = null;
  protected boolean repersistingForChildKeys = false;

  private DatastoreFieldManager(ObjectProvider op, boolean createdWithoutEntity,
      DatastoreManager storeManager, Entity datastoreEntity, int[] fieldNumbers) {
    // We start with an ammdProvider that just gets member meta data from the class meta data.
    AbstractMemberMetaDataProvider ammdProvider = new AbstractMemberMetaDataProvider() {
      public AbstractMemberMetaData get(int fieldNumber) {
        return getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
      }
    };
    this.op = op;
    this.createdWithoutEntity = createdWithoutEntity;
    this.storeManager = storeManager;
    this.datastoreEntity = datastoreEntity;
    InsertMappingConsumer mappingConsumer = buildMappingConsumer(op.getClassMetaData(), 
        op.getExecutionContext().getClassLoaderResolver(), fieldNumbers);
    this.fieldManagerStateStack.addFirst(new FieldManagerState(op, ammdProvider, mappingConsumer, false));
    this.relationFieldManager = new DatastoreRelationFieldManager(this);

    // Sanity check
    String expectedKind = EntityUtils.determineKind(getClassMetaData(), op.getExecutionContext());
    if (!expectedKind.equals(datastoreEntity.getKind())) {
      throw new NucleusException(
          "StateManager is for <" + expectedKind + "> but key is for <" + datastoreEntity.getKind()
              + ">.  One way this can happen is if you attempt to fetch an object of one type using"
              + " a Key of a different type.").setFatal();
    }
  }

  /**
   * Creates a DatastoreFieldManager using the given StateManager and Entity.
   * Only use this overload when you have been provided with an Entity object
   * that already has a well-formed Key.  This will be the case when the entity
   * has been returned by the datastore (get or query), or after the entity has
   * been put into the datastore.
   */
  DatastoreFieldManager(ObjectProvider op, DatastoreManager storeManager,
      Entity datastoreEntity, int[] fieldNumbers) {
    this(op, false, storeManager, datastoreEntity, fieldNumbers);
  }

  public DatastoreFieldManager(ObjectProvider op, DatastoreManager storeManager,
      Entity datastoreEntity) {
    this(op, false, storeManager, datastoreEntity, new int[0]);
  }

  DatastoreFieldManager(ObjectProvider op, String kind,
      DatastoreManager storeManager) {
    this(op, true, storeManager, new Entity(kind), new int[0]);
  }

  protected boolean fieldIsOfTypeKey(int fieldNumber) {
    // Key is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(Key.class);
  }

  protected boolean fieldIsOfTypeString(int fieldNumber) {
    // String is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(String.class);
  }

  protected boolean fieldIsOfTypeLong(int fieldNumber) {
    // Long is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(Long.class);
  }

  protected RuntimeException exceptionForUnexpectedKeyType(String fieldType, int fieldNumber) {
    return new IllegalStateException(
        fieldType + " for type " + getClassMetaData().getName()
            + " is of unexpected type " + getMetaData(fieldNumber).getType().getName()
            + " (must be String, Long, or " + Key.class.getName() + ")");
  }

  protected AbstractMemberMetaDataProvider getEmbeddedAbstractMemberMetaDataProvider(
      final InsertMappingConsumer consumer) {
    // This implementation gets the meta data from the mapping consumer.
    // This is needed to ensure we see column overrides that are specific to
    // a specific embedded field and subclass fields, which aren't included
    // in EmbeddedMetaData for some reason.
    return new AbstractMemberMetaDataProvider() {
      public AbstractMemberMetaData get(int fieldNumber) {
        return consumer.getMemberMetaDataForIndex(fieldNumber);
      }
    };
  }

  protected ObjectProvider getEmbeddedObjectProvider(AbstractMemberMetaData ammd, int fieldNumber, Object value) {
    if (value == null) {
      // Not positive this is the right approach, but when we read the values
      // of an embedded field out of the datastore we have no way of knowing
      // if the field should be null or it should contain an instance of the
      // embeddable whose members are all initialized to their default values
      // (the result of calling the default ctor).  Also, we can't risk
      // storing 'null' for every field of the embedded class because some of
      // the members might be base types and therefore non-nullable.  Writing
      // nulls to the datastore for these fields would cause NPEs when we read
      // the object back out.  Seems like the only safe thing to do here is
      // instantiate a fresh instance of the embeddable class using the default
      // constructor and then persist that.
        // TODO Use metadata nullIndicatorColumn/Value
      value = JDOImplHelper.getInstance().newInstance(
          ammd.getType(), (javax.jdo.spi.StateManager) getObjectProvider());
    }
    ObjectProvider embeddedOP = getExecutionContext().findObjectProvider(value);
    if (embeddedOP == null) {
        embeddedOP = ObjectProviderFactory.newForEmbedded(getExecutionContext(), value, false, getObjectProvider(), fieldNumber);
        embeddedOP.setPcObjectType(ObjectProvider.EMBEDDED_PC);
    }
    return embeddedOP;
  }

  /**
   * Currently all relationships are parent-child.  If a datastore entity
   * doesn't have a parent there are 4 places we can look for one.
   * 1)  It's possible that a pojo in the cascade chain registered itself as
   * the parent.
   * 2)  It's possible that the pojo has an external foreign key mapping
   * to the object that owns it, in which case we can use the key of that field
   * as the parent.
   * 3)  It's possible that the pojo has a field containing the parent that is
   * not an external foreign key mapping but is labeled as a "parent
   * provider" (this is an app engine orm term).  In this case, as with
   * #2, we can use the key of that field as the parent.
   * 4) If part of the attachment process we can consult the ExecutionContext
   * for the owner of this object (that caused the attach of this object).
   *
   * It _should_ be possible to get rid of at least one of these
   * mechanisms, most likely the first.
   *
   * @return The parent key if the pojo class has a parent property.
   * Note that a return value of {@code null} does not mean that an entity
   * group was not established, it just means the pojo doesn't have a distinct
   * field for the parent.
   */
  Object establishEntityGroup() {
    Key parentKey = getEntity().getParent();
    if (parentKey == null) {
      ObjectProvider op = getObjectProvider();
      // Mechanism 1
      parentKey = KeyRegistry.getKeyRegistry(getExecutionContext()).getRegisteredKey(op.getObject());
      if (parentKey == null) {
        // Mechanism 2
        parentKey = getParentKeyFromExternalFKMappings(op);
      }
      if (parentKey == null) {
        // Mechanism 3
        parentKey = getParentKeyFromParentField(op);
      }
      if (parentKey == null) {
        // Mechanism 4, use attach parent info from ExecutionContext
        ObjectProvider ownerOP = op.getExecutionContext().getObjectProviderOfOwnerForAttachingObject(op.getObject());
        if (ownerOP != null) {
          Object parentPojo = ownerOP.getObject();
          parentKey = getKeyFromParentPojo(parentPojo);
        }
      }
//      if (parentKey == null) {
//        // Mechanism 4
//        Object parentPojo = DatastoreJPACallbackHandler.getAttachingParent(op.getObject());
//        if (parentPojo != null) {
//          parentKey = getKeyFromParentPojo(parentPojo);
//        }
//      }
      if (parentKey != null) {
        recreateEntityWithParent(parentKey);
      }
    }
    if (parentKey != null && getParentMemberMetaData() != null) {
      return getParentMemberMetaData().getType().equals(Key.class)
          ? parentKey : KeyFactory.keyToString(parentKey);
    }
    return null;
  }

  void recreateEntityWithParent(Key parentKey) {
    Entity old = datastoreEntity;
    if (old.getKey().getName() != null) {
      datastoreEntity =
          new Entity(old.getKind(), old.getKey().getName(), parentKey);
    } else {
      datastoreEntity = new Entity(old.getKind(), parentKey);
    }
    copyProperties(old, datastoreEntity);
  }

  private Key getKeyFromParentPojo(Object mergeEntity) {
    ObjectProvider mergeOP = getExecutionContext().findObjectProvider(mergeEntity);
    if (mergeOP == null) {
      return null;
    }
    return EntityUtils.getPrimaryKeyAsKey(getExecutionContext().getApiAdapter(), mergeOP);
  }

  private Key getKeyForObject(Object pc) {
    ApiAdapter adapter = getStoreManager().getApiAdapter();
    Object internalPk = adapter.getTargetKeyForSingleFieldIdentity(adapter.getIdForObject(pc));
    AbstractClassMetaData acmd =
        getExecutionContext().getMetaDataManager().getMetaDataForClass(pc.getClass(), getClassLoaderResolver());
    return EntityUtils.getPkAsKey(internalPk, acmd, getExecutionContext());
  }

  private Key getParentKeyFromParentField(ObjectProvider op) {
    AbstractMemberMetaData parentField = getInsertMappingConsumer().getParentMappingField();
    if (parentField == null) {
      return null;
    }
    Object parent = op.provideField(parentField.getAbsoluteFieldNumber());
    return parent == null ? null : getKeyForObject(parent);
  }

  private Key getParentKeyFromExternalFKMappings(ObjectProvider op) {
    // We don't have a registered key for the object associated with the
    // state manager but there might be one tied to the foreign key
    // mappings for this object.  If this is the Many side of a bidirectional
    // One To Many it might also be available on the parent object.
    // TODO(maxr): Unify the 2 mechanisms.  We probably want to get rid of the KeyRegistry.
    Set<JavaTypeMapping> externalFKMappings = getInsertMappingConsumer().getExternalFKMappings();
    for (JavaTypeMapping fkMapping : externalFKMappings) {
      Object fkValue = op.getAssociatedValue(fkMapping);
      if (fkValue != null) {
        return getKeyForObject(fkValue);
      }
    }
    return null;
  }

  private static final Field PROPERTY_MAP_FIELD;
  static {
    try {
      PROPERTY_MAP_FIELD = Entity.class.getDeclaredField("propertyMap");
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    PROPERTY_MAP_FIELD.setAccessible(true);
  }

  // TODO(maxr) Get rid of this once we have a formal way of figuring out
  // which properties are indexed and which are unindexed.
  private static Map<String, Object> getPropertyMap(Entity entity) {
    try {
      return (Map<String, Object>) PROPERTY_MAP_FIELD.get(entity);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  // TODO(maxr) Rewrite once Entity.setPropertiesFrom() is available.
  static void copyProperties(Entity src, Entity dest) {
    for (Map.Entry<String, Object> entry : getPropertyMap(src).entrySet()) {
      // barf
      if (entry.getValue() != null &&
          entry.getValue().getClass().getName().equals("com.google.appengine.api.datastore.Entity$UnindexedValue")) {
        dest.setUnindexedProperty(entry.getKey(), src.getProperty(entry.getKey()));
      } else {
        dest.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  protected boolean isPKNameField(int fieldNumber) {
    return DatastoreManager.isPKNameField(getClassMetaData(), fieldNumber);
  }

  protected boolean isPKIdField(int fieldNumber) {
    return DatastoreManager.isPKIdField(getClassMetaData(), fieldNumber);
  }

  protected boolean isParentPK(int fieldNumber) {
    return DatastoreManager.isParentPKField(getClassMetaData(), fieldNumber);
  }

  /**
   * @see DatastoreRelationFieldManager#storeRelations
   */
  boolean storeRelations() {
    return relationFieldManager.storeRelations(KeyRegistry.getKeyRegistry(getExecutionContext()));
  }

  ClassLoaderResolver getClassLoaderResolver() {
    return getExecutionContext().getClassLoaderResolver();
  }

  ObjectProvider getObjectProvider() {
    return fieldManagerStateStack.getFirst().op;
  }

  ExecutionContext getExecutionContext() {
    return getObjectProvider().getExecutionContext();
  }

  InsertMappingConsumer getInsertMappingConsumer() {
    return fieldManagerStateStack.getFirst().mappingConsumer;
  }

  protected boolean isPK(int fieldNumber) {
    // ignore the pk annotations if this object is embedded
    if (fieldManagerStateStack.getFirst().isEmbedded) {
      return false;
    }
    int[] pkPositions = getClassMetaData().getPKMemberPositions();
    // Assumes that if we have a pk we only have a single field pk
    return pkPositions != null && pkPositions[0] == fieldNumber;
  }

  protected AbstractMemberMetaData getMetaData(int fieldNumber) {
    return fieldManagerStateStack.getFirst().abstractMemberMetaDataProvider.get(fieldNumber);
  }

  AbstractClassMetaData getClassMetaData() {
    return getObjectProvider().getClassMetaData();
  }

  Entity getEntity() {
    return datastoreEntity;
  }

  AbstractMemberMetaData getParentMemberMetaData() {
    return parentMemberMetaData;
  }

  DatastoreManager getStoreManager() {
    return storeManager;
  }

  /**
   * In JDO, 1-to-many relationsihps that are expressed using a
   * {@link List} are ordered by a column in the child
   * table that stores the position of the child in the parent's list.
   * This function is responsible for making sure the appropriate values
   * for these columns find their way into the Entity.  In certain scenarios,
   * DataNucleus does not make the index of the container element being
   * written available until later on in the workflow.  The expectation
   * is that we will insert the record without the index and then perform
   * an update later on when the value becomes available.  This is problematic
   * for the App Engine datastore because we can only write an entity once
   * per transaction.  So, to get around this, we detect the case where the
   * index is not available and instruct the caller to hold off writing.
   * Later on in the workflow, when DataNucleus calls down into our plugin
   * to request the update with the index, we perform the insert.  This will
   * break someday.  Fortunately we have tests so we should find out.
   *
   * @return {@code true} if the caller (expected to be
   * {@link DatastorePersistenceHandler#insertObject}) should delay its write
   * of this object.
   */
  boolean handleIndexFields() {
    Set<JavaTypeMapping> orderMappings = getInsertMappingConsumer().getExternalOrderMappings();
    boolean delayWrite = false;
    for (JavaTypeMapping orderMapping : orderMappings) {
      if (orderMapping instanceof IndexMapping) {
        delayWrite = true;
        // DataNucleus hides the value in the state manager, keyed by the
        // mapping for the order field.
        Object orderValue = getObjectProvider().getAssociatedValue(orderMapping);
        if (orderValue != null) {
          // We got a value!  Set it on the entity.
          delayWrite = false;
          orderMapping.setObject(getExecutionContext(), getEntity(), NOT_USED, orderValue);
        }
      }
    }
    return delayWrite;
  }

  private InsertMappingConsumer buildMappingConsumer(AbstractClassMetaData acmd, ClassLoaderResolver clr, int[] fieldNumbers) {
    return buildMappingConsumer(acmd, clr, fieldNumbers, null);
  }

  /**
   * Constructs a {@link MappingConsumer}.  If an {@link EmbeddedMetaData} is
   * provided that means we need to construct the consumer in an embedded
   * context.
   */
  protected InsertMappingConsumer buildMappingConsumer(
      AbstractClassMetaData acmd, ClassLoaderResolver clr, int[] fieldNumbers, EmbeddedMetaData emd) {
    DatastoreTable table = getStoreManager().getDatastoreClass(acmd.getFullClassName(), clr);
    if (table == null) {
      // We've seen this when there is a class with the superclass inheritance
      // strategy that does not have a parent
      throw new NoPersistenceInformationException(acmd.getFullClassName());
    }

    InsertMappingConsumer consumer = new InsertMappingConsumer(acmd);
    if (emd == null) {
      table.provideDatastoreIdMappings(consumer);
      table.providePrimaryKeyMappings(consumer);
    } else {
      // skip pk mappings if embedded
    }
    table.provideParentMappingField(consumer);

    if (createdWithoutEntity || emd != null) {
      // This is the insert case or we're dealing with an embedded field.
      // Either way we want to fill the consumer with mappings for everything.
      table.provideNonPrimaryKeyMappings(consumer, emd != null);
      table.provideExternalMappings(consumer, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
      table.provideExternalMappings(consumer, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
    } else {
      // This is the update case.  We only want to fill the consumer mappings
      // for the specific fields that were provided.
      AbstractMemberMetaData[] fmds = new AbstractMemberMetaData[fieldNumbers.length];
      if (fieldNumbers.length > 0) {
        for (int i = 0; i < fieldNumbers.length; i++) {
          fmds[i] = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
        }
      }
      table.provideMappingsForMembers(consumer, fmds, false);
    }

    if (emd != null) {
      DatastoreTable parentTable = getStoreManager().getDatastoreClass(getClassMetaData().getFullClassName(), clr);
      AbstractMemberMetaData parentField = (AbstractMemberMetaData) emd.getParent();
      EmbeddedMapping embeddedMapping =
          (EmbeddedMapping) parentTable.getMappingForFullFieldName(parentField.getFullFieldName());

      // Build a map that takes us from full field name to member meta data.
      // The member meta data in this map comes from the embedded mapping,
      // which means it will have subclass fields and column overrides.
      Map<String, AbstractMemberMetaData> embeddedMetaDataByFullFieldName = Utils.newHashMap();
      int numMappings = embeddedMapping.getNumberOfJavaTypeMappings();
      for (int i = 0; i < numMappings; i++) {
        JavaTypeMapping fieldMapping = embeddedMapping.getJavaTypeMapping(i);
        AbstractMemberMetaData ammd = fieldMapping.getMemberMetaData();
        embeddedMetaDataByFullFieldName.put(ammd.getFullFieldName(), ammd);
      }

      // We're going to update the consumer's map so make a copy over which we can safely iterate.
      Map<Integer, AbstractMemberMetaData> map =
          new HashMap<Integer, AbstractMemberMetaData>(consumer.getFieldIndexToMemberMetaData());
      for (Map.Entry<Integer, AbstractMemberMetaData> entry : map.entrySet()) {
        // For each value in the consumer's map, find the corresponding value in
        // the embeddedMetaDataByFullFieldName we just built and install it as
        // a replacement.  This will given us access to subclass fields and
        // column overrides as we fetch/store the embedded field.
        AbstractMemberMetaData replacement = embeddedMetaDataByFullFieldName.get(entry.getValue().getFullFieldName());
        if (replacement == null) {
          throw new RuntimeException(
              "Unable to locate " + entry.getValue().getFullFieldName() + " in embedded meta-data "
              + "map.  This is most likely an App Engine bug.");
        }
        consumer.getFieldIndexToMemberMetaData().put(entry.getKey(), replacement);
      }
    }
    return consumer;
  }

  /**
   * Just exists so we can override in tests. 
   */
  TypeConversionUtils getConversionUtils() {
    return TYPE_CONVERSION_UTILS;
  }

  Integer getPkIdPos() {
    return pkIdPos;
  }

  void setRepersistingForChildKeys(boolean repersistingForChildKeys) {
    this.repersistingForChildKeys = repersistingForChildKeys;
  }

  DatastoreTable getDatastoreTable() {
    return storeManager.getDatastoreClass(getClassMetaData().getFullClassName(), getClassLoaderResolver());
  }

  /**
   * Translates field numbers into {@link AbstractMemberMetaData}.
   */
  protected interface AbstractMemberMetaDataProvider {
    AbstractMemberMetaData get(int fieldNumber);
  }

  protected static final class FieldManagerState {
    private final ObjectProvider op;
    private final AbstractMemberMetaDataProvider abstractMemberMetaDataProvider;
    private final InsertMappingConsumer mappingConsumer;
    private final boolean isEmbedded;

    protected FieldManagerState(ObjectProvider op,
        AbstractMemberMetaDataProvider abstractMemberMetaDataProvider,
        InsertMappingConsumer mappingConsumer,
        boolean isEmbedded) {
      this.op = op;
      this.abstractMemberMetaDataProvider = abstractMemberMetaDataProvider;
      this.mappingConsumer = mappingConsumer;
      this.isEmbedded = isEmbedded;
    }
  }
}