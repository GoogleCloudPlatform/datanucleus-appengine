/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.exceptions.NotYetFlushedException;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.InterfaceMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.PersistableMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.sco.SCO;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.datanucleus.mapping.DatastoreTable;
import com.google.appengine.datanucleus.mapping.InsertMappingConsumer;

/**
 * FieldManager to handle the putting of fields from a managed object into an Entity.
 */
public class StoreFieldManager extends DatastoreFieldManager {
  private static final String PARENT_ALREADY_SET =
    "Cannot set both the primary key and a parent pk field.  If you want the datastore to "
    + "generate an id for you, set the parent pk field to be the value of your parent key "
    + "and leave the primary key field blank.  If you wish to "
    + "provide a named key, leave the parent pk field blank and set the primary key to be a "
    + "Key object made up of both the parent key and the named child.";

  /**
   * Relation types where we want to store child keys on the parent
   */
  private static final Set<Integer> PARENT_RELATION_TYPES =
    Collections.synchronizedSet(Utils.newHashSet(
        Relation.ONE_TO_MANY_BI,
        Relation.ONE_TO_MANY_UNI,
        Relation.ONE_TO_ONE_BI,
        Relation.ONE_TO_ONE_UNI));

  public static final int IS_PARENT_VALUE = -1;
  private static final int[] IS_PARENT_VALUE_ARR = {IS_PARENT_VALUE};
  public static final String PARENT_KEY_PROPERTY = "____PARENT_KEY____";

  public static final int IS_FK_VALUE = -2;
  private static final int[] IS_FK_VALUE_ARR = {IS_FK_VALUE};

  protected enum Operation { INSERT, UPDATE };

  protected final Operation operation;

  protected boolean repersistingForChildKeys = false;

  protected boolean parentAlreadySet = false;

  protected boolean keyAlreadySet = false;

  // Events that know how to store relations.
  private final List<StoreRelationEvent> storeRelationEvents = Utils.newArrayList();

  /**
   * Constructor for a StoreFieldManager when inserting a new object.
   * The Entity will be constructed.
   * @param op ObjectProvider of the object being stored
   * @param kind Kind of entity
   */
  public StoreFieldManager(ObjectProvider op, String kind) {
    super(op, new Entity(kind), null);
    this.operation = Operation.INSERT;
  }

  /**
   * Constructor for a StoreFieldManager when updating an object.
   * The Entity will be passed in (to be updated).
   * @param op ObjectProvider of the object being stored
   * @param datastoreEntity The Entity to update with the field values
   * @param fieldNumbers The field numbers being updated in the Entity
   */
  public StoreFieldManager(ObjectProvider op, Entity datastoreEntity, int[] fieldNumbers) {
    super(op, datastoreEntity, fieldNumbers);
    this.operation = Operation.UPDATE;
  }

  public void storeBooleanField(int fieldNumber, boolean value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeByteField(int fieldNumber, byte value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeCharField(int fieldNumber, char value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeDoubleField(int fieldNumber, double value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeFloatField(int fieldNumber, float value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeIntField(int fieldNumber, int value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeLongField(int fieldNumber, long value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeShortField(int fieldNumber, short value) {
    storeFieldInEntity(fieldNumber, value);
  }

  public void storeStringField(int fieldNumber, String value) {
    if (isPK(fieldNumber)) {
      storeStringPKField(fieldNumber, value);
    } else if (MetaDataUtils.isParentPKField(getClassMetaData(), fieldNumber)) {
      storeParentStringField(value);
    } else if (MetaDataUtils.isPKNameField(getClassMetaData(), fieldNumber)) {
      storePKNameField(fieldNumber, value);
    } else {
      // could be a JPA "lob" field, in which case we want to store it as Text.
      // DataNucleus sets a cmd with a jdbc type of CLOB if this is the case.
      Object valueToStore = value;
      AbstractMemberMetaData ammd = getMetaData(fieldNumber);
      if (ammd.getColumnMetaData() != null &&
          ammd.getColumnMetaData().length == 1) {
        if ("CLOB".equals(ammd.getColumnMetaData()[0].getJdbcType())) {
          valueToStore = new Text(value);
        }/* else if (ammd.getColumnMetaData()[0].getLength() > 500) {
          // Can only store up to 500 characters in String, so use Text
          valueToStore = new Text(value);
        }*/
      }

      storeFieldInEntity(fieldNumber, valueToStore);
    }
  }

  public void storeObjectField(int fieldNumber, Object value) {
    if (isPK(fieldNumber)) {
      storePrimaryKey(fieldNumber, value);
    } else if (MetaDataUtils.isParentPKField(getClassMetaData(), fieldNumber)) {
      storeParentField(fieldNumber, value);
    } else if (MetaDataUtils.isPKIdField(getClassMetaData(), fieldNumber)) {
      storePKIdField(fieldNumber, value);
    } else {
      ClassLoaderResolver clr = getClassLoaderResolver();
      AbstractMemberMetaData ammd = getMetaData(fieldNumber);
      int relationType = ammd.getRelationType(clr);

      // TODO Work out what this next block does! and then remove it
      if (repersistingForChildKeys && !PARENT_RELATION_TYPES.contains(relationType)) {
        // nothing for us to store
        return;
      }

      storeFieldInEntity(fieldNumber, value);

      if (!(value instanceof SCO)) {
        // TODO Wrap SCO fields, remove the relation check so it applies to all. See Issue 144
        // This is currently not done since the elements may not be persisted at this point and the test classes
        // rely on "id" being set for hashCode/equals to work. Fix the persistence process first
        if (relationType == Relation.NONE) {
          getObjectProvider().wrapSCOField(fieldNumber, value, false, false, true);
        }
      }
    }
  }

  /**
   * Method to store the provided value in the Entity for the specified field.
   * @param fieldNumber The absolute field number
   * @param value Value to store (or rather to manipulate into a suitable form for the datastore).
   */
  private void storeFieldInEntity(int fieldNumber, Object value) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    if (!(operation == Operation.INSERT && ammd.isInsertable()) &&
        !(operation == Operation.UPDATE && ammd.isUpdateable())) {
      return;
    }

    if (ammd.getEmbeddedMetaData() != null) {
      // Embedded field handling
      ObjectProvider embeddedOP = getEmbeddedObjectProvider(ammd, fieldNumber, value);
      // TODO Create own FieldManager instead of reusing this one
      // We need to build a mapping consumer for the embedded class so that we get correct
      // fieldIndex --> metadata mappings for the class in the proper embedded context
      // TODO(maxr) Consider caching this
      InsertMappingConsumer mc = buildMappingConsumer(
          embeddedOP.getClassMetaData(), getClassLoaderResolver(),
          embeddedOP.getClassMetaData().getAllMemberPositions(),
          ammd.getEmbeddedMetaData());
      fieldManagerStateStack.addFirst(
          new FieldManagerState(embeddedOP, getEmbeddedAbstractMemberMetaDataProvider(mc), mc, true));
      embeddedOP.provideFields(embeddedOP.getClassMetaData().getAllMemberPositions(), this);
      fieldManagerStateStack.removeFirst();
      return;
    }

    ClassLoaderResolver clr = getClassLoaderResolver();
    if (value != null ) {
      if (ammd.isSerialized()) {
        // Serialize the field, producing a Blob
        value = getStoreManager().getSerializationManager().serialize(clr, ammd, value);
      } else {
        // Perform any conversions from the field type to the stored-type
        TypeManager typeMgr = op.getExecutionContext().getNucleusContext().getTypeManager();
        value = getConversionUtils().pojoValueToDatastoreValue(typeMgr, clr, value, ammd);
      }
    }

    int relationType = ammd.getRelationType(clr);
    if (ammd.isSerialized() || relationType == Relation.NONE) {
      // Basic field or serialised so just set the property
      if (value instanceof SCO) {
        // Use the unwrapped value so the datastore doesn't fail on unknown types
        value = ((SCO)value).getValue();
      }

      // Set the property on the entity, allowing for null rules
      checkSettingToNullValue(ammd, value);
      EntityUtils.setEntityProperty(datastoreEntity, ammd, 
          EntityUtils.getPropertyName(getStoreManager().getIdentifierFactory(), ammd), value);
      return;
    } else {
      boolean owned = MetaDataUtils.isOwnedRelation(ammd);
      if (!owned) {
        NucleusLogger.GENERAL.debug("Field=" + ammd.getFullFieldName() + " is UNOWNED");
      }

      // Relation type, so provide treatment depending on whether this object is persistent yet
      if (!repersistingForChildKeys) {
        // register a callback for later TODO Remove this
        storeRelationField(getClassMetaData(), ammd, value, operation == StoreFieldManager.Operation.INSERT,
            fieldManagerStateStack.getFirst().mappingConsumer);
      }

      if (owned) {
        // Owned - Skip out for all situations where aren't the owner (since our key has the parent key)
        if (!getStoreManager().storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
          // don't write child keys to the parent if the storage version isn't high enough
          return;
        }
        if (relationType == Relation.MANY_TO_ONE_BI) {
          // We don't store any "FK" of the parent TODO We ought to but Google don't want to
          return;
        } else if (relationType == Relation.ONE_TO_ONE_BI && ammd.getMappedBy() != null) {
          // We don't store any "FK" of the other side TODO We ought to but Google don't want to
          return;
        }
      }

      if (value == null) {
        // Nothing to extract
      } else if (Relation.isRelationSingleValued(relationType)) {
        // TODO Cater for flushing the object where necessary
        Key key = extractChildKey(value);
        if (key == null && repersistingForChildKeys) {
          // Flag that the key for this member isn't yet set
          getObjectProvider().setAssociatedValue(DatastorePersistenceHandler.MISSING_RELATION_KEY, true);
        }
        value = key;
      } else if (Relation.isRelationMultiValued(relationType)) {
        if (ammd.hasCollection()) {
          Collection coll = (Collection) value;
          int size = coll.size();

          List<Key> keys = Utils.newArrayList();
          for (Object obj : coll) {
            Key key = extractChildKey(obj);
            if (key != null) {
              keys.add(key);
            } else {
              // TODO Cater for flushing this element where necessary
            }
          }

          if (size != keys.size() && repersistingForChildKeys) {
            // Flag that some key(s) for this member aren't yet set
            getObjectProvider().setAssociatedValue(DatastorePersistenceHandler.MISSING_RELATION_KEY, true);
          }
          value = keys;
        }
        // TODO Cater for PC array, maps
      }

      if (value instanceof SCO) {
        // Use the unwrapped value so the datastore doesn't fail on unknown types
        value = ((SCO)value).getValue();
      }

      // Set the property on the entity, allowing for null rules
      checkSettingToNullValue(ammd, value);
      EntityUtils.setEntityProperty(datastoreEntity, ammd, 
          EntityUtils.getPropertyName(getStoreManager().getIdentifierFactory(), ammd), value);
      return;
    }
  }

  void storeParentField(int fieldNumber, Object value) {
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    if (mmd.getType().equals(Key.class)) {
      storeParentKeyPK((Key) value);
    } else {
      throw exceptionForUnexpectedKeyType("Parent primary key", fieldNumber);
    }
  }

  private void storePrimaryKey(int fieldNumber, Object value) {
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    if (mmd.getType().equals(Long.class)) {
      Key key = null;
      if (value != null) {
        key = KeyFactory.createKey(datastoreEntity.getKind(), (Long) value);
      }
      storeKeyPK(key);
    } else if (mmd.getType().equals(Key.class)) {
      Key key = (Key) value;
      if (key != null && key.getParent() != null && parentAlreadySet) {
        throw new NucleusFatalUserException(PARENT_ALREADY_SET);
      }
      storeKeyPK((Key) value);
    } else {
      throw exceptionForUnexpectedKeyType("Primary key", fieldNumber);
    }
  }

  void storePKIdField(int fieldNumber, Object value) {
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    if (!mmd.getType().equals(Long.class)) {
      throw new NucleusFatalUserException(
          "Field with \"" + DatastoreManager.PK_ID + "\" extension must be of type Long");
    }
    Key key = null;
    if (value != null) {
      key = KeyFactory.createKey(datastoreEntity.getKind(), (Long) value);
    }
    storeKeyPK(key);
  }

  private void storePKNameField(int fieldNumber, String value) {
    // TODO(maxr) make sure the pk is an encoded string
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    if (!mmd.getType().equals(String.class)) {
      throw new NucleusFatalUserException(
          "Field with \"" + DatastoreManager.PK_ID + "\" extension must be of type String");
    }
    Key key = null;
    if (value != null) {
      key = KeyFactory.createKey(datastoreEntity.getParent(), datastoreEntity.getKind(), value);
    }
    storeKeyPK(key);
  }

  private void storeParentStringField(String value) {
    Key key = null;
    if (value != null) {
      try {
        key = KeyFactory.stringToKey(value);
      } catch (IllegalArgumentException iae) {
        throw new NucleusFatalUserException(
            "Attempt was made to set parent to " + value
            + " but this cannot be converted into a Key.");
      }
    }
    storeParentKeyPK(key);
  }

  private void storeParentKeyPK(Key key) {
    if (key != null && parentAlreadySet) {
      throw new NucleusFatalUserException(PARENT_ALREADY_SET);
    }
    if (datastoreEntity.getParent() != null) {
      // update is ok if it's a no-op
      if (!datastoreEntity.getParent().equals(key)) {
        if (!parentAlreadySet) {
          throw new NucleusFatalUserException(
              "Attempt was made to modify the parent of an object of type "
              + getObjectProvider().getClassMetaData().getFullClassName() + " identified by "
              + "key " + datastoreEntity.getKey() + ".  Parents are immutable (changed value is " + key + ").");
        }
      }
    } else if (key != null) {
      if (operation == StoreFieldManager.Operation.UPDATE) {
        // Shouldn't even happen.
        throw new NucleusFatalUserException("You can only rely on this class to properly handle "
            + "parent pks if you instantiated the class without providing a datastore "
            + "entity to the constructor.");
      }

      if (keyAlreadySet) {
        throw new NucleusFatalUserException(PARENT_ALREADY_SET);
      }

      // If this field is labeled as a parent PK we need to recreate the Entity, passing
      // the value of this field as an arg to the Entity constructor and then moving all
      // properties on the old entity to the new entity.
      datastoreEntity = EntityUtils.recreateEntityWithParent(key, datastoreEntity);
      parentAlreadySet = true;
    } else {
      // Null parent.  Parent is defined on a per-instance basis so
      // annotating a field as a parent is not necessarily a commitment
      // to always having a parent.  Null parent is fine.
    }
  }

  private void storeStringPKField(int fieldNumber, String value) {
    Key key = null;
    if (MetaDataUtils.isEncodedPKField(getClassMetaData(), fieldNumber)) {
      if (value != null) {
        try {
          key = KeyFactory.stringToKey(value);
        } catch (IllegalArgumentException iae) {
          throw new NucleusFatalUserException(
              "Invalid primary key for " + getClassMetaData().getFullClassName() + ".  The "
              + "primary key field is an encoded String but an unencoded value has been provided. "
              + "If you want to set an unencoded value on this field you can either change its "
              + "type to be an unencoded String (remove the \"" + DatastoreManager.ENCODED_PK
              + "\" extension), change its type to be a " + Key.class.getName() + " and then set "
              + "the Key's name field, or create a separate String field for the name component "
              + "of your primary key and add the \"" + DatastoreManager.PK_NAME
              + "\" extension.");
        }
      }
    } else {
      if (value == null) {
        throw new NucleusFatalUserException(
            "Invalid primary key for " + getClassMetaData().getFullClassName() + ".  Cannot have "
            + "a null primary key field if the field is unencoded and of type String.  "
            + "Please provide a value or, if you want the datastore to generate an id on your "
            + "behalf, change the type of the field to Long.");
      }
      if (value != null) {
        if (datastoreEntity.getParent() != null) {
          key = new Entity(datastoreEntity.getKey().getKind(), value, datastoreEntity.getParent()).getKey();
        } else {
          key = new Entity(datastoreEntity.getKey().getKind(), value).getKey();
        }
      }
    }
    storeKeyPK(key);
  }

  private void storeKeyPK(Key key) {
    if (key != null && !datastoreEntity.getKind().equals(key.getKind())) {
      throw new NucleusFatalUserException(
          "Attempt was made to set the primray key of an entity with kind "
          + datastoreEntity.getKind() + " to a key with kind " + key.getKind());
    }
    if (datastoreEntity.getKey().isComplete()) {
      // this modification is only okay if it's actually a no-op
      if (!datastoreEntity.getKey().equals(key)) {
        if (!keyAlreadySet) {
          // Different key provided so the update isn't allowed.
          throw new NucleusFatalUserException(
              "Attempt was made to modify the primary key of an object of type "
              + getObjectProvider().getClassMetaData().getFullClassName() + " identified by "
              + "key " + datastoreEntity.getKey() + "  Primary keys are immutable.  "
              + "(New value: " + key);
        }
      }
    } else if (key != null) {
      Entity old = datastoreEntity;
      if (key.getParent() != null) {
        if (keyAlreadySet) {
          // can't provide a key and a parent - one or the other
          throw new NucleusFatalUserException(PARENT_ALREADY_SET);
        }
        parentAlreadySet = true;
      }
      datastoreEntity = new Entity(key);
      EntityUtils.copyProperties(old, datastoreEntity);
      keyAlreadySet = true;
    }
  }

  /**
   * @param value The persistable object from which to extract a key.
   * @return The key of the object. Returns {@code null} if the object is being deleted 
   *    or the object does not yet have a key.
   */
  private Key extractChildKey(Object value) {
    if (value == null) {
      return null;
    }

    ExecutionContext ec = getExecutionContext();
    ObjectProvider valueOP = ec.findObjectProvider(value);
    if (valueOP == null) {
      // TODO If this is detached it should still be possible to get the identity which is all we need here!
      // that's fine, it just means the object hasn't been saved or that it is detached
      return null;
    }

    if (valueOP.getLifecycleState().isDeleted()) {
      return null;
    }

    // TODO Cater for datastore identity
    Object primaryKey = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(valueOP.getInternalObjectId());
    /*      if (primaryKey == null && operation == Operation.UPDATE && op.getInternalObjectId() != null) {
        // *** This was added to attempt to get child objects persistent so we note their ids for persist of parent ***
        NucleusLogger.GENERAL.info(">> this.op=" + op + " childobject.op=" + valueOP + " does not provide its id, so flushing it (since part of UPDATE)");
        valueOP.flush();
        primaryKey = storeManager.getApiAdapter().getTargetKeyForSingleFieldIdentity(
            valueOP.getInternalObjectId());
      }*/
    if (primaryKey == null) {
      // this is ok, it just means the object has not yet been persisted
      return null;
    }
    Key key = EntityUtils.getPrimaryKeyAsKey(op.getExecutionContext().getApiAdapter(), valueOP);
    if (key == null) {
      throw new NullPointerException("Could not extract a key from " + value);
    }

    // We only support owned relationships so this key should be a child of the entity we are persisting. WRONG!
    if (key.getParent() == null) {
      // TODO This is caused by persisting from non-owner side of a relation when owned and GAE having a very basic
      // restriction of not allowing persistence of such things due to its silly parent-key rationale.
      throw new EntityUtils.ChildWithoutParentException(datastoreEntity.getKey(), key);
    } else if (!key.getParent().equals(datastoreEntity.getKey())) {
      throw new EntityUtils.ChildWithWrongParentException(datastoreEntity.getKey(), key);
    }
    return key;
  }

  private void checkSettingToNullValue(AbstractMemberMetaData ammd, Object value) {
    if (value == null) {
      if (ammd.getNullValue() == NullValue.EXCEPTION) {
        // JDO spec 18.15, throw XXXUserException when trying to store null and have handler set to EXCEPTION
        throw new NucleusUserException("Field/Property " + ammd.getFullFieldName() +
          " is null, but is mandatory as it's described in the jdo metadata");
      }

      ColumnMetaData[] colmds = ammd.getColumnMetaData();
      if (colmds != null && colmds.length > 0) {
        if (colmds[0].getAllowsNull() == Boolean.FALSE) {
          // Column specifically marked as not-nullable
          throw new NucleusDataStoreException("Field/Property " + ammd.getFullFieldName() +
            " is null, but the column is specified as not-nullable");
        }
      }
    }
  }

  void setRepersistingForChildKeys(boolean repersistingForChildKeys) {
    this.repersistingForChildKeys = repersistingForChildKeys;
  }

  /**
   * Applies all the relation events that have been built up.
   * @return {@code true} if the relations changed in a way that
   * requires an update to the relation owner, {@code false} otherwise.
   */
  boolean storeRelations(KeyRegistry keyRegistry) {
    NucleusLogger.GENERAL.info(">> StoreFM.storeRelations ");
    if (storeRelationEvents.isEmpty()) {
      return false;
    }

    if (datastoreEntity.getKey() != null) {
      // TODO Enable this when we have the key set upon creation of Entity.
      /*      if (relationType == Relation.ONE_TO_MANY_UNI || relationType == Relation.ONE_TO_MANY_BI) {
                // Owner of 1-N, so other side has this as parent
                Object childValue = op.provideField(ammd.getAbsoluteFieldNumber());
                if (childValue != null) {
                  NucleusLogger.GENERAL.info(">> StoreFM field=" + ammd.getFullFieldName() + " value=" + StringUtils.toJVMIDString(childValue));
                  if (childValue instanceof Object[]) {
                    childValue  = Arrays.asList((Object[]) childValue);
                  }
                  if (childValue instanceof Iterable) {
                    for (Object element : (Iterable)childValue) {
                      // TODO Check polymorphism
                      keyReg.registerParentKeyForOwnedObject(element, key);
                    }
                  }
                }
              } else if (relationType == Relation.ONE_TO_ONE_UNI ||
                  (relationType == Relation.ONE_TO_ONE_BI && ammd.getMappedBy() == null)) {
                // Owner of 1-1 relation, so other side is child
                Object childValue = op.provideField(ammd.getAbsoluteFieldNumber());
                if (childValue != null) {
                  // TODO Check polymorphism
                  keyReg.registerParentKeyForOwnedObject(childValue, key);
                }
              }*/
      // Register parent key - TODO why do this for all and not just for the relations being updated?!!!
      ObjectProvider op = getObjectProvider();
      DatastoreTable dt = getDatastoreTable();
      Key key = datastoreEntity.getKey();
      AbstractClassMetaData acmd = op.getClassMetaData();
      for (AbstractMemberMetaData dependent : dt.getSameEntityGroupMemberMetaData()) {
        // Make sure we only provide the field for the correct part of any inheritance tree
        if (dependent.getAbstractClassMetaData().isSameOrAncestorOf(acmd)) {
          Object childValue = op.provideField(dependent.getAbsoluteFieldNumber());
          if (childValue != null) {
            if (childValue instanceof Object[]) {
              childValue  = Arrays.asList((Object[]) childValue);
            }
            if (childValue instanceof Iterable) {
              // TODO(maxr): Make sure we're not pulling back unnecessary data when we iterate over the values.
              String expectedType = getExpectedChildType(dependent);
              for (Object element : (Iterable) childValue) {
                addToParentKeyMap(keyRegistry, element, key, op.getExecutionContext(), expectedType, true);
              }
            } else {
              boolean checkForPolymorphism = !dt.isParentKeyProvider(dependent);
              addToParentKeyMap(keyRegistry, childValue, key, op.getExecutionContext(), 
                  getExpectedChildType(dependent), checkForPolymorphism);
            }
          }
        }
      }
    }

    for (StoreRelationEvent event : storeRelationEvents) {
      event.apply();
    }
    storeRelationEvents.clear();

    try {
      return keyRegistry.parentNeedsUpdate(datastoreEntity.getKey());
    } finally {
      keyRegistry.clearModifiedParent(datastoreEntity.getKey());
    }
  }

  // Nonsense about registering parent key
  private String getExpectedChildType(AbstractMemberMetaData dependent) {
    if (dependent.getCollection() != null) {
      CollectionMetaData cmd = dependent.getCollection();
      return cmd.getElementType();
    } else if (dependent.getArray() != null) {
      ArrayMetaData amd = dependent.getArray();
      return amd.getElementType();
    }
    return dependent.getTypeName();
  }

  // Nonsense about registering parent key
  private void addToParentKeyMap(KeyRegistry keyRegistry, Object childValue, Key key, ExecutionContext ec,
      String expectedType, boolean checkForPolymorphism) {
    if (checkForPolymorphism && childValue != null && !childValue.getClass().getName().equals(expectedType)) {
      AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(childValue.getClass(),
          ec.getClassLoaderResolver());
      if (!MetaDataUtils.isNewOrSuperclassTableInheritanceStrategy(acmd)) {
        // TODO cache the result of this evaluation to improve performance
        throw new UnsupportedOperationException(
            "Received a child of type " + childValue.getClass().getName() + " for a field of type " +
            expectedType + ". Unfortunately polymorphism in relationships is only supported for the " +
            "superclass-table inheritance mapping strategy.");
      }
    }

    keyRegistry.registerParentKeyForOwnedObject(childValue, key);
  }

  private void storeRelationField(final AbstractClassMetaData acmd, final AbstractMemberMetaData ammd, final Object value,
      final boolean isInsert, final InsertMappingConsumer consumer) {
    NucleusLogger.GENERAL.info(">> StoreFM.storeRelationField " + ammd.getFullFieldName() + " value=" + StringUtils.toJVMIDString(value));

    StoreRelationEvent event = new StoreRelationEvent() {
      public void apply() {
        DatastoreTable table = getDatastoreTable();

        ObjectProvider op = getObjectProvider();
        int fieldNumber = ammd.getAbsoluteFieldNumber();
        // Based on ParameterSetter
        try {
          JavaTypeMapping mapping = table.getMemberMappingInDatastoreClass(ammd);
          if (mapping instanceof EmbeddedPCMapping ||
              mapping instanceof SerialisedPCMapping ||
              mapping instanceof SerialisedReferenceMapping ||
              mapping instanceof PersistableMapping ||
              mapping instanceof InterfaceMapping) {
            setObjectViaMapping(mapping, table, value, op, fieldNumber);
            op.wrapSCOField(fieldNumber, value, false, true, true);
          } else {
            // TODO This is total crap. We are storing a particular field and this code calls postInsert on ALL
            // relation fields. Why ? Should just call postInsert on this field
            if (isInsert) {
              for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
                callback.postInsert(op);
              }
            } else {
              for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
                callback.postUpdate(op);
              }
            }
          }
        } catch (NotYetFlushedException e) {
          if (acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getNullValue() == NullValue.EXCEPTION) {
            throw e;
          }
          op.updateFieldAfterInsert(e.getPersistable(), fieldNumber);
        }
      }

      private void setObjectViaMapping(JavaTypeMapping mapping, DatastoreTable table, Object value,
          ObjectProvider op, int fieldNumber) {
        boolean fieldIsParentKeyProvider = table.isParentKeyProvider(ammd);
        if (!fieldIsParentKeyProvider) {
          EntityUtils.checkParentage(value, op);
        }
        Entity entity = datastoreEntity;
        mapping.setObject(getExecutionContext(), entity,
            fieldIsParentKeyProvider ? IS_PARENT_VALUE_ARR : IS_FK_VALUE_ARR,
                value, op, fieldNumber);

        // If the field we're setting is the one side of an owned many-to-one,
        // its pk needs to be the parent of the key of the entity we're
        // currently populating.  We look for a magic property that tells
        // us if this change needs to be made.  See
        // DatastoreFKMapping.setObject for all the gory details.
        Object parentKeyObj = entity.getProperty(PARENT_KEY_PROPERTY);
        if (parentKeyObj != null) {
          AbstractClassMetaData parentCmd = op.getExecutionContext().getMetaDataManager().getMetaDataForClass(
              ammd.getType(), getClassLoaderResolver());
          Key parentKey = EntityUtils.getPkAsKey(parentKeyObj, parentCmd, getExecutionContext());
          entity.removeProperty(PARENT_KEY_PROPERTY);
          datastoreEntity = EntityUtils.recreateEntityWithParent(parentKey, datastoreEntity);
        }
      }
    };

    // If the related object can't exist without the parent it should be part
    // of the parent's entity group.  In order to be part of the parent's
    // entity group we need the parent's key.  In order to get the parent's key
    // we must save the parent before we save the child.  In order to avoid
    // saving the child until after we've saved the parent we register an event
    // that we will apply later.
    storeRelationEvents.add(event);
  }

  /** Supports a mechanism for delaying the storage of a relation. */
  private interface StoreRelationEvent {
    void apply();
  }

  /**
   * Method to make sure that the Entity has its parentKey assigned.
   * Returns the assigned parent PK (when we have a "gae.parent-pk" field/property in this class).
   * @return The parent key if the pojo class has a parent property. Note that a return value of {@code null} 
   *   does not mean that an entity group was not established, it just means the pojo doesn't have a distinct
   *   field for the parent.
   */
  Object establishEntityGroup() {
    Key parentKey = datastoreEntity.getParent();
    if (parentKey == null) {
      parentKey = EntityUtils.getParentKey(datastoreEntity, op);
      if (parentKey != null) {
        datastoreEntity = EntityUtils.recreateEntityWithParent(parentKey, datastoreEntity);
      }
    }

    AbstractMemberMetaData parentPkMmd = ((DatastoreManager)getStoreManager()).getMetaDataForParentPK(getClassMetaData());
    if (parentKey != null && parentPkMmd != null) {
      return parentPkMmd.getType().equals(Key.class) ? parentKey : KeyFactory.keyToString(parentKey);
    }
    return null;
  }
}