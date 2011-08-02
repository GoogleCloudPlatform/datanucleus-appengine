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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.exceptions.NotYetFlushedException;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.IndexMapping;
import org.datanucleus.store.mapped.mapping.InterfaceMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.PersistableMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;
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

  private static final int[] NOT_USED = {0};
  public static final int IS_PARENT_VALUE = -1;
  private static final int[] IS_PARENT_VALUE_ARR = {IS_PARENT_VALUE};
  public static final String PARENT_KEY_PROPERTY = "____PARENT_KEY____";

  public static final int IS_FK_VALUE = -2;
  private static final int[] IS_FK_VALUE_ARR = {IS_FK_VALUE};

  public enum Operation { INSERT, UPDATE, DELETE };

  protected final Operation operation;

  protected boolean repersistingForChildKeys = false;

  protected boolean parentAlreadySet = false;

  protected boolean keyAlreadySet = false;

  // Events that know how to store relations.
  private final List<StoreRelationEvent> storeRelationEvents = Utils.newArrayList();

  /**
   * @param op ObjectProvider of the object being stored
   * @param storeManager StoreManager for this object
   * @param datastoreEntity The Entity to update with the field values
   * @param fieldNumbers The field numbers being updated in the Entity
   * @param operation Operation being performed
   */
  public StoreFieldManager(ObjectProvider op, DatastoreManager storeManager, Entity datastoreEntity, int[] fieldNumbers,
      Operation operation) {
    super(op, false, storeManager, datastoreEntity, fieldNumbers);
    this.operation = operation;
  }

  /**
   * @param op ObjectProvider of the object being stored
   * @param storeManager StoreManager for this object
   * @param datastoreEntity The Entity to update with the field values
   * @param operation Operation being performed
   */
  public StoreFieldManager(ObjectProvider op, DatastoreManager storeManager, Entity datastoreEntity, Operation operation) {
    super(op, false, storeManager, datastoreEntity, new int[0]);
    this.operation = operation;
  }

  /**
   * @param op ObjectProvider of the object being stored
   * @param kind Kind of entity
   * @param storeManager StoreManager for this object
   * @param operation Operation being performed
   */
  public StoreFieldManager(ObjectProvider op, String kind, DatastoreManager storeManager, Operation operation) {
    super(op, true, storeManager, new Entity(kind), new int[0]);
    this.operation = operation;
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
      parentMemberMetaData = getMetaData(fieldNumber);
      storeParentStringField(value);
    } else if (MetaDataUtils.isPKNameField(getClassMetaData(), fieldNumber)) {
      storePKNameField(fieldNumber, value);
    } else {
      // could be a JPA "lob" field, in which case we want to store it as Text.
      // DataNucleus sets a cmd with a jdbc type of CLOB if this is the case.
      Object valueToStore = value;
      AbstractMemberMetaData ammd = getMetaData(fieldNumber);
      if (ammd.getColumnMetaData() != null &&
          ammd.getColumnMetaData().length == 1 &&
          "CLOB".equals(ammd.getColumnMetaData()[0].getJdbcType())) {
        valueToStore = new Text(value);
      }

      storeFieldInEntity(fieldNumber, valueToStore);
    }
  }

  public void storeObjectField(int fieldNumber, Object value) {
    if (isPK(fieldNumber)) {
      storePrimaryKey(fieldNumber, value);
    } else if (MetaDataUtils.isParentPKField(getClassMetaData(), fieldNumber)) {
      parentMemberMetaData = getMetaData(fieldNumber);
      storeParentField(fieldNumber, value);
    } else if (MetaDataUtils.isPKIdField(getClassMetaData(), fieldNumber)) {
      storePKIdField(fieldNumber, value);
    } else {
      ClassLoaderResolver clr = getClassLoaderResolver();
      AbstractMemberMetaData ammd = getMetaData(fieldNumber);

      // TODO Work out what this next block does!
      if (repersistingForChildKeys && !PARENT_RELATION_TYPES.contains(ammd.getRelationType(clr))) {
        // nothing for us to store
        return;
      }

      storeFieldInEntity(fieldNumber, value);

      if (!(value instanceof SCO)) {
        // TODO Wrap SCO fields. Currently this causes some tests to fail due to the fragility of the design of this plugin
        //          getObjectProvider().wrapSCOField(fieldNumber, value, false, false, true);
      }
    }
  }

  /**
   * Method to store the provided value in the Entity for the specified field.
   * @param fieldNumber The absolute field number
   * @param value Value to store (or rather to manipulate into a suitable form for the datastore).
   * @return Whether it stored the value (doesn't mean an error, just that wasn't needed to be stored)
   */
  private boolean storeFieldInEntity(int fieldNumber, Object value) {
    ClassLoaderResolver clr = getClassLoaderResolver();
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    if (value != null ) {
      if (ammd.isSerialized()) {
        // If the field is serialized we don't need to apply any conversions before setting it on the 
        // entity since the serialization is guaranteed to produce a Blob.
        value = storeManager.getSerializationManager().serialize(clr, ammd, value);
      } else {
        // Perform any conversions from the field type to the stored-type
        value = getConversionUtils().pojoValueToDatastoreValue(clr, value, ammd);
      }
    }

    if (ammd.getEmbeddedMetaData() != null) {
      // Embedded field handling
      ObjectProvider esm = getEmbeddedObjectProvider(ammd, fieldNumber, value);
      // We need to build a mapping consumer for the embedded class so that we get correct
      // fieldIndex --> metadata mappings for the class in the proper embedded context
      // TODO(maxr) Consider caching this
      InsertMappingConsumer mc = buildMappingConsumer(
          esm.getClassMetaData(), getClassLoaderResolver(),
          esm.getClassMetaData().getAllMemberPositions(),
          ammd.getEmbeddedMetaData());
      // TODO Create own FieldManager instead of reusing this one
      AbstractMemberMetaDataProvider ammdProvider = getEmbeddedAbstractMemberMetaDataProvider(mc);
      fieldManagerStateStack.addFirst(new FieldManagerState(esm, ammdProvider, mc, true));
      AbstractClassMetaData acmd = esm.getClassMetaData();
      esm.provideFields(acmd.getAllMemberPositions(), this);
      fieldManagerStateStack.removeFirst();
    }
    else if ((operation == Operation.INSERT && ammd.isInsertable()) ||
        (operation == Operation.UPDATE && ammd.isUpdateable())) {
      if (ammd.getRelationType(clr) != Relation.NONE && !ammd.isSerialized()) {

        if (!repersistingForChildKeys) {
          // register a callback for later
          storeRelationField(getClassMetaData(), ammd, value, createdWithoutEntity, getInsertMappingConsumer());
        }

        DatastoreTable table = getDatastoreTable();
        if (table != null && table.isParentKeyProvider(ammd)) {
          // a parent key provider is either a many-to-one or the child side of a one-to-one.  
          // Either way we don't want the entity to have a property corresponding to this field 
          // because this information is available in the key itself
          return false;
        }

        if (!getStoreManager().storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
          // don't write child keys to the parent if the storage version isn't high enough
          return false;
        }

        // We still want to write the entity property with the keys
        value = extractRelationKeys(value);
      }

      if (value instanceof SCO) {
        // Use the unwrapped value so the datastore doesn't fail on unknown types
        value = ((SCO)value).getValue();
      }

      // Set the property on the entity, allowing for null rules
      checkNullValue(ammd, value);
      EntityUtils.setEntityProperty(datastoreEntity, ammd, 
          EntityUtils.getPropertyName(storeManager.getIdentifierFactory(), ammd), value);
      return true;
    }
    return false;
  }

  void storeParentField(int fieldNumber, Object value) {
    if (fieldIsOfTypeKey(fieldNumber)) {
      storeParentKeyPK((Key) value);
    } else {
      throw exceptionForUnexpectedKeyType("Parent primary key", fieldNumber);
    }
  }

  private void storePrimaryKey(int fieldNumber, Object value) {
    if (fieldIsOfTypeLong(fieldNumber)) {
      Key key = null;
      if (value != null) {
        key = KeyFactory.createKey(datastoreEntity.getKind(), (Long) value);
      }
      storeKeyPK(key);
    } else if (fieldIsOfTypeKey(fieldNumber)) {
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
    if (!fieldIsOfTypeLong(fieldNumber)) {
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
    if (!fieldIsOfTypeString(fieldNumber)) {
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
      if (!createdWithoutEntity) {
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
      recreateEntityWithParent(key);
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

  boolean storeRelations() {
    return storeRelations(KeyRegistry.getKeyRegistry(getExecutionContext()));
  }

  private Object extractRelationKeys(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Collection) {
      Collection coll = (Collection) value;
      // TODO What if any persistable objects here have not yet been flushed?
      int size = coll.size();

      List<Key> keys = Utils.newArrayList();
      for (Object obj : (Collection) value) {
        Key key = extractChildKey(obj);
        if (key != null) {
          keys.add(key);
        }
      }

      // if we have fewer keys than objects then there is at least one child
      // object that still needs to be inserted.  communicate this upstream.
      getObjectProvider().setAssociatedValue(
          DatastorePersistenceHandler.MISSING_RELATION_KEY,
          repersistingForChildKeys && size != keys.size() ? true : null);
      return keys;
    }
    Key key = extractChildKey(value);
    // if we didn't come up with a key that there is a child object that
    // still needs to be inserted.  communicate this upstream.
    getObjectProvider().setAssociatedValue(
        DatastorePersistenceHandler.MISSING_RELATION_KEY,
        repersistingForChildKeys && key == null ? true : null);
    return key;
  }

  /**
   * @param value The object from which to extract a key.
   * @return The key of the object.  Returns {@code null} if the object is
   * being deleted or the object does not yet have a key.
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

    Object primaryKey = storeManager.getApiAdapter().getTargetKeyForSingleFieldIdentity(
        valueOP.getInternalObjectId());
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
    Key key = EntityUtils.getPrimaryKeyAsKey(storeManager.getApiAdapter(), valueOP);
    if (key == null) {
      throw new NullPointerException("Could not extract a key from " + value);
    }

    // We only support owned relationships so this key should be a child of the entity we are persisting.
    if (key.getParent() == null) {
      throw new EntityUtils.ChildWithoutParentException(datastoreEntity.getKey(), key);
    } else if (!key.getParent().equals(datastoreEntity.getKey())) {
      throw new EntityUtils.ChildWithWrongParentException(datastoreEntity.getKey(), key);
    }
    return key;
  }

  private void checkNullValue(AbstractMemberMetaData ammd, Object value) {
    if (value == null) {
      // This test goes against the member meta data, since the member meta data
      // says how to behave in case of unwanted null values (but JDO only).
      // All other cases of unwanted null values will be handled
      // by the DatastoreTable as the proxy for "tables" in the datastore.
      if (ammd.getNullValue() == NullValue.EXCEPTION) {
        // always throw a XXXUserException as required by the jdo spec
        throw new NucleusUserException("Field " + ammd.getFullFieldName() +
        " is null, but is mandatory as it's described in the jdo metadata");
      }
    }
  }

  void setRepersistingForChildKeys(boolean repersistingForChildKeys) {
    this.repersistingForChildKeys = repersistingForChildKeys;
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
    EntityUtils.copyProperties(old, datastoreEntity);
  }

  InsertMappingConsumer getInsertMappingConsumer() {
    return fieldManagerStateStack.getFirst().mappingConsumer;
  }

  private Key getKeyFromParentPojo(Object mergeEntity) {
    ObjectProvider mergeOP = getExecutionContext().findObjectProvider(mergeEntity);
    if (mergeOP == null) {
      return null;
    }
    return EntityUtils.getPrimaryKeyAsKey(getExecutionContext().getApiAdapter(), mergeOP);
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

  private Key getKeyForObject(Object pc) {
    ApiAdapter adapter = getStoreManager().getApiAdapter();
    Object internalPk = adapter.getTargetKeyForSingleFieldIdentity(adapter.getIdForObject(pc));
    AbstractClassMetaData acmd =
        getExecutionContext().getMetaDataManager().getMetaDataForClass(pc.getClass(), getClassLoaderResolver());
    return EntityUtils.getPkAsKey(internalPk, acmd, getExecutionContext());
  }

  /**
   * Indexed 1-to-many relationsihps are expressed using a {@link List} and are ordered by a 
   * column in the child table that stores the position of the child in the parent's list.
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

  protected boolean fieldIsOfTypeLong(int fieldNumber) {
    // Long is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(Long.class);
  }

  protected boolean fieldIsOfTypeKey(int fieldNumber) {
    // Key is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(Key.class);
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
      keyRegistry.registerKey(getObjectProvider(), this);
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

  void storeRelationField(final AbstractClassMetaData acmd,
      final AbstractMemberMetaData ammd, final Object value,
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
          recreateEntityWithParent(parentKey);
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

}