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
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.types.sco.SCO;

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

    public enum Operation { INSERT, UPDATE, DELETE };

    protected final Operation operation;

    /**
     * @param op ObjectProvider of the object being stored
     * @param storeManager StoreManager for this object
     * @param datastoreEntity The Entity to update with the field values
     * @param fieldNumbers The field numbers being updated in the Entity
     * @param operation Operation being performed
     */
    public StoreFieldManager(ObjectProvider op, DatastoreManager storeManager, Entity datastoreEntity, int[] fieldNumbers,
            Operation operation) {
        super(op, storeManager, datastoreEntity, fieldNumbers);
        this.operation = operation;
    }

    /**
     * @param op ObjectProvider of the object being stored
     * @param storeManager StoreManager for this object
     * @param datastoreEntity The Entity to update with the field values
     * @param operation Operation being performed
     */
    public StoreFieldManager(ObjectProvider op, DatastoreManager storeManager, Entity datastoreEntity, Operation operation) {
        super(op, storeManager, datastoreEntity);
        this.operation = operation;
    }

    /**
     * @param op ObjectProvider of the object being stored
     * @param kind Kind of entity
     * @param storeManager StoreManager for this object
     * @param operation Operation being performed
     */
    public StoreFieldManager(ObjectProvider op, String kind, DatastoreManager storeManager, Operation operation) {
        super(op, kind, storeManager);
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
      } else if (isParentPK(fieldNumber)) {
        parentMemberMetaData = getMetaData(fieldNumber);
        storeParentStringField(value);
      } else if (isPKNameField(fieldNumber)) {
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
      } else if (isParentPK(fieldNumber)) {
        parentMemberMetaData = getMetaData(fieldNumber);
        storeParentField(fieldNumber, value);
      } else if (isPKIdField(fieldNumber)) {
        storePKIdField(fieldNumber, value);
      } else {
        ClassLoaderResolver clr = getClassLoaderResolver();
        AbstractMemberMetaData ammd = getMetaData(fieldNumber);
        if (repersistingForChildKeys && !PARENT_RELATION_TYPES.contains(ammd.getRelationType(clr))) {
          // nothing for us to store
          return;
        }

        storeFieldInEntity(fieldNumber, value);

        if (!(value instanceof SCO)) {
        // TODO Wrap SCO fields. Currently this causes some tests to fail due to the fragility of the design of this plugin
//        getObjectProvider().wrapSCOField(fieldNumber, value, false, false, true);
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
        storeEmbeddedField(ammd, fieldNumber, value);
      }
      else if ((operation == Operation.INSERT && isInsertable(ammd)) ||
               (operation == Operation.UPDATE && isUpdatable(ammd))) {
        if (ammd.getRelationType(clr) != Relation.NONE && !ammd.isSerialized()) {
          if (!repersistingForChildKeys) {
            // register a callback for later
            relationFieldManager.storeRelationField(
                getClassMetaData(), ammd, value, createdWithoutEntity, getInsertMappingConsumer());
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

        // unwrap SCO values so that the datastore api doesn't honk on unknown types
        if (value instanceof SCO) {
          value = unwrapSCOField(fieldNumber, value);
        }

        // Set the property on the entity, allowing for null rules
        String propName = EntityUtils.getPropertyName(storeManager.getIdentifierFactory(), ammd);
        checkNullValue(ammd, propName, value);
        EntityUtils.setEntityProperty(datastoreEntity, ammd, propName, value);
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

    /**
     * We can't trust the fieldNumber on the ammd provided because some embedded
     * fields don't have this set.  That's why we pass it in as a separate param.
     */
    private void storeEmbeddedField(AbstractMemberMetaData ammd, int fieldNumber, Object value) {
      ObjectProvider esm = getEmbeddedObjectProvider(ammd, fieldNumber, value);
      // We need to build a mapping consumer for tObhe embedded class so that we
      // get correct fieldIndex --> metadata mappings for the class in the proper
      // embedded context
      // TODO(maxr) Consider caching this
      InsertMappingConsumer mc = buildMappingConsumer(
          esm.getClassMetaData(), getClassLoaderResolver(),
          esm.getClassMetaData().getAllMemberPositions(),
          ammd.getEmbeddedMetaData());
      AbstractMemberMetaDataProvider ammdProvider = getEmbeddedAbstractMemberMetaDataProvider(mc);
      fieldManagerStateStack.addFirst(new FieldManagerState(esm, ammdProvider, mc, true));
      AbstractClassMetaData acmd = esm.getClassMetaData();
      esm.provideFields(acmd.getAllMemberPositions(), this);
      fieldManagerStateStack.removeFirst();
    }

    private Object extractRelationKeys(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Collection) {
        Collection coll = (Collection) value;
        // TODO What if any persistable objects here have not yet been flushed?
        int size = coll.size();
        List<Key> keys = extractRelationKeys((Collection) value);
        // if we have fewer keys than objects then there is at least one child
        // object that still needs to be inserted.  communicate
        // this upstream.
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

    private List<Key> extractRelationKeys(Collection<?> values) {
      List<Key> keys = Utils.newArrayList();
      for (Object obj : values) {
        Key key = extractChildKey(obj);
        if (key != null) {
          keys.add(key);
        }
      }
      return keys;
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
  // TODO Was this in 1.1   if (valueOP.isDeleted((PersistenceCapable) valueOP.getObject()) ) {
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

      // We only support owned relationships so this key should be a child
      // of the entity we are persisting.
      if (key.getParent() == null) {
        throw new DatastoreRelationFieldManager.ChildWithoutParentException(datastoreEntity.getKey(), key);
      } else if (!key.getParent().equals(datastoreEntity.getKey())) {
        throw new DatastoreRelationFieldManager.ChildWithWrongParentException(datastoreEntity.getKey(), key);
      }
      return key;
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
      pkIdPos = fieldNumber;
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
      if (DatastoreManager.isEncodedPKField(getClassMetaData(), fieldNumber)) {
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
          copyProperties(old, datastoreEntity);
          keyAlreadySet = true;
        }
      }

    private void checkNullValue(AbstractMemberMetaData ammd, String propName, Object value) {
      if (value == null) {
        // This test goes against the member meta data, since the member meta data
        // says how to behave in case of unwanted null values (but JDO only).
        // All other cases of unwanted null values will be handled
        // by the DatastoreTable as the proxy for "tables" in the datastore.
        if (ammd.getNullValue() == NullValue.EXCEPTION) {
        // always throw a JDOUserException as required by the jdo spec
          throw new NucleusUserException(
              "Field "
              + ammd.getFullFieldName()
              + " is null, but is mandatory as it's described in the jdo metadata");
        }
      }
    }

    private ColumnMetaData getColumnMetaData(AbstractMemberMetaData ammd) {
      if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length > 0) {
        return ammd.getColumnMetaData()[0];
      }
      // A OneToMany has its column metadata stored inside
      // ElementMetaData so look there as well
      if (ammd.getElementMetaData() != null &&
          ammd.getElementMetaData().getColumnMetaData() != null &&
          ammd.getElementMetaData().getColumnMetaData().length > 0) {
        return ammd.getElementMetaData().getColumnMetaData()[0];
      }
      return null;
    }

    private boolean isInsertable(AbstractMemberMetaData ammd) {
      ColumnMetaData cmd = getColumnMetaData(ammd);
      return cmd == null || cmd.getInsertable();
    }

    private boolean isUpdatable(AbstractMemberMetaData ammd) {
      ColumnMetaData cmd = getColumnMetaData(ammd);
      return cmd == null || cmd.getUpdateable();
    }

    Object unwrapSCOField(int fieldNumber, Object value) {
      return getObjectProvider().unwrapSCOField(fieldNumber, value, false);
    }
}