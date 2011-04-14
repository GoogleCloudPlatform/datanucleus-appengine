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

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NoPersistenceInformationException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.Relation;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.store.appengine.jpa.DatastoreJPACallbackHandler;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.mapping.EmbeddedMapping;
import org.datanucleus.store.mapped.mapping.IndexMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.spi.JDOImplHelper;
import javax.jdo.spi.PersistenceCapable;

// TODO(maxr): Make this a base class and extract 2 subclasses - one for
// reads and one for writes.
/**
 * FieldManager for converting app engine datastore entities into POJOs and
 * vice-versa.
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
public class DatastoreFieldManager implements FieldManager {

  public enum Operation { INSERT, UPDATE, DELETE, READ };

  private static final String ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT =
      "Datastore entity with kind %s and key %s has a null property named %s.  This property is "
          + "mapped to %s, which cannot accept null values.";

  private static final int[] NOT_USED = {0};

  private static final TypeConversionUtils TYPE_CONVERSION_UTILS = new TypeConversionUtils();

  // Stack used to maintain the current field state manager to use.  We push on
  // to this stack as we encounter embedded classes and then pop when we're
  // done.
  private final LinkedList<FieldManagerState> fieldManagerStateStack =
      new LinkedList<FieldManagerState>();

  // true if we instantiated the entity ourselves.
  private final boolean createdWithoutEntity;

  private final DatastoreManager storeManager;

  private final DatastoreRelationFieldManager relationFieldManager;

  private final SerializationManager serializationManager;

  private final Operation operation;

  // Not final because we will reallocate if we hit a parent pk field
  // and the key of the current value does not have a parent, or if the pk
  // gets set.
  private Entity datastoreEntity;

  // We'll assign this if we have a parent member and we store a value
  // into it.
  private AbstractMemberMetaData parentMemberMetaData;

  private boolean parentAlreadySet = false;
  private boolean keyAlreadySet = false;
  private Integer pkIdPos = null;
  private boolean repersistingForChildKeys = false;

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

  private DatastoreFieldManager(StateManager sm, boolean createdWithoutEntity,
      DatastoreManager storeManager, Entity datastoreEntity, int[] fieldNumbers, Operation operation) {
    // We start with an ammdProvider that just gets member meta data from the class meta data.
    AbstractMemberMetaDataProvider ammdProvider = new AbstractMemberMetaDataProvider() {
      public AbstractMemberMetaData get(int fieldNumber) {
        return getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
      }
    };
    this.createdWithoutEntity = createdWithoutEntity;
    this.storeManager = storeManager;
    this.datastoreEntity = datastoreEntity;
    InsertMappingConsumer mappingConsumer = buildMappingConsumer(sm.getClassMetaData(), sm.getObjectManager().getClassLoaderResolver(), fieldNumbers);
    this.fieldManagerStateStack.addFirst(new FieldManagerState(sm, ammdProvider, mappingConsumer, false));
    this.relationFieldManager = new DatastoreRelationFieldManager(this);
    this.serializationManager = new SerializationManager();
    this.operation = operation;

    // Sanity check
    String expectedKind = EntityUtils.determineKind(getClassMetaData(), getIdentifierFactory());
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
  DatastoreFieldManager(StateManager stateManager, DatastoreManager storeManager,
      Entity datastoreEntity, int[] fieldNumbers, Operation operation) {
    this(stateManager, false, storeManager, datastoreEntity, fieldNumbers, operation);
  }

  public DatastoreFieldManager(StateManager stateManager, DatastoreManager storeManager,
      Entity datastoreEntity, Operation operation) {
    this(stateManager, false, storeManager, datastoreEntity, new int[0], operation);
  }

  DatastoreFieldManager(StateManager stateManager, String kind,
      DatastoreManager storeManager, Operation operation) {
    this(stateManager, true, storeManager, new Entity(kind), new int[0], operation);
  }

  public String fetchStringField(int fieldNumber) {
    if (isPK(fieldNumber)) {
      return fetchStringPKField(fieldNumber);
    } else if (isParentPK(fieldNumber)) {
      return fetchParentStringPKField(fieldNumber);
    } else if (isPKNameField(fieldNumber)) {
      if (!fieldIsOfTypeString(fieldNumber)) {
        throw new FatalNucleusUserException(
            "Field with \"" + DatastoreManager.PK_NAME + "\" extension must be of type String");
      }
      return fetchPKNameField();
    }
    Object fieldVal = fetchObjectField(fieldNumber);
    if (fieldVal instanceof Text) {
      // must be a lob field
      fieldVal = ((Text) fieldVal).getValue();
    }
    return (String) fieldVal;
  }

  private String fetchPKNameField() {
    Key key = datastoreEntity.getKey();
    if (key.getName() == null) {
      throw new FatalNucleusUserException(
          "Attempting to fetch field with \"" + DatastoreManager.PK_NAME + "\" extension but the "
          + "entity is identified by an id, not a name.");
    }
    return datastoreEntity.getKey().getName();
  }

  private long fetchPKIdField() {
    Key key = datastoreEntity.getKey();
    if (key.getName() != null) {
      throw new FatalNucleusUserException(
          "Attempting to fetch field with \"" + DatastoreManager.PK_ID + "\" extension but the "
          + "entity is identified by a name, not an id.");
    }
    return datastoreEntity.getKey().getId();
  }

  private String fetchParentStringPKField(int fieldNumber) {
    Key parentKey = datastoreEntity.getKey().getParent();
    if (parentKey == null) {
      return null;
    }
    return KeyFactory.keyToString(parentKey);
  }

  private String fetchStringPKField(int fieldNumber) {
    if (DatastoreManager.isEncodedPKField(getClassMetaData(), fieldNumber)) {
      // If this is an encoded pk field, transform the Key into its String
      // representation.
      return KeyFactory.keyToString(datastoreEntity.getKey());
    } else {
      if (datastoreEntity.getKey().isComplete() && datastoreEntity.getKey().getName() == null) {
        // This is trouble, probably an incorrect mapping.
        throw new FatalNucleusUserException(
            "The primary key for " + getClassMetaData().getFullClassName() + " is an unencoded "
            + "string but the key of the corresponding entity in the datastore does not have a "
            + "name.  You may want to either change the primary key to be an encoded string "
            + "(add the \"" + DatastoreManager.ENCODED_PK + "\" extension), change the "
            + "primary key to be of type " + Key.class.getName() + ", or, if you're certain that "
            + "this class will never have a parent, change the primary key to be of type Long.");
      }
      return datastoreEntity.getKey().getName();
    }
  }

  public short fetchShortField(int fieldNumber) {
    return (Short) fetchObjectField(fieldNumber);
  }

  private boolean fieldIsOfTypeKey(int fieldNumber) {
    // Key is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(Key.class);
  }

  private boolean fieldIsOfTypeString(int fieldNumber) {
    // String is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(String.class);
  }

  private boolean fieldIsOfTypeLong(int fieldNumber) {
    // Long is final so we don't need to worry about checking for subclasses.
    return getMetaData(fieldNumber).getType().equals(Long.class);
  }

  private RuntimeException exceptionForUnexpectedKeyType(String fieldType, int fieldNumber) {
    return new IllegalStateException(
        fieldType + " for type " + getClassMetaData().getName()
            + " is of unexpected type " + getMetaData(fieldNumber).getType().getName()
            + " (must be String, Long, or " + Key.class.getName() + ")");
  }

  public Object fetchObjectField(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    if (ammd.getEmbeddedMetaData() != null) {
      return fetchEmbeddedField(ammd, fieldNumber);
    } else if (ammd.getRelationType(getClassLoaderResolver()) != Relation.NONE && !ammd.isSerialized()) {
      return relationFieldManager.fetchRelationField(getClassLoaderResolver(), ammd);
    }

    if (isPK(fieldNumber)) {
      if (fieldIsOfTypeKey(fieldNumber)) {
        // If this is a pk field, transform the Key into its String
        // representation.
        return datastoreEntity.getKey();
      } else if(fieldIsOfTypeLong(fieldNumber)) {
        return datastoreEntity.getKey().getId();
      }
      throw exceptionForUnexpectedKeyType("Primary key", fieldNumber);
    } else if (isParentPK(fieldNumber)) {
      if (fieldIsOfTypeKey(fieldNumber)) {
        return datastoreEntity.getKey().getParent();
      }
      throw exceptionForUnexpectedKeyType("Parent key", fieldNumber);
    } else if (isPKIdField(fieldNumber)) {
      return fetchPKIdField();
    } else {
      Object value = datastoreEntity.getProperty(getPropertyName(fieldNumber));
      ClassLoaderResolver clr = getClassLoaderResolver();
      if (ammd.isSerialized()) {
        if (value != null) {
          // If the field is serialized we know it's a Blob that we
          // can deserialize without any conversion necessary.
          value = deserializeFieldValue(value, clr, ammd);
        }
      } else {
        if (ammd.getAbsoluteFieldNumber() == -1) {
          // Embedded fields don't have their field number set because
          // we pull the field from the EmbeddedMetaData, not the
          // ClassMetaData.  So, if the field doesn't know its field number
          // we'll pull the metadata from the ClassMetaData instead and use
          // that one from this point forward.
          ammd = getClassMetaData().getMetaDataForMember(ammd.getName());
        }
        // Datanucleus invokes this method for the object versions
        // of primitive types as well as collections of non-persistent types.
        // We need to make sure we convert appropriately.
        value = getConversionUtils().datastoreValueToPojoValue(clr, value, getStateManager(), ammd);
        if (value != null && Enum.class.isAssignableFrom(ammd.getType())) {
          @SuppressWarnings("unchecked")
          Class<Enum> enumClass = ammd.getType();
          value = Enum.valueOf(enumClass, (String) value);
        }
      }
      return value;
    }
  }

  private Object deserializeFieldValue(
      Object value, ClassLoaderResolver clr, AbstractMemberMetaData ammd) {
    if (!(value instanceof Blob)) {
      throw new NucleusException(
          "Datastore value is of type " + value.getClass().getName() + " (must be Blob).").setFatal();
    }
    return serializationManager.deserialize(clr, ammd, (Blob) value);
  }

  private AbstractMemberMetaDataProvider getEmbeddedAbstractMemberMetaDataProvider(
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

  private StateManager getEmbeddedStateManager(AbstractMemberMetaData ammd, int fieldNumber, Object value) {
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
      value = JDOImplHelper.getInstance().newInstance(
          ammd.getType(), (javax.jdo.spi.StateManager) getStateManager());
    }
    ObjectManager objMgr = getObjectManager();
    StateManager embeddedStateMgr = objMgr.findStateManager(value);
    if (embeddedStateMgr == null) {
      embeddedStateMgr = StateManagerFactory.newStateManagerForEmbedded(objMgr, value, false);
      embeddedStateMgr.addEmbeddedOwner(getStateManager(), fieldNumber);
      embeddedStateMgr.setPcObjectType(StateManager.EMBEDDED_PC);
    }
    return embeddedStateMgr;
  }

  /**
   * We can't trust the fieldNumber on the ammd provided because some embedded
   * fields don't have this set.  That's why we pass it in as a separate param.
   */
  private Object fetchEmbeddedField(AbstractMemberMetaData ammd, int fieldNumber) {
    StateManager esm = getEmbeddedStateManager(ammd, fieldNumber, null);
    // We need to build a mapping consumer for the embedded class so that we
    // get correct fieldIndex --> metadata mappings for the class in the proper
    // embedded context
    // TODO(maxr) Consider caching this
    InsertMappingConsumer mappingConsumer = buildMappingConsumer(
        esm.getClassMetaData(), getClassLoaderResolver(),
        esm.getClassMetaData().getAllMemberPositions(),
        ammd.getEmbeddedMetaData());
    AbstractMemberMetaDataProvider ammdProvider = getEmbeddedAbstractMemberMetaDataProvider(mappingConsumer);
    fieldManagerStateStack.addFirst(new FieldManagerState(esm, ammdProvider, mappingConsumer, true));
    AbstractClassMetaData acmd = esm.getClassMetaData();
    esm.replaceFields(acmd.getAllMemberPositions(), this);
    fieldManagerStateStack.removeFirst();
    return esm.getObject();
  }

  /**
   * Ensures that the given value is not null.  Throws
   * {@link NullPointerException} with a helpful error message if it is.
   */
  private Object checkAssignmentToNotNullField(Object val, int fieldNumber) {
    if (val != null) {
      // not null so no problem
      return val;
    }
    // Put together a really helpful error message
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    String propertyName = getPropertyName(fieldNumber);
    final String msg = String.format(ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT,
        datastoreEntity.getKind(), datastoreEntity.getKey(), propertyName,
        ammd.getFullFieldName());
    throw new NullPointerException(msg);
  }

  public long fetchLongField(int fieldNumber) {
    return (Long) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public int fetchIntField(int fieldNumber) {
    return (Integer) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public float fetchFloatField(int fieldNumber) {
    return (Float) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public double fetchDoubleField(int fieldNumber) {
    return (Double) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public char fetchCharField(int fieldNumber) {
    return (Character) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public byte fetchByteField(int fieldNumber) {
    return (Byte) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public boolean fetchBooleanField(int fieldNumber) {
    return (Boolean) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
  }

  public void storeStringField(int fieldNumber, String value) {
    if (isPK(fieldNumber)) {
      storeStringPKField(fieldNumber, value);
    } else if (isParentPK(fieldNumber)) {
      storeParentStringField(value);
    } else if (isPKNameField(fieldNumber)) {
      storePKNameField(fieldNumber, value);
    } else {
      // could be a JPA "lob" field, in which case we want to store it as
      // Text.  DataNucleus sets a cmd with a jdbc type of CLOB
      // if this is the case.
      Object valueToStore = value;
      AbstractMemberMetaData ammd = getMetaData(fieldNumber);
      if (ammd.getColumnMetaData() != null &&
          ammd.getColumnMetaData().length == 1 &&
          "CLOB".equals(ammd.getColumnMetaData()[0].getJdbcType())) {
        valueToStore = new Text(value);
      }
      storeObjectField(fieldNumber, valueToStore);
    }
  }

  void storePKIdField(int fieldNumber, Object value) {
    if (!fieldIsOfTypeLong(fieldNumber)) {
      throw new FatalNucleusUserException(
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
      throw new FatalNucleusUserException(
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
        throw new FatalNucleusUserException(
            "Attempt was made to set parent to " + value
            + " but this cannot be converted into a Key.");
      }
    }
    storeParentKeyPK(key);
  }

  private void storeStringPKField(int fieldNumber, String value) {
    Key key = null;
    if (DatastoreManager.isEncodedPKField(getClassMetaData(), fieldNumber)) {
      if (value != null) {
        try {
          key = KeyFactory.stringToKey(value);
        } catch (IllegalArgumentException iae) {
          throw new FatalNucleusUserException(
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
        throw new FatalNucleusUserException(
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
   * 4) It's possible that we have attached a unidirectional child to a
   * detached parent in JPA.  In this case we consult the
   * {@link DatastoreJPACallbackHandler} to see if we can find a parent.
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
      StateManager sm = getStateManager();
      // Mechanism 1
      parentKey = KeyRegistry.getKeyRegistry(getObjectManager()).getRegisteredKey(sm.getObject());
      if (parentKey == null) {
        // Mechanism 2
        parentKey = getParentKeyFromExternalFKMappings(sm);
      }
      if (parentKey == null) {
        // Mechanism 3
        parentKey = getParentKeyFromParentField(sm);
      }
      if (parentKey == null) {
        // Mechanism 4
        Object parentPojo = DatastoreJPACallbackHandler.getAttachingParent(sm.getObject());
        if (parentPojo != null) {
          parentKey = getKeyFromParentPojo(parentPojo);
        }
      }
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

  private Key getKeyFromParentPojo(Object mergeEntity) {
    StateManager sm = getObjectManager().findStateManager(mergeEntity);
    if (sm == null) {
      return null;
    }
    return EntityUtils.getPrimaryKeyAsKey(getObjectManager().getApiAdapter(), sm);
  }

  private Key getKeyForObject(Object pc) {
    ApiAdapter adapter = getStoreManager().getOMFContext().getApiAdapter();
    Object internalPk = adapter.getTargetKeyForSingleFieldIdentity(adapter.getIdForObject(pc));
    ObjectManager om = getObjectManager();
    AbstractClassMetaData acmd =
        om.getMetaDataManager().getMetaDataForClass(pc.getClass(), getClassLoaderResolver());
    return EntityUtils.getPkAsKey(internalPk, acmd, om);
  }

  private Key getParentKeyFromParentField(StateManager sm) {
    AbstractMemberMetaData parentField = getInsertMappingConsumer().getParentMappingField();
    if (parentField == null) {
      return null;
    }
    Object parent = sm.provideField(parentField.getAbsoluteFieldNumber());
    return parent == null ? null : getKeyForObject(parent);
  }

  private Key getParentKeyFromExternalFKMappings(StateManager sm) {
    // We don't have a registered key for the object associated with the
    // state manager but there might be one tied to the foreign key
    // mappings for this object.  If this is the Many side of a bidirectional
    // One To Many it might also be available on the parent object.
    // TODO(maxr): Unify the 2 mechanisms.  We probably want to get rid of
    // the KeyRegistry.
    Set<JavaTypeMapping> externalFKMappings = getInsertMappingConsumer().getExternalFKMappings();
    for (JavaTypeMapping fkMapping : externalFKMappings) {
      Object fkValue = sm.getAssociatedValue(fkMapping);
      if (fkValue != null) {
        return getKeyForObject(fkValue);
      }
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

  private void storeKeyPK(Key key) {
    if (key != null && !datastoreEntity.getKind().equals(key.getKind())) {
      throw new FatalNucleusUserException(
          "Attempt was made to set the primray key of an entity with kind "
          + datastoreEntity.getKind() + " to a key with kind " + key.getKind());
    }
    if (datastoreEntity.getKey().isComplete()) {
      // this modification is only okay if it's actually a no-op
      if (!datastoreEntity.getKey().equals(key)) {
        if (!keyAlreadySet) {
          // Different key provided so the update isn't allowed.
          throw new FatalNucleusUserException(
              "Attempt was made to modify the primary key of an object of type "
              + getStateManager().getClassMetaData().getFullClassName() + " identified by "
              + "key " + datastoreEntity.getKey() + "  Primary keys are immutable.  "
              + "(New value: " + key);
        }
      }
    } else if (key != null) {
      Entity old = datastoreEntity;
      if (key.getParent() != null) {
        if (keyAlreadySet) {
          // can't provide a key and a parent - one or the other
          throw new FatalNucleusUserException(PARENT_ALREADY_SET);
        }
        parentAlreadySet = true;
      }
      datastoreEntity = new Entity(key);
      copyProperties(old, datastoreEntity);
      keyAlreadySet = true;
    }
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

  private boolean isPKNameField(int fieldNumber) {
    return DatastoreManager.isPKNameField(getClassMetaData(), fieldNumber);
  }

  private boolean isPKIdField(int fieldNumber) {
    return DatastoreManager.isPKIdField(getClassMetaData(), fieldNumber);
  }

  private boolean isParentPK(int fieldNumber) {
    boolean result = DatastoreManager.isParentPKField(getClassMetaData(), fieldNumber);
    if (result) {
      // ew, side effect
      parentMemberMetaData = getMetaData(fieldNumber);
    }
    return result;
  }

  private boolean isUnindexedProperty(AbstractMemberMetaData ammd) {
    return ammd.hasExtension(DatastoreManager.UNINDEXED_PROPERTY);
  }

  public void storeShortField(int fieldNumber, short value) {
    storeObjectField(fieldNumber, value);
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
        throw new FatalNucleusUserException(PARENT_ALREADY_SET);
      }
      storeKeyPK((Key) value);
    } else {
      throw exceptionForUnexpectedKeyType("Primary key", fieldNumber);
    }
  }

  void storeParentField(int fieldNumber, Object value) {
    if (fieldIsOfTypeKey(fieldNumber)) {
      storeParentKeyPK((Key) value);
    } else {
      throw exceptionForUnexpectedKeyType("Parent primary key", fieldNumber);
    }
  }

  public void storeObjectField(int fieldNumber, Object value) {
    if (isPK(fieldNumber)) {
      storePrimaryKey(fieldNumber, value);
    } else if (isParentPK(fieldNumber)) {
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
      if (value != null ) {
        if (ammd.isSerialized()) {
          // If the field is serialized we don't need to apply
          // any conversions before setting it on the entity since
          // the serialization is guaranteed to produce a Blob.
          value = serializationManager.serialize(clr, ammd, value);
        } else {
          if (Enum.class.isAssignableFrom(ammd.getType())) {
            value = ((Enum) value).name();
          }
          value = getConversionUtils().pojoValueToDatastoreValue(clr, value, ammd);
        }
      }
      if (ammd.getEmbeddedMetaData() != null) {
        storeEmbeddedField(ammd, fieldNumber, value);
      } else if ((operation == Operation.INSERT && isInsertable(ammd)) ||
                 (operation == Operation.UPDATE && isUpdatable(ammd))) {
        if (ammd.getRelationType(clr) != Relation.NONE && !ammd.isSerialized()) {
          if (!repersistingForChildKeys) {
            // register a callback for later
            relationFieldManager.storeRelationField(
                getClassMetaData(), ammd, value, createdWithoutEntity, getInsertMappingConsumer());
          }
          DatastoreTable table = getDatastoreTable();
          if (table != null && table.isParentKeyProvider(ammd)) {
            // a parent key provider is either a many-to-one or the child side of a
            // one-to-one.  Either way we don't want the entity to have a property
            // corresponding to this field because this information is available in
            // the key itself
            return;
          }

          if (!getStoreManager().storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
            // don't write child keys to the parent if the storage version isn't high enough
            return;
          }

          // We still want to write the entity property with the keys
          value = extractRelationKeys(value);
        }
        // unwrap SCO values so that the datastore api doesn't
        // honk on unknown types
        value = unwrapSCOField(fieldNumber, value);
        String propName = EntityUtils.getPropertyName(getIdentifierFactory(), ammd);
        // validate null values against ammd
        checkNullValue(ammd, propName, value);
        if (isUnindexedProperty(ammd)) {
          datastoreEntity.setUnindexedProperty(propName, value);
        } else {
          datastoreEntity.setProperty(propName, value);
        }
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

  private Object extractRelationKeys(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Collection) {
      Collection coll = (Collection) value;
      int size = coll.size();
      List<Key> keys = extractRelationKeys((Collection) value);
      // if we have fewer keys than objects then there is at least one child
      // object that still needs to be inserted.  communicate
      // this upstream.
      getStateManager().setAssociatedValue(
          DatastorePersistenceHandler.MISSING_RELATION_KEY,
           repersistingForChildKeys && size != keys.size() ? true : null);
      return keys;
    }
    Key key = extractChildKey(value);
    // if we didn't come up with a key that there is a child object that
    // still needs to be inserted.  communicate this upstream.
    getStateManager().setAssociatedValue(
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
    ObjectManager om = getObjectManager();
    StateManager sm = om.findStateManager(value);
    if (sm == null) {
      // that's fine, it just means the object hasn't been saved or that it is detached
      return null;
    }
    if (sm.isDeleted((PersistenceCapable) sm.getObject()) ) {
      return null;
    }
    Object primaryKey = storeManager.getApiAdapter().getTargetKeyForSingleFieldIdentity(
        sm.getInternalObjectId());
    if (primaryKey == null) {
      // this is ok, it just means the object has not yet been persisted
      return null;
    }
    Key key = EntityUtils.getPrimaryKeyAsKey(storeManager.getApiAdapter(), sm);
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

  Object unwrapSCOField(int fieldNumber, Object value) {
    return getStateManager().unwrapSCOField(fieldNumber, value, false);
  }

  /**
   * @see DatastoreRelationFieldManager#storeRelations
   */
  boolean storeRelations() {
    return relationFieldManager.storeRelations(KeyRegistry.getKeyRegistry(getObjectManager()));
  }

  ClassLoaderResolver getClassLoaderResolver() {
    return getObjectManager().getClassLoaderResolver();
  }

  StateManager getStateManager() {
    return fieldManagerStateStack.getFirst().stateManager;
  }

  ObjectManager getObjectManager() {
    return getStateManager().getObjectManager();
  }

  InsertMappingConsumer getInsertMappingConsumer() {
    return fieldManagerStateStack.getFirst().mappingConsumer;
  }

  /**
   * We can't trust the fieldNumber on the ammd provided because some embedded
   * fields don't have this set.  That's why we pass it in as a separate param.
   */
  private void storeEmbeddedField(AbstractMemberMetaData ammd, int fieldNumber, Object value) {
    StateManager esm = getEmbeddedStateManager(ammd, fieldNumber, value);
    // We need to build a mapping consumer for the embedded class so that we
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

  private void storeParentKeyPK(Key key) {
    if (key != null && parentAlreadySet) {
      throw new FatalNucleusUserException(PARENT_ALREADY_SET);
    }
    if (datastoreEntity.getParent() != null) {
      // update is ok if it's a no-op
      if (!datastoreEntity.getParent().equals(key)) {
        if (!parentAlreadySet) {
          throw new FatalNucleusUserException(
              "Attempt was made to modify the parent of an object of type "
              + getStateManager().getClassMetaData().getFullClassName() + " identified by "
              + "key " + datastoreEntity.getKey() + ".  Parents are immutable (changed value is " + key + ").");
        }
      }
    } else if (key != null) {
      if (!createdWithoutEntity) {
        // Shouldn't even happen.
        throw new FatalNucleusUserException("You can only rely on this class to properly handle "
            + "parent pks if you instantiated the class without providing a datastore "
            + "entity to the constructor.");
      }

      if (keyAlreadySet) {
        throw new FatalNucleusUserException(PARENT_ALREADY_SET);
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

  public void storeLongField(int fieldNumber, long value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeIntField(int fieldNumber, int value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeFloatField(int fieldNumber, float value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeDoubleField(int fieldNumber, double value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeCharField(int fieldNumber, char value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeByteField(int fieldNumber, byte value) {
    storeObjectField(fieldNumber, value);
  }

  public void storeBooleanField(int fieldNumber, boolean value) {
    storeObjectField(fieldNumber, value);
  }

  private boolean isPK(int fieldNumber) {
    // ignore the pk annotations if this object is embedded
    if (fieldManagerStateStack.getFirst().isEmbedded) {
      return false;
    }
    int[] pkPositions = getClassMetaData().getPKMemberPositions();
    // Assumes that if we have a pk we only have a single field pk
    return pkPositions != null && pkPositions[0] == fieldNumber;
  }

  private String getPropertyName(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    return EntityUtils.getPropertyName(getIdentifierFactory(), ammd);
  }

  private AbstractMemberMetaData getMetaData(int fieldNumber) {
    return fieldManagerStateStack.getFirst().abstractMemberMetaDataProvider.get(fieldNumber);
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

  AbstractClassMetaData getClassMetaData() {
    return getStateManager().getClassMetaData();
  }

  Entity getEntity() {
    return datastoreEntity;
  }

  private IdentifierFactory getIdentifierFactory() {
    return storeManager.getIdentifierFactory();
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
        Object orderValue = getStateManager().getAssociatedValue(orderMapping);
        if (orderValue != null) {
          // We got a value!  Set it on the entity.
          delayWrite = false;
          orderMapping.setObject(getObjectManager(), getEntity(), NOT_USED, orderValue);
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
  private InsertMappingConsumer buildMappingConsumer(
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
      // Either way we want to fill the consumer with mappings
      // for everything.
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

      // We're going to update the consumer's map so make a copy over which we
      // can safely iterate.
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
  private interface AbstractMemberMetaDataProvider {
    AbstractMemberMetaData get(int fieldNumber);
  }

  private static final class FieldManagerState {
    private final StateManager stateManager;
    private final AbstractMemberMetaDataProvider abstractMemberMetaDataProvider;
    private final InsertMappingConsumer mappingConsumer;
    private final boolean isEmbedded;

    private FieldManagerState(StateManager stateManager,
        AbstractMemberMetaDataProvider abstractMemberMetaDataProvider,
        InsertMappingConsumer mappingConsumer,
        boolean isEmbedded) {
      this.stateManager = stateManager;
      this.abstractMemberMetaDataProvider = abstractMemberMetaDataProvider;
      this.mappingConsumer = mappingConsumer;
      this.isEmbedded = isEmbedded;
    }
  }

}
