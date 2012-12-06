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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.ExecutionContext;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.exceptions.NotYetFlushedException;
import org.datanucleus.store.mapped.mapping.ArrayMapping;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.InterfaceMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MapMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.PersistableMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.SCO;
import org.datanucleus.util.Localiser;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.datanucleus.mapping.DatastoreTable;

/**
 * FieldManager to handle the putting of fields from a managed object into an Entity.
 * There are typically two steps to use of this field manager.
 * <ul>
 * <li>Call <pre>op.provideFields(fieldNumbers, fieldMgr);</pre></li>
 * <li>Call <pre>fieldMgr.storeRelations(keyRegistry);</pre>
 * </ul>
 * The first step will process the requested fields and set them in the Entity as required, and will also
 * register any relation fields for later processing. The second step will take the relation fields (if any)
 * from step 1, perform cascade-persist on them, and set child keys in the (parent) Entity where required.
 */
public class StoreFieldManager extends DatastoreFieldManager {
  protected static final Localiser GAE_LOCALISER = Localiser.getInstance(
      "com.google.appengine.datanucleus.Localisation", DatastoreManager.class.getClassLoader());

  private static final String PARENT_ALREADY_SET =
    "Cannot set both the primary key and a parent pk field.  If you want the datastore to "
    + "generate an id for you, set the parent pk field to be the value of your parent key "
    + "and leave the primary key field blank.  If you wish to "
    + "provide a named key, leave the parent pk field blank and set the primary key to be a "
    + "Key object made up of both the parent key and the named child.";

  public static final int IS_FK_VALUE = -2;
  private static final int[] IS_FK_VALUE_ARR = {IS_FK_VALUE};

  protected final boolean insert;

  protected boolean parentAlreadySet = false;

  protected boolean keyAlreadySet = false;

  /** Cache of relation information for processing later in "storeRelations". */
  private final List<RelationStoreInformation> relationStoreInfos = Utils.newArrayList();

  /**
   * Constructor for a StoreFieldManager when inserting a new object.
   * The Entity will be constructed.
   * @param op ObjectProvider of the object being stored
   * @param kind Kind of entity
   */
  public StoreFieldManager(ObjectProvider op, String kind) {
    super(op, new Entity(kind), null);
    this.insert = true;
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
    this.insert = false;
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
    if (isPK(fieldNumber)) {
      storePrimaryKey(fieldNumber, value);
    } else {
      storeFieldInEntity(fieldNumber, value);
    }
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
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      if (mmd.getColumnMetaData() != null &&
          mmd.getColumnMetaData().length == 1) {
        if ("CLOB".equals(mmd.getColumnMetaData()[0].getJdbcType())) {
          valueToStore = new Text(value);
        }/* else if (mmd.getColumnMetaData()[0].getLength() > 500) {
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
      storeFieldInEntity(fieldNumber, value);
    }
  }

  protected boolean isStorable(AbstractMemberMetaData mmd) {
    return ((insert && mmd.isInsertable()) || (!insert && mmd.isUpdateable()));
  }

  /**
   * Method to store the provided value in the Entity for the specified field.
   * @param fieldNumber The absolute field number
   * @param value Value to store (or rather to manipulate into a suitable form for the datastore).
   */
  private void storeFieldInEntity(int fieldNumber, Object value) {
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    if (!isStorable(mmd)) {
      return;
    }

    ClassLoaderResolver clr = getClassLoaderResolver();
    RelationType relationType = mmd.getRelationType(clr);

    if (RelationType.isRelationSingleValued(relationType) && mmd.getEmbeddedMetaData() != null) {
      // Embedded persistable object
      ObjectProvider embeddedOP = getEmbeddedObjectProvider(mmd.getType(), fieldNumber, value);

      fieldManagerStateStack.addFirst(new FieldManagerState(embeddedOP, mmd.getEmbeddedMetaData()));
      embeddedOP.provideFields(embeddedOP.getClassMetaData().getAllMemberPositions(), this);
      fieldManagerStateStack.removeFirst();

      return;
    } else if (RelationType.isRelationMultiValued(relationType) && mmd.isEmbedded()) {
      // Embedded container field
      if (mmd.hasCollection()) {
        // Embedded collections
        // This is stored flat with all property names for the element class gaining a suffix ".{index}"
        // so we have properties like "NAME.0", "PRICE.0", "NAME.1", "PRICE.1" etc.
        Class elementType = clr.classForName(mmd.getCollection().getElementType());
        Collection valueColl = (Collection) value;
        AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr, ec.getMetaDataManager());
        EmbeddedMetaData embmd = 
          mmd.getElementMetaData() != null ? mmd.getElementMetaData().getEmbeddedMetaData() : null;

        // Add property for size of collection
        String collPropName = getPropertyNameForMember(mmd) + ".size";
        EntityUtils.setEntityProperty(datastoreEntity, mmd, collPropName,
            (valueColl != null ? valueColl.size() : -1));

        // Add discriminator for elements if defined
        String collDiscName = null;
        if (elemCmd.hasDiscriminatorStrategy()) {
          collDiscName = elemCmd.getDiscriminatorColumnName();
          if (embmd != null && embmd.getDiscriminatorMetaData() != null) {
            // Override if specified under <embedded>
            DiscriminatorMetaData dismd = embmd.getDiscriminatorMetaData();
            ColumnMetaData discolmd = dismd.getColumnMetaData();
            if (discolmd != null && discolmd.getName() != null) {
              collDiscName = discolmd.getName();
            }
          }
          if (collDiscName == null) {
            collDiscName = getPropertyNameForMember(mmd) + ".discrim";
          }
        }

        if (valueColl != null) {
          Iterator collIter = valueColl.iterator();
          int index = 0;
          while (collIter.hasNext()) {
            Object element = collIter.next();

            ObjectProvider embeddedOP = getEmbeddedObjectProvider(elementType, fieldNumber, element);
            fieldManagerStateStack.addFirst(new FieldManagerState(embeddedOP, embmd, index));
            embeddedOP.provideFields(embeddedOP.getClassMetaData().getAllMemberPositions(), this);

            // Add discriminator for elements if defined
            if (collDiscName != null) {
              EntityUtils.setEntityProperty(datastoreEntity, elemCmd.getDiscriminatorMetaDataRoot(), 
                  collDiscName + "." + index, embeddedOP.getClassMetaData().getDiscriminatorValue());
            }

            fieldManagerStateStack.removeFirst();
            index++;
          }
        }
        return;
      } else if (mmd.hasArray()) {
        // Embedded arrays
        // This is stored flat with all property names for the element class gaining a suffix ".{index}"
        // so we have properties like "NAME.0", "PRICE.0", "NAME.1", "PRICE.1" etc.
        Class elementType = clr.classForName(mmd.getArray().getElementType());
        AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr, ec.getMetaDataManager());
        EmbeddedMetaData embmd = 
          mmd.getElementMetaData() != null ? mmd.getElementMetaData().getEmbeddedMetaData() : null;

        // Add property for size of array
        String arrBaseName = getPropertyNameForMember(mmd);
        EntityUtils.setEntityProperty(datastoreEntity, mmd, arrBaseName + ".size",
            (value != null ? Array.getLength(value) : -1));

        // Add discriminator for elements if defined
        String arrDiscName = null;
        if (elemCmd.hasDiscriminatorStrategy()) {
          arrDiscName = elemCmd.getDiscriminatorColumnName();
          if (embmd != null && embmd.getDiscriminatorMetaData() != null) {
            // Override if specified under <embedded>
            DiscriminatorMetaData dismd = embmd.getDiscriminatorMetaData();
            ColumnMetaData discolmd = dismd.getColumnMetaData();
            if (discolmd != null && discolmd.getName() != null) {
              arrDiscName = discolmd.getName();
            }
          }
          if (arrDiscName == null) {
            arrDiscName = getPropertyNameForMember(mmd) + ".discrim";
          }
        }

        if (value != null) {
          for (int i=0;i<Array.getLength(value);i++) {
            Object element = Array.get(value, i);

            ObjectProvider embeddedOP = getEmbeddedObjectProvider(elementType, fieldNumber, element);
            fieldManagerStateStack.addFirst(new FieldManagerState(embeddedOP, embmd, i));
            embeddedOP.provideFields(embeddedOP.getClassMetaData().getAllMemberPositions(), this);

            // Add discriminator for elements if defined
            if (arrDiscName != null) {
              EntityUtils.setEntityProperty(datastoreEntity, elemCmd.getDiscriminatorMetaDataRoot(),
                  arrDiscName + "." + i, embeddedOP.getClassMetaData().getDiscriminatorValue());
            }

            fieldManagerStateStack.removeFirst();
          }
        }
        return;
      } else if (mmd.hasMap()) {
        // TODO Support embedded maps
        throw new NucleusUserException("Don't currently support embedded Maps (" + mmd.getFullFieldName() + ")");
      }
    }

    if (mmd.isSerialized()) {
      if (value != null) {
        // Serialise the field (producing a Blob)
        value = getStoreManager().getSerializationManager().serialize(clr, mmd, value);
      } else {
        // Make sure we can have a null property for this field
        checkSettingToNullValue(mmd, value);
      }
      EntityUtils.setEntityProperty(datastoreEntity, mmd, getPropertyNameForMember(mmd), value);
      return;
    }

    if (relationType == RelationType.NONE) {
      // Basic field
      if (value == null) {
        checkSettingToNullValue(mmd, value);
      } else {
        // Perform any conversions from the field type to the stored-type
        TypeManager typeMgr = ec.getNucleusContext().getTypeManager();
        value = ((DatastoreManager) ec.getStoreManager())
            .getTypeConversionUtils()
            .pojoValueToDatastoreValue(typeMgr, clr, value, mmd);

        if (value instanceof SCO) {
          // Use the unwrapped value so the datastore doesn't fail on unknown types
          value = ((SCO)value).getValue();
        }
      }

      EntityUtils.setEntityProperty(datastoreEntity, mmd, getPropertyNameForMember(mmd), value);
      return;
    }

    // Register this relation field for later update
    relationStoreInfos.add(new RelationStoreInformation(mmd, value));

    boolean owned = MetaDataUtils.isOwnedRelation(mmd, getStoreManager());
    if (owned) {
      if (!getStoreManager().storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
        // don't write child keys to the parent if the storage version isn't high enough
        return;
      }

      // Skip out for all situations where this isn't the owner (since our key has the parent key)
      if (relationType == RelationType.MANY_TO_ONE_BI) {
        // We don't store any "FK" of the parent
        return;
      } else if (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() != null) {
        // We don't store any "FK" of the other side
        return;
      }
    }

    if (insert) {
      if (value == null) {
        // Nothing to extract
        checkSettingToNullValue(mmd, value);
      } else if (RelationType.isRelationSingleValued(relationType)) {
        Key key = EntityUtils.extractChildKey(value, ec, owned ? datastoreEntity : null);
        if (key != null && owned && !datastoreEntity.getKey().equals(key.getParent())) {
          // Detect attempt to add an object with its key set (and hence parent set) on owned field
          throw new NucleusFatalUserException(GAE_LOCALISER.msg("AppEngine.OwnedChildCannotChangeParent",
              key, datastoreEntity.getKey()));
        }
        value = key;
      } else if (RelationType.isRelationMultiValued(relationType)) {
        if (mmd.hasCollection()) {
          if (value != null) {
            value = getDatastoreObjectForCollection(mmd, (Collection)value, ec, owned, false);
          }
        } else if (mmd.hasArray()) {
          if (value != null) {
            value = getDatastoreObjectForArray(mmd, value, ec, owned, false);
          }
        } else if (mmd.hasMap()) {
          if (value != null) {
            value = getDatastoreObjectForMap(mmd, (Map)value, ec, clr, owned, false);
          }
        }

        if (value instanceof SCO) {
          // Use the unwrapped value so the datastore doesn't fail on unknown types
          value = ((SCO)value).getValue();
        }
      }

      EntityUtils.setEntityProperty(datastoreEntity, mmd, getPropertyNameForMember(mmd), value);
    }
  }

  /**
   * Convenience method to process a related persistable object, persisting and flushing it as necessary
   * and returning the persistent object.
   * @param mmd Field where the object is stored.
   * @param pc The object to process
   * @return The persisted form of the object (or null if deleted)
   */
  Object processPersistable(AbstractMemberMetaData mmd, Object pc) {
    if (ec.getApiAdapter().isDeleted(pc)) {
      // Child is deleted, so return null
      return null;
    }

    Object childPC = pc;
    if (ec.getApiAdapter().isDetached(pc)) {
      // Child is detached, so attach it
      childPC = ec.persistObjectInternal(pc, null, -1, ObjectProvider.PC);
    }
    ObjectProvider childOP = ec.findObjectProvider(childPC);
    if (childOP == null) {
      // Not yet persistent, so persist it
      childPC = ec.persistObjectInternal(childPC, null, -1, ObjectProvider.PC);
      childOP = ec.findObjectProvider(childPC);
    }

    if (mmd.getAbstractClassMetaData().getIdentityType() == IdentityType.DATASTORE) {
      OID oid = (OID)childOP.getInternalObjectId();
      if (oid == null) {
        // Object has not yet flushed to the datastore
        childOP.flush();
      }
    } else {
      Object primaryKey = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(childOP.getInternalObjectId());
      if (primaryKey == null) {
        // Object has not yet flushed to the datastore
        childOP.flush();
      }
    }
    return childPC;
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

    if (mmd.getType().equals(long.class)) {
      Key key = null;
      if (mmd.getValueStrategy() == IdentityStrategy.IDENTITY) {
        // Key being generated by datastore so put null key in
        key = null;
      } else {
        key = KeyFactory.createKey(datastoreEntity.getKind(), (Long) value);
      }
      storeKeyPK(key);
    } else if (mmd.getType().equals(Long.class)) {
      Key key = null;
      if (value != null) {
        // TODO This is actually against the JDO/JPA specs; having IDENTITY strategy means the DB will (always) choose
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
        throw new NucleusFatalUserException("Attempt was made to set parent to " + value +
            " but this cannot be converted into a Key.");
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
      if (!insert) {
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
          "Attempt was made to set the primary key of an entity with kind "
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

  private void checkSettingToNullValue(AbstractMemberMetaData mmd, Object value) {
    if (value == null) {
      if (mmd.getNullValue() == NullValue.EXCEPTION) {
        // JDO spec 18.15, throw XXXUserException when trying to store null and have handler set to EXCEPTION
        throw new NucleusUserException("Field/Property " + mmd.getFullFieldName() +
          " is null, but is mandatory as it's described in the jdo metadata");
      }

      ColumnMetaData[] colmds = mmd.getColumnMetaData();
      if (colmds != null && colmds.length > 0) {
        if (colmds[0].getAllowsNull() == Boolean.FALSE) {
          // Column specifically marked as not-nullable
          throw new NucleusDataStoreException("Field/Property " + mmd.getFullFieldName() +
            " is null, but the column is specified as not-nullable");
        }
      }
    }
  }

  /**
   * Method to process all relations that have been identified by earlier call(s) of op.provideField(...).
   * Registers the parent key against any owned child objects, performs cascade-persist, and then stores
   * child keys in the parent where the storageVersion requires it.
   * @param keyRegistry The key registry to set any parent information in
   * @return {@code true} if the entity has had properties updated during this method, {@code false} otherwise.
   */
  boolean storeRelations(KeyRegistry keyRegistry) {
    if (relationStoreInfos.isEmpty()) {
      // No relations waiting to be persisted
      return false;
    }

    ObjectProvider op = getObjectProvider();
    DatastoreTable table = getDatastoreTable();
    if (datastoreEntity.getKey() != null) {
      Key key = datastoreEntity.getKey();
      AbstractClassMetaData acmd = op.getClassMetaData();
      int[] relationFieldNums = acmd.getRelationMemberPositions(ec.getClassLoaderResolver(), ec.getMetaDataManager());
      if (relationFieldNums != null) {
        for (int i=0;i<relationFieldNums.length;i++) {
          AbstractMemberMetaData mmd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(relationFieldNums[i]);
          boolean owned = MetaDataUtils.isOwnedRelation(mmd, getStoreManager());
          if (owned) {
            // Register parent key for all owned related objects
            Object childValue = op.provideField(mmd.getAbsoluteFieldNumber());
            if (childValue != null) {
              if (childValue instanceof Object[]) {
                childValue = Arrays.asList((Object[]) childValue);
              }

              String expectedType = mmd.getTypeName();
              if (mmd.getCollection() != null) {
                CollectionMetaData cmd = mmd.getCollection();
                expectedType = cmd.getElementType();
              } else if (mmd.getArray() != null) {
                ArrayMetaData amd = mmd.getArray();
                expectedType = amd.getElementType();
              }

              if (childValue instanceof Iterable) {
                // TODO(maxr): Make sure we're not pulling back unnecessary data when we iterate over the values.
                for (Object element : (Iterable) childValue) {
                  addToParentKeyMap(keyRegistry, element, key, ec, expectedType, true);
                }
              } else if (childValue.getClass().isArray()) {
                for (int j=0;j<Array.getLength(childValue);i++) {
                  addToParentKeyMap(keyRegistry, Array.get(childValue, i), key, ec, expectedType, true);
                }
              } else if (childValue instanceof Map) {
                boolean persistableKey = (mmd.getMap().getKeyClassMetaData(ec.getClassLoaderResolver(), ec.getMetaDataManager()) != null);
                boolean persistableVal = (mmd.getMap().getValueClassMetaData(ec.getClassLoaderResolver(), ec.getMetaDataManager()) != null);
                Iterator entryIter = ((Map)childValue).entrySet().iterator();
                while (entryIter.hasNext()) {
                  Map.Entry entry = (Map.Entry)entryIter.next();
                  if (persistableKey) {
                    addToParentKeyMap(keyRegistry, entry.getKey(), key, ec, expectedType, true);
                  }
                  if (persistableVal) {
                    addToParentKeyMap(keyRegistry, entry.getValue(), key, ec, expectedType, true);
                  }
                }
              } else {
                addToParentKeyMap(keyRegistry, childValue, key, ec, expectedType, 
                    !table.isParentKeyProvider(mmd));
              }
            }
          } else {
            // Register that related object(s) is unowned
            Object childValue = op.provideField(mmd.getAbsoluteFieldNumber());
            if (childValue != null) {
              if (childValue instanceof Object[]) {
                childValue = Arrays.asList((Object[]) childValue);
              }

              if (childValue instanceof Iterable) {
                // TODO(maxr): Make sure we're not pulling back unnecessary data when we iterate over the values.
                for (Object element : (Iterable) childValue) {
                  keyRegistry.registerUnownedObject(element);
                }
              } else {
                keyRegistry.registerUnownedObject(childValue);
              }
            }
          }
        }
      }
    }

    boolean modifiedEntity = false;

    // Stage 1 : process FKs
    for (RelationStoreInformation relInfo : relationStoreInfos) {
      AbstractMemberMetaData mmd = relInfo.mmd;
      try {
        JavaTypeMapping mapping = table.getMemberMappingInDatastoreClass(relInfo.mmd);
        if (mapping instanceof EmbeddedPCMapping ||
            mapping instanceof SerialisedPCMapping ||
            mapping instanceof SerialisedReferenceMapping ||
            mapping instanceof PersistableMapping ||
            mapping instanceof InterfaceMapping) {
          boolean owned = MetaDataUtils.isOwnedRelation(mmd, getStoreManager());
          RelationType relationType = mmd.getRelationType(ec.getClassLoaderResolver());
          if (owned) {
            // Owned relation
            boolean owner = false;
            if (relationType == RelationType.ONE_TO_ONE_UNI || relationType == RelationType.ONE_TO_MANY_UNI ||
                relationType == RelationType.ONE_TO_MANY_BI) {
              owner = true;
            } else if (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() == null) {
              owner = true;
            }

            if (!table.isParentKeyProvider(mmd)) {
              // Make sure the parent key is set properly between parent and child objects
              if (!owner) {
                ObjectProvider parentOP = ec.findObjectProvider(relInfo.value);
                EntityUtils.checkParentage(op.getObject(), parentOP);
              } else {
                EntityUtils.checkParentage(relInfo.value, op);
              }
              mapping.setObject(ec, datastoreEntity, IS_FK_VALUE_ARR, relInfo.value, op, mmd.getAbsoluteFieldNumber());
            }
          } else {
            // Unowned relation
            mapping.setObject(ec, datastoreEntity, IS_FK_VALUE_ARR, relInfo.value, op, mmd.getAbsoluteFieldNumber());
          }
        }
      } catch (NotYetFlushedException e) {
        // Ignore this. We always have the object in the datastore, at least partially to get the key
      }
    }

    // Stage 2 : postInsert/postUpdate
    for (RelationStoreInformation relInfo : relationStoreInfos) {
      try {
        JavaTypeMapping mapping = table.getMemberMappingInDatastoreClass(relInfo.mmd);
        if (mapping instanceof ArrayMapping || mapping instanceof MapMapping) {
          // Ignore postInsert/update for arrays/maps since don't support backing stores
        } else if (mapping instanceof MappingCallbacks) {
          if (insert) {
            ((MappingCallbacks)mapping).postInsert(op);
          } else {
            ((MappingCallbacks)mapping).postUpdate(op);
          }
        }
      } catch (NotYetFlushedException e) {
        // Ignore this. We always have the object in the datastore, at least partially to get the key
      }
    }

    // Stage 3 : set child keys in parent
    for (RelationStoreInformation relInfo : relationStoreInfos) {
      AbstractMemberMetaData mmd = relInfo.mmd;
      RelationType relationType = mmd.getRelationType(ec.getClassLoaderResolver());

      boolean owned = MetaDataUtils.isOwnedRelation(mmd, getStoreManager());
      if (owned) {
        // Owned relations only store child keys if storageVersion high enough, and at "owner" side.
        if (!getStoreManager().storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
          // don't write child keys to the parent if the storage version isn't high enough
          continue;
        }
        if (relationType == RelationType.MANY_TO_ONE_BI) {
          // We don't store any "FK" of the parent at the child side (use parent key instead)
          continue;
        } else if (relationType == RelationType.ONE_TO_ONE_BI && mmd.getMappedBy() != null) {
          // We don't store any "FK" at the non-owner side (use parent key instead)
          continue;
        }
      }

      Object value = relInfo.value;
      String propName = EntityUtils.getPropertyName(getStoreManager().getIdentifierFactory(), mmd);
      if (value == null) {
        // Nothing to extract
        checkSettingToNullValue(mmd, value);
        if (!datastoreEntity.hasProperty(propName) || datastoreEntity.getProperty(propName) != null) {
          modifiedEntity = true;
          EntityUtils.setEntityProperty(datastoreEntity, mmd, propName, value);
        }
      } else if (RelationType.isRelationSingleValued(relationType)) {
        if (ec.getApiAdapter().isDeleted(value)) {
          value = null;
        } else {
          Key key = EntityUtils.extractChildKey(value, ec, owned ? datastoreEntity : null);
          if (key == null) {
            Object childPC = processPersistable(mmd, value);
            if (childPC != value) {
              // Child object has been persisted/attached, so update it in the owner
              op.replaceField(mmd.getAbsoluteFieldNumber(), childPC);
            }
            key = EntityUtils.extractChildKey(childPC, ec, owned ? datastoreEntity : null);
          }
          if (owned) {
            // Check that we aren't assigning an owned child with different parent
            if (!datastoreEntity.getKey().equals(key.getParent())) {
              throw new NucleusFatalUserException(GAE_LOCALISER.msg("AppEngine.OwnedChildCannotChangeParent",
                  key, datastoreEntity.getKey()));
            }
          }
          value = key;
          if (!datastoreEntity.hasProperty(propName) || !value.equals(datastoreEntity.getProperty(propName))) {
            modifiedEntity = true;
            EntityUtils.setEntityProperty(datastoreEntity, mmd, propName, value);
          }
        }
      } else if (RelationType.isRelationMultiValued(relationType)) {
        if (mmd.hasCollection()) {
          value = getDatastoreObjectForCollection(mmd, (Collection)value, ec, owned, true);
          if (!datastoreEntity.hasProperty(propName) || !value.equals(datastoreEntity.getProperty(propName))) {
            modifiedEntity = true;
            EntityUtils.setEntityProperty(datastoreEntity, mmd, propName, value);
          }
        } else if (mmd.hasArray()) {
          value = getDatastoreObjectForArray(mmd, value, ec, owned, true);
          if (!datastoreEntity.hasProperty(propName) || !value.equals(datastoreEntity.getProperty(propName))) {
            modifiedEntity = true;
            EntityUtils.setEntityProperty(datastoreEntity, mmd, propName, value);
          }
        } else if (mmd.hasMap()) {
          value = getDatastoreObjectForMap(mmd, (Map)value, ec, ec.getClassLoaderResolver(), owned, true);
          if (!datastoreEntity.hasProperty(propName) || !value.equals(datastoreEntity.getProperty(propName))) {
            modifiedEntity = true;
            EntityUtils.setEntityProperty(datastoreEntity, mmd, propName, value);
          }
        }
      }
    }

    relationStoreInfos.clear();

    return modifiedEntity;
  }

  // Nonsense about registering parent key
  private void addToParentKeyMap(KeyRegistry keyRegistry, Object childValue, Key key, ExecutionContext ec,
      String expectedType, boolean checkForPolymorphism) {

    boolean throwException = getStoreManager().getBooleanProperty("datanucleus.appengine.throwExceptionOnUnexpectedPolymorphism", true);
    if (checkForPolymorphism && !throwException) {
      // Override the throw of an exception for something that is illogical (to me)
      checkForPolymorphism = false;
    }

    // TODO Remove this check when we dump the old storageVersion. We're storing the Key of the other object
    // in the owner; there are no remote FKs with the latest storageVersion so why should this check be here?
    if (checkForPolymorphism && childValue != null && !childValue.getClass().getName().equals(expectedType)) {
      AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(childValue.getClass(),
          ec.getClassLoaderResolver());
      if (!MetaDataUtils.isNewOrSuperclassTableInheritanceStrategy(acmd)) {
        throw new UnsupportedOperationException(
            "Received a child of type " + childValue.getClass().getName() + " for a field of type " +
            expectedType + ". Unfortunately polymorphism in relationships is only supported for the " +
            "superclass-table inheritance mapping strategy.");
      }
    }

    keyRegistry.registerParentKeyForOwnedObject(childValue, key);
  }

  private class RelationStoreInformation {
    AbstractMemberMetaData mmd;
    Object value;

    public RelationStoreInformation(AbstractMemberMetaData mmd, Object val) {
      this.mmd = mmd;
      this.value = val;
    }
  }

  /**
   * Method to make sure that the Entity has its parentKey assigned.
   * Will update the Entity of this StoreFieldManager as necessary for the parent key.
   * Returns the assigned parent PK (when we have a "gae.parent-pk" field/property in this class).
   * @return The parent key if the pojo class has a parent property. Note that a return value of {@code null} 
   *   does not mean that an entity group was not established, it just means the pojo doesn't have a distinct
   *   field for the parent.
   */
  Object establishEntityGroup() {
    Key parentKey = datastoreEntity.getParent();
    if (parentKey == null) {
      KeyRegistry keyReg = KeyRegistry.getKeyRegistry(ec);
      if (keyReg.isUnowned(getObjectProvider().getObject())) {
        return null;
      }

      parentKey = EntityUtils.getParentKey(datastoreEntity, getObjectProvider());
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

  /**
   * Convenience method to convert a collection field into a form suitable for the datastore.
   * Converts the collection into a List, and any persistable elements become just the Key for that object.
   * @param mmd Metadata for the map field
   * @param map The map value
   * @param ec Execution Context
   * @param owned Whether the field is owned
   * @param cascadePersist Whether to cascade persist any persistable keys/values in the collection that arent yet persistent
   * @return The datastore object
   */
  protected Object getDatastoreObjectForCollection(AbstractMemberMetaData mmd, Collection coll,
      ExecutionContext ec, boolean owned, boolean cascadePersist) {
    List<Key> keys = Utils.newArrayList();
    for (Object obj : coll) {
      if (!ec.getApiAdapter().isDeleted(obj)) {
        Key key = EntityUtils.extractChildKey(obj, ec, owned ? datastoreEntity : null);
        if (key != null) {
          keys.add(key);
        } else if (cascadePersist) {
          Object childPC = processPersistable(mmd, obj);
          key = EntityUtils.extractChildKey(childPC, ec, owned ? datastoreEntity : null);
          keys.add(key);
        } else {
          keys.add(null);
        }

        if (owned) {
          // Check that we aren't assigning an owned child with different parent
          if (key != null && !datastoreEntity.getKey().equals(key.getParent())) {
            throw new NucleusFatalUserException(GAE_LOCALISER.msg("AppEngine.OwnedChildCannotChangeParent",
                key, datastoreEntity.getKey()));
          }
        }
      }
    }
    return keys;
  }

  /**
   * Convenience method to convert an array field into a form suitable for the datastore.
   * Converts the array into a List, and any persistable elements become just the Key for that object.
   * @param mmd Metadata for the map field
   * @param map The map value
   * @param ec Execution Context
   * @param owned Whether the field is owned
   * @param cascadePersist Whether to cascade persist any persistable keys/values in the array that arent yet persistent
   * @return The datastore object
   */
  protected List getDatastoreObjectForArray(AbstractMemberMetaData mmd, Object arr,
      ExecutionContext ec, boolean owned, boolean cascadePersist) {
    List keys = Utils.newArrayList();
    for (int i=0;i<Array.getLength(arr);i++) {
      Object obj = Array.get(arr, i);
      if (!ec.getApiAdapter().isDeleted(obj)) {
        Key key = EntityUtils.extractChildKey(obj, ec, owned ? datastoreEntity : null);
        if (key != null) {
          keys.add(key);
        } else if (cascadePersist) {
          Object childPC = processPersistable(mmd, obj);
          key = EntityUtils.extractChildKey(childPC, ec, owned ? datastoreEntity : null);
          keys.add(key);
        } else {
          keys.add(null);
        }

        if (owned) {
          // Check that we aren't assigning an owned child with different parent
          if (key != null && !datastoreEntity.getKey().equals(key.getParent())) {
            throw new NucleusFatalUserException(GAE_LOCALISER.msg("AppEngine.OwnedChildCannotChangeParent",
                key, datastoreEntity.getKey()));
          }
        }
      }
    }
    return keys;
  }

  /**
   * Convenience method to convert a Map field into a form suitable for the datastore.
   * Converts the Map into a List, and any persistable keys/values become just the Key for that object.
   * @param mmd Metadata for the map field
   * @param map The map value
   * @param ec Execution Context
   * @param clr ClassLoader resolver
   * @param owned Whether the field is owned
   * @param cascadePersist Whether to cascade persist any persistable keys/values in the map that arent yet persistent
   * @return The datastore object
   */
  protected List getDatastoreObjectForMap(AbstractMemberMetaData mmd, Map map,
      ExecutionContext ec, ClassLoaderResolver clr, boolean owned, boolean cascadePersist) {
    if (map == null) {
      return null;
    }

    boolean persistableKey = (mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager()) != null);
    boolean persistableVal = (mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager()) != null);
    List keysValues = Utils.newArrayList();
    Iterator<Map.Entry> entryIter = map.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry entry = entryIter.next();
      if (persistableKey) {
        Key key = EntityUtils.extractChildKey(entry.getKey(), ec, owned ? datastoreEntity : null);
        if (key != null) {
          keysValues.add(key);
        } else if (cascadePersist) {
          Object childPC = processPersistable(mmd, entry.getKey());
          key = EntityUtils.extractChildKey(childPC, ec, owned ? datastoreEntity : null);
          keysValues.add(key);
        } else {
          keysValues.add(null);
        }
      } else {
        // TODO Make use of TypeConversionUtils
        keysValues.add(entry.getKey());
      }

      if (persistableVal) {
        Key key = EntityUtils.extractChildKey(entry.getValue(), ec, owned ? datastoreEntity : null);
        if (key != null) {
          keysValues.add(key);
        } else if (cascadePersist) {
          Object childPC = processPersistable(mmd, entry.getValue());
          key = EntityUtils.extractChildKey(childPC, ec, owned ? datastoreEntity : null);
          keysValues.add(key);
        } else {
          keysValues.add(null);
        }
      } else {
        // TODO Make use of TypeConversionUtils
        keysValues.add(entry.getValue());
      }
    }
    return keysValues;
  }
}