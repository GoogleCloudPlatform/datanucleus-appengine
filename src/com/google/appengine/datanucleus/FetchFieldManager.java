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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.ExecutionContext;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.NucleusLogger;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.datanucleus.mapping.DatastoreTable;

/**
 * FieldManager to handle the fetching of fields from an Entity into a managed object.
 */
public class FetchFieldManager extends DatastoreFieldManager
{
  private static final int[] NOT_USED = {0};
  private static final String ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT =
      "Datastore entity with kind %s and key %s has a null property named %s.  This property is " +
      "mapped to %s, which cannot accept null values.";

  /**
   * Constructor where you want to retrieve particular fields from the Entity.
   * Typically this is called from a "find" call where we are passed the ObjectProvider of a managed object
   * and we want to retrieve the values of some fields from the datastore putting them into the object
   * @param op ObjectProvider of the object being fetched
   * @param datastoreEntity The Entity to extract the results from
   * @param fieldNumbers The field numbers being extracted
   */
  public FetchFieldManager(ObjectProvider op, Entity datastoreEntity, int[] fieldNumbers) {
    super(op, datastoreEntity, fieldNumbers);
  }

  /**
   * Constructor where you want to retrieve any fields from the Entity.
   * Typically this is called from a Query where we have created an ObjectProvider to hold the object
   * and we are copying the field values in.
   * @param op ObjectProvider for the object being fetched
   * @param datastoreEntity The Entity to extract results from
   */
  public FetchFieldManager(ObjectProvider op, Entity datastoreEntity) {
    super(op, datastoreEntity, null);
  }

  public boolean fetchBooleanField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null) {
          return Boolean.valueOf(dflt);
      }
      return false;
    }
    return (Boolean) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public byte fetchByteField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null) {
          return Byte.valueOf(dflt);
      }
      return 0;
    }
    return (Byte) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public char fetchCharField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null && dflt.length() > 0) {
          return dflt.charAt(0);
      }
      return 0;
    }
    return (Character) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public double fetchDoubleField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null) {
          return Double.valueOf(dflt);
      }
      return 0;
    }
    return (Double) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public float fetchFloatField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null) {
          return Float.valueOf(dflt);
      }
      return 0;
    }
    return (Float) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public int fetchIntField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null) {
          return Integer.valueOf(dflt);
      }
      return 0;
    }
    return (Integer) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public long fetchLongField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null) {
          return Long.valueOf(dflt);
      }
      return 0;
    }
    return (Long) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public short fetchShortField(int fieldNumber) {
    Object value = fetchFieldFromEntity(fieldNumber);
    if (value == null) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      String dflt = MetaDataUtils.getDefaultValueForMember(mmd);
      if (dflt != null) {
          return Short.valueOf(dflt);
      }
      return 0;
    }
    return (Short) checkAssignmentToNotNullField(value, fieldNumber);
  }

  public String fetchStringField(int fieldNumber) {
    if (isPK(fieldNumber)) {
      if (MetaDataUtils.isEncodedPKField(getClassMetaData(), fieldNumber)) {
        // If this is an encoded pk field, transform the Key into its String representation.
        return KeyFactory.keyToString(datastoreEntity.getKey());
      } else {
        if (datastoreEntity.getKey().isComplete() && datastoreEntity.getKey().getName() == null) {
          // This is trouble, probably an incorrect mapping.
          throw new NucleusFatalUserException(
              "The primary key for " + getClassMetaData().getFullClassName() + " is an unencoded "
              + "string but the key of the corresponding entity in the datastore does not have a "
              + "name.  You may want to either change the primary key to be an encoded string "
              + "(add the \"" + DatastoreManager.ENCODED_PK + "\" extension), change the "
              + "primary key to be of type " + Key.class.getName() + ", or, if you're certain that "
              + "this class will never have a parent, change the primary key to be of type Long.");
        }
        return datastoreEntity.getKey().getName();
      }
    } else if (MetaDataUtils.isParentPKField(getClassMetaData(), fieldNumber)) {
      Key parentKey = datastoreEntity.getKey().getParent();
      if (parentKey == null) {
        return null;
      }
      return KeyFactory.keyToString(parentKey);
    } else if (MetaDataUtils.isPKNameField(getClassMetaData(), fieldNumber)) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      if (!mmd.getType().equals(String.class)) {
        throw new NucleusFatalUserException(
            "Field with \"" + DatastoreManager.PK_NAME + "\" extension must be of type String");
      }

      Key key = datastoreEntity.getKey();
      if (key.getName() == null) {
        throw new NucleusFatalUserException(
            "Attempting to fetch field with \"" + DatastoreManager.PK_NAME + "\" extension but the "
            + "entity is identified by an id, not a name.");
      }
      return datastoreEntity.getKey().getName();
    }

    Object fieldVal = fetchFieldFromEntity(fieldNumber);
    if (fieldVal instanceof Text) {
      // must be a lob field
      fieldVal = ((Text) fieldVal).getValue();
    }
    return (String) fieldVal;
  }

  public Object fetchObjectField(int fieldNumber) {
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    ClassLoaderResolver clr = getClassLoaderResolver();
    RelationType relationType = mmd.getRelationType(clr);
    if (mmd.getEmbeddedMetaData() != null && RelationType.isRelationSingleValued(relationType)) {
      // Embedded persistable object
      ObjectProvider embeddedOP = getEmbeddedObjectProvider(mmd.getType(), fieldNumber, null);

      fieldManagerStateStack.addFirst(new FieldManagerState(embeddedOP, mmd.getEmbeddedMetaData()));
      try {
        embeddedOP.replaceFields(embeddedOP.getClassMetaData().getAllMemberPositions(), this);

        // Checks for whether the member values imply a null object
        if (mmd.getEmbeddedMetaData() != null && mmd.getEmbeddedMetaData().getNullIndicatorColumn() != null) {
          String nullColumn = mmd.getEmbeddedMetaData().getNullIndicatorColumn();
          String nullValue = mmd.getEmbeddedMetaData().getNullIndicatorValue();
          AbstractMemberMetaData[] embMmds = mmd.getEmbeddedMetaData().getMemberMetaData();
          AbstractMemberMetaData nullMmd = null;
          for (int i=0;i<embMmds.length;i++) {
            ColumnMetaData[] colmds = embMmds[i].getColumnMetaData();
            if (colmds != null && colmds.length > 0 && colmds[0].getName() != null && colmds[0].getName().equals(nullColumn)) {
              nullMmd = embMmds[i];
              break;
            }
          }
          if (nullMmd != null) {
            int nullFieldPos = embeddedOP.getClassMetaData().getAbsolutePositionOfMember(nullMmd.getName());
            Object val = embeddedOP.provideField(nullFieldPos);
            if (val == null && nullValue == null) {
              return null;
            } else if (val != null && nullValue != null && val.equals(nullValue)) {
              return null;
            }
          }
          return embeddedOP.getObject();
        }
        else {
          return embeddedOP.getObject();
        }
      } finally {
        fieldManagerStateStack.removeFirst();
      }
    } else if (RelationType.isRelationMultiValued(relationType) && mmd.isEmbedded()) {
      // Embedded container
      if (mmd.hasCollection()) {
        // Embedded collections
        String collPropName = getPropertyNameForMember(mmd) + ".size";
        Long collSize = (Long)datastoreEntity.getProperty(collPropName);
        if (collSize == null || collSize == -1) {
          // Size of collection not stored or stored as -1, so null on persist
          return null;
        }

        Class elementType = clr.classForName(mmd.getCollection().getElementType());
        AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr, ec.getMetaDataManager());
        EmbeddedMetaData embmd = 
          mmd.getElementMetaData() != null ? mmd.getElementMetaData().getEmbeddedMetaData() : null;
        Collection<Object> coll;
        try {
          Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
          coll = (Collection<Object>) instanceType.newInstance();
        } catch (Exception e) {
          throw new NucleusDataStoreException(e.getMessage(), e);
        }

        // Use discriminator for elements if available
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

        for (int i=0;i<collSize;i++) {
          Class elementCls = elementType;
          if (collDiscName != null) {
            Object discVal = datastoreEntity.getProperty(collDiscName + "." + i);
            String className = 
              org.datanucleus.metadata.MetaDataUtils.getClassNameFromDiscriminatorValue((String)discVal, 
                  elemCmd.getDiscriminatorMetaDataRoot(), ec);
            elementCls = clr.classForName(className);
          }

          ObjectProvider embeddedOP = getEmbeddedObjectProvider(elementCls, fieldNumber, null);
          fieldManagerStateStack.addFirst(new FieldManagerState(embeddedOP, embmd, i));
          try {
            embeddedOP.replaceFields(embeddedOP.getClassMetaData().getAllMemberPositions(), this);
          } finally {
            fieldManagerStateStack.removeFirst();
          }
          coll.add(embeddedOP.getObject());
        }
        return getObjectProvider().wrapSCOField(fieldNumber, coll, false, false, true);
      } else if (mmd.hasArray()) {
        // Embedded arrays
        String arrPropName = getPropertyNameForMember(mmd) + ".size";
        Long arrSize = (Long)datastoreEntity.getProperty(arrPropName);
        if (arrSize == null || arrSize == -1) {
          // Size of array not stored or stored as -1, so null on persist
          return null;
        }

        Class elementType = clr.classForName(mmd.getArray().getElementType());
        AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr, ec.getMetaDataManager());
        EmbeddedMetaData embmd =
          mmd.getElementMetaData() != null ? mmd.getElementMetaData().getEmbeddedMetaData() : null;
        Object value = Array.newInstance(elementType, arrSize.intValue());

        // Use discriminator for elements if available
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

        for (int i=0;i<arrSize;i++) {
          Class elementCls = elementType;
          if (arrDiscName != null) {
            Object discVal = datastoreEntity.getProperty(arrDiscName + "." + i);
            String className = 
              org.datanucleus.metadata.MetaDataUtils.getClassNameFromDiscriminatorValue((String)discVal, 
                  elemCmd.getDiscriminatorMetaDataRoot(), ec);
            elementCls = clr.classForName(className);
          }

          ObjectProvider embeddedOP = getEmbeddedObjectProvider(elementCls, fieldNumber, null);
          fieldManagerStateStack.addFirst(new FieldManagerState(embeddedOP, embmd, i));
          try {
            embeddedOP.replaceFields(embeddedOP.getClassMetaData().getAllMemberPositions(), this);
          } finally {
            fieldManagerStateStack.removeFirst();
          }
          Array.set(value, i, embeddedOP.getObject());
        }
        return value;
      } else if (mmd.hasMap()) {
        // TODO Support embedded maps
        throw new NucleusUserException("Don't currently support embedded maps at " + mmd.getFullFieldName());
      }
    }

    if (mmd.getRelationType(clr) != RelationType.NONE && !mmd.isSerialized()) {
      return fetchRelationField(clr, mmd);
    }

    return fetchFieldFromEntity(fieldNumber);
  }

  Object fetchFieldFromEntity(int fieldNumber) {
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    if (isPK(fieldNumber)) {
      if (mmd.getType().equals(Key.class)) {
        // If this is a pk field, transform the Key into its String representation.
        return datastoreEntity.getKey();
      } else if(mmd.getType().equals(Long.class) || mmd.getType().equals(long.class)) {
        return datastoreEntity.getKey().getId();
      }
      throw exceptionForUnexpectedKeyType("Primary key", fieldNumber);
    } else if (MetaDataUtils.isParentPKField(getClassMetaData(), fieldNumber)) {
      if (mmd.getType().equals(Key.class)) {
        return datastoreEntity.getKey().getParent();
      }
      throw exceptionForUnexpectedKeyType("Parent key", fieldNumber);
    } else if (MetaDataUtils.isPKIdField(getClassMetaData(), fieldNumber)) {
      Key key = datastoreEntity.getKey();
      if (key.getName() != null) {
        throw new NucleusFatalUserException(
            "Attempting to fetch field with \"" + DatastoreManager.PK_ID + "\" extension but the "
            + "entity is identified by a name, not an id.");
      }
      return datastoreEntity.getKey().getId();
    } else {
      Object value = datastoreEntity.getProperty(getPropertyNameForMember(mmd));
      ClassLoaderResolver clr = getClassLoaderResolver();
      if (mmd.isSerialized()) {
        if (value != null) {
          // If the field is serialized we know it's a Blob that we can deserialize without any conversion necessary.
          if (!(value instanceof Blob)) {
            throw new NucleusException(
                "Datastore value is of type " + value.getClass().getName() + " (must be Blob).").setFatal();
          }
          value = getStoreManager().getSerializationManager().deserialize(clr, mmd, (Blob) value);
        }
      } else {
        if (mmd.getAbsoluteFieldNumber() == -1) {
          // Embedded fields don't have their field number set because
          // we pull the field from the EmbeddedMetaData, not the
          // ClassMetaData.  So, if the field doesn't know its field number
          // we'll pull the metadata from the ClassMetaData instead and use
          // that one from this point forward.
          mmd = getClassMetaData().getMetaDataForMember(mmd.getName());
        }

        if (mmd.getTypeConverterName() != null) {
          // User-defined TypeConverter
          TypeConverter conv = ec.getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
          value = conv.toMemberType(value);
        } else {
          // Perform any conversions from the stored-type to the field type
          TypeManager typeMgr = ec.getNucleusContext().getTypeManager();
          value = ((DatastoreManager) ec.getStoreManager()).getTypeConversionUtils()
              .datastoreValueToPojoValue(typeMgr, clr, value, mmd);
        }

        if (value != null && !(value instanceof SCO)) {
          value = getObjectProvider().wrapSCOField(fieldNumber, value, false, false, true);
        }
      }
      return value;
    }
  }

  Object fetchRelationField(ClassLoaderResolver clr, AbstractMemberMetaData mmd) {
    Object value = null;
    RelationType relationType = mmd.getRelationType(clr);
    if (RelationType.isRelationMultiValued(relationType)) {
      String propName = getPropertyNameForMember(mmd);
      if (datastoreEntity.hasProperty(propName)) {
        if (mmd.hasCollection()) {
          // Fields of type Collection<PC>
          return getCollectionFromDatastoreObject(mmd, ec, clr, propName);
        } else if (mmd.hasArray()) {
          // Fields of type PC[]
          return getArrayFromDatastoreObject(mmd, ec, clr, propName);
        } else if (mmd.hasMap()) {
          // Fields of type Map<PC, NonPC>, Map<NonPC, PC>, Map<PC, PC>
          return getMapFromDatastoreObject(mmd, ec, clr, propName);
        }
      }
      return null;
    } else if (RelationType.isRelationSingleValued(relationType)) {
      boolean owned = MetaDataUtils.isOwnedRelation(mmd, getStoreManager());
      if (owned) {
        // Owned relation, so 1-1_uni/1-1_bi store the relation in the property, and all others at other side
        DatastoreTable dt = getDatastoreTable();
        JavaTypeMapping mapping = dt.getMemberMappingInDatastoreClass(mmd);
        if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.ONE_TO_ONE_UNI) {
          // Even though the mapping is 1 to 1, we model it as a 1 to many and then
          // just throw a runtime exception if we get multiple children.  We would
          // prefer to store the child id on the parent, but we can't because creating
          // a parent and child at the same time involves 3 distinct writes:
          // 1) We put the parent object in order to get a Key.
          // 2) We put the child object, which needs the Key of the parent as
          // the parent of its own Key so that parent and child reside in the
          // same entity group.
          // 3) We re-put the parent object, adding the Key of the child object
          // as a property on the parent.
          // The problem is that the datastore does not support multiple writes
          // to the same entity within a single transaction, so there's no way
          // to perform this sequence of events atomically, and that's a problem.

          // We have 2 scenarios here.  The first is that we're loading the parent
          // side of a 1 to 1 and we want the child.  In that scenario we're going
          // to issue a parent query against the child table with the expectation
          // that there is either 1 result or 0.

          // The second scearnio is that we're loading the child side of a
          // bidirectional 1 to 1 and we want the parent.  In that scenario
          // the key of the parent is part of the child's key so we can just
          // issue a fetch using the parent's key.
          if (dt.isParentKeyProvider(mmd)) {
            // bidir 1 to 1 and we are the child
            return lookupParent(mmd, mapping, false);
          } else {
            // bidir 1 to 1 and we are the parent
            return lookupOneToOneChild(mmd, clr);
          }
        } else if (relationType == RelationType.MANY_TO_ONE_BI) {
          // Get owner via parent key of this object
          // Do not complain about a non existing parent if we have a self referencing relation 
          // and are on the top of the hierarchy.
          MetaData other = mmd.getRelatedMemberMetaData(clr)[0].getParent();
          MetaData parent = mmd.getParent();
          boolean allowNullParent = (other == parent && datastoreEntity.getKey().getParent() == null);
          return lookupParent(mmd, mapping, allowNullParent);
        }
      } else {
        // Unowned relation, so get related object from the property
        return lookupOneToOneChild(mmd, clr);
      }
    }
    return value;
  }

  private Object lookupParent(AbstractMemberMetaData mmd, JavaTypeMapping mapping, boolean allowNullParent) {
    Key parentKey = datastoreEntity.getParent();
    if (parentKey == null) {
      if (!allowNullParent) {
        String childClass = getObjectProvider().getClassMetaData().getFullClassName();
        throw new NucleusFatalUserException("Field " + mmd.getFullFieldName() + " should be able to "
            + "provide a reference to its parent but the entity does not have a parent.  "
            + "Did you perhaps try to establish an instance of " + childClass  +  " as "
            + "the child of an instance of " + mmd.getTypeName() + " after the child had already been "
            + "persisted?");
      } else {
        return null;
      }
    }

    return mapping.getObject(ec, parentKey, NOT_USED);
  }

  private Object lookupOneToOneChild(AbstractMemberMetaData mmd, ClassLoaderResolver clr) {
    AbstractClassMetaData childCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
    String kind = getStoreManager().getIdentifierFactory().newDatastoreContainerIdentifier(childCmd).getIdentifierName();
    if (MetaDataUtils.readRelatedKeysFromParent(getStoreManager(), mmd)) {
      // Use the child key stored in the parent (if present)
      String propName = getPropertyNameForMember(mmd);
      if (datastoreEntity.hasProperty(propName)) {
        Object value = datastoreEntity.getProperty(propName);
        if (value == null) {
          return null;
        }

        if (value instanceof Key) {
          DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
          DatastoreService datastoreService = DatastoreServiceFactoryInternal.getDatastoreService(config);
          try {
            return EntityUtils.entityToPojo(datastoreService.get((Key)value), childCmd, clr, ec, false, ec.getFetchPlan());
          } catch (EntityNotFoundException enfe) {
            // TODO: Should this throw a data integrity exception? It seems to for 1-N.
            NucleusLogger.PERSISTENCE.error("Member " + mmd.getFullFieldName() + " of " + getObjectProvider().getInternalObjectId() +
                " was pointing to object with key " + value + " but this doesn't exist! Returning null");
            return null;
          }
        }
      } else if (MetaDataUtils.isOwnedRelation(mmd, getStoreManager())) {
          // Not yet got the property in the parent, so this entity has not yet been migrated to latest storage version
          NucleusLogger.PERSISTENCE.info("Persistable object at member " + mmd.getFullFieldName() + " of " + getObjectProvider() +
          " not yet migrated to latest storage version, so reading the object via its parent key");
      } else {
        // Unowned relation but we don't have the property! Why? Maybe was originally uni but changed to bi?
        NucleusLogger.PERSISTENCE.error("Object " + datastoreEntity.getKey() + " has unowned member " + mmd.getFullFieldName() +
          " but no corresponding property " + propName + " on its datastore entity, so returning null");
        return null;
      }
    }

    // Owned 1-1, so find all entities with this as a parent. There ought to be only 1 (limitation of early GAE)
    Entity parentEntity = datastoreEntity;
    Query q = new Query(kind, parentEntity.getKey());
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    DatastoreService datastoreService = DatastoreServiceFactoryInternal.getDatastoreService(config);
    // We have to pull back all children because the datastore does not let us filter ancestors by
    // depth and an indirect child could come back before a direct child.  eg: a/b/c,  a/c
    for (Entity e : datastoreService.prepare(q).asIterable()) {
      if (parentEntity.getKey().equals(e.getKey().getParent())) {
        return EntityUtils.entityToPojo(e, childCmd, clr, ec, false, ec.getFetchPlan());
        // We are potentially ignoring data errors where there is more than one
        // direct child for the one to one.  Unfortunately, in order to detect
        // this we need to read all the way to the end of the Iterable and that
        // might pull back a lot more data than is really necessary.
      }
    }
    return null;
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
    AbstractMemberMetaData mmd = getMetaData(fieldNumber);
    String propertyName = getPropertyNameForMember(mmd);
    final String msg = String.format(ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT,
        datastoreEntity.getKind(), datastoreEntity.getKey(), propertyName,
        mmd.getFullFieldName());
    throw new NullPointerException(msg);
  }

  /**
   * Convenience method to convert a datastore value to a Collection.
   * Converts the datastore List<Key> into a collection.
   * @param mmd Metadata for the collection field
   * @param ec Execution Context
   * @param clr ClassLoader resolver
   * @param propName Property name in the Entity storing this value
   * @return The datastore object
   */
  protected Collection getCollectionFromDatastoreObject(AbstractMemberMetaData mmd, 
      ExecutionContext ec, ClassLoaderResolver clr, String propName) {

    // Fields of type Collection<PC>
    Object propValue = datastoreEntity.getProperty(propName);
    if (propValue != null) {
      Collection<Object> coll;
      try {
        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
        coll = (Collection<Object>) instanceType.newInstance();
      } catch (Exception e) {
        throw new NucleusDataStoreException(e.getMessage(), e);
      }

      // Retrieve all Entities in one call
      DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
      DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);
      List<Key> keys = (List<Key>)propValue;
      Map<Key, Entity> entitiesByKey = ds.get(keys);

      boolean changeDetected = false;
      AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr, ec.getMetaDataManager());
      for (Key key : keys) {
        Entity entity = entitiesByKey.get(key);
        if (entity == null) {
          // User must have deleted it? Ignore the entry
          changeDetected = true;
          NucleusLogger.DATASTORE_RETRIEVE.info("Field " + mmd.getFullFieldName() + " of " + datastoreEntity.getKey() +
              " was marked as having child " + key + " but doesn't exist, so must have been deleted. Ignoring");
          continue;
        }

        Object pojo = EntityUtils.entityToPojo(entity, elemCmd, clr, ec, false, ec.getFetchPlan());
        coll.add(pojo);
      }

      if (mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getOrdering() != null &&
          !mmd.getOrderMetaData().getOrdering().equals("#PK")) {
        // Reorder the collection as per the ordering clause (DN 3.0.10+)
        coll = QueryUtils.orderCandidates((List)coll, mmd.getType(), mmd.getOrderMetaData().getOrdering(), ec, clr);
      }

      coll = (Collection)getObjectProvider().wrapSCOField(mmd.getAbsoluteFieldNumber(), coll, false, false, false);
      if (changeDetected) {
        getObjectProvider().makeDirty(mmd.getAbsoluteFieldNumber());
      }
      return coll;
    }

    return null;
  }

  /**
   * Convenience method to convert a datastore value to an array.
   * Converts the datastore List into an array.
   * @param mmd Metadata for the array field
   * @param ec Execution Context
   * @param clr ClassLoader resolver
   * @param propName Property name in the Entity storing this value
   * @return The datastore object
   */
  protected Object getArrayFromDatastoreObject(AbstractMemberMetaData mmd,
      ExecutionContext ec, ClassLoaderResolver clr, String propName) {

    // Note this is stored as a List with elements elem1,elem2,elem3, etc.
    Object propValue = datastoreEntity.getProperty(propName);
    if (propValue != null) {
      List<Key> keys = (List<Key>)propValue;
      Object value = Array.newInstance(mmd.getType().getComponentType(), keys.size());

      // Retrieve all Entities in one call
      DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
      DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);
      Map<Key, Entity> entitiesByKey = ds.get(keys);

      AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr, ec.getMetaDataManager());
      int i = 0;
      boolean changeDetected = false;
      for (Key key : keys) {
        Entity entity = entitiesByKey.get(key);
        if (entity == null) {
          // User must have deleted it? Ignore the entry
          changeDetected = true;
          NucleusLogger.DATASTORE_RETRIEVE.info("Field " + mmd.getFullFieldName() + " of " + datastoreEntity.getKey() +
              " was marked as having child " + key + " but doesn't exist, so must have been deleted. Ignoring");
          continue;
        }

        Object pojo = EntityUtils.entityToPojo(entity, elemCmd, clr, ec, false, ec.getFetchPlan());
        Array.set(value, i, pojo);
        i++;
      }
      if (changeDetected) {
        getObjectProvider().makeDirty(mmd.getAbsoluteFieldNumber());
      }
      return value;
    }
    return null;
  }

  /**
   * Convenience method to convert a datastore value to a Map.
   * Converts the datastore List into a Map.
   * @param mmd Metadata for the map field
   * @param ec Execution Context
   * @param clr ClassLoader resolver
   * @param propName Property name in the Entity storing this value
   * @return The datastore object
   */
  protected Map getMapFromDatastoreObject(AbstractMemberMetaData mmd,
      ExecutionContext ec, ClassLoaderResolver clr, String propName) {
    // Note this is stored as a List with elements key1,val1,key2,val2, etc.
    Object propValue = datastoreEntity.getProperty(propName);
    List keysValues = (List)propValue;
    if (propValue != null) {
      Map map = null;
      try {
        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), null);
        map = (Map) instanceType.newInstance();
      } catch (Exception e) {
        throw new NucleusDataStoreException(e.getMessage(), e);
      }

      // Find all use of Key and retrieve all Entities in one call
      AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
      AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());
      Iterator keyValIter = keysValues.iterator();
      List<Key> keysToRetrieve = new ArrayList<Key>();
      while (keyValIter.hasNext()) {
        Object key = keyValIter.next();
        if (keyCmd != null) {
          keysToRetrieve.add((Key)key);
        }

        Object val = keyValIter.next();
        if (valCmd != null) {
          keysToRetrieve.add((Key)val);
        }
      }
      DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
      DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);
      Map<Key, Entity> entitiesByKey = ds.get(keysToRetrieve);

      keyValIter = keysValues.iterator();
      boolean changeDetected = false;
      while (keyValIter.hasNext()) {
        Object key = keyValIter.next();
        if (keyCmd != null) {
          // Persistable key
          Entity entity = entitiesByKey.get(key);
          if (entity == null) {
            // User must have deleted it? Ignore the entry
            changeDetected = true;
            NucleusLogger.DATASTORE_RETRIEVE.info("Field " + mmd.getFullFieldName() + " of " + datastoreEntity.getKey() +
                " has a map referring to key=" + key + " but doesn't exist, so must have been deleted. Ignoring");
            continue;
          }
          key = EntityUtils.entityToPojo(entity, keyCmd, clr, ec, false, ec.getFetchPlan());
        } else {
          // TODO Make use of TypeConversionUtils for non-PC types
        }

        Object val = keyValIter.next();
        if (valCmd != null) {
          // Persistable value
          Entity entity = entitiesByKey.get(val);
          if (entity == null) {
            // User must have deleted it? Ignore the entry
            changeDetected = true;
            NucleusLogger.DATASTORE_RETRIEVE.info("Field " + mmd.getFullFieldName() + " of " + datastoreEntity.getKey() +
                " has a map referring to value=" + val + " but doesn't exist, so must have been deleted. Ignoring");
            continue;
          }
          val = EntityUtils.entityToPojo(entity, valCmd, clr, ec, false, ec.getFetchPlan());
        } else {
          // TODO Make use of TypeConversionUtils for non-PC types
        }

        map.put(key, val);
      }

      map = (Map)getObjectProvider().wrapSCOField(mmd.getAbsoluteFieldNumber(), map, false, changeDetected, false);
      if (changeDetected) {
        getObjectProvider().makeDirty(mmd.getAbsoluteFieldNumber());
      }
      return map;
    }
    return null;
  }
}