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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.sco.SCO;

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
import com.google.appengine.datanucleus.mapping.InsertMappingConsumer;

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
      return fetchStringPKField(fieldNumber);
    } else if (MetaDataUtils.isParentPKField(getClassMetaData(), fieldNumber)) {
      return fetchParentStringPKField(fieldNumber);
    } else if (MetaDataUtils.isPKNameField(getClassMetaData(), fieldNumber)) {
      AbstractMemberMetaData mmd = getMetaData(fieldNumber);
      if (!mmd.getType().equals(String.class)) {
        throw new NucleusFatalUserException(
            "Field with \"" + DatastoreManager.PK_NAME + "\" extension must be of type String");
      }
      return fetchPKNameField();
    }

    Object fieldVal = fetchFieldFromEntity(fieldNumber);
    if (fieldVal instanceof Text) {
      // must be a lob field
      fieldVal = ((Text) fieldVal).getValue();
    }
    return (String) fieldVal;
  }

  public Object fetchObjectField(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    if (ammd.getEmbeddedMetaData() != null) {
      return fetchEmbeddedField(ammd, fieldNumber);
    } else if (ammd.getRelationType(getClassLoaderResolver()) != Relation.NONE && !ammd.isSerialized()) {
      return fetchRelationField(getClassLoaderResolver(), ammd);
    } else {
      return fetchFieldFromEntity(fieldNumber);
    }
  }

  Object fetchFieldFromEntity(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    if (isPK(fieldNumber)) {
      if (ammd.getType().equals(Key.class)) {
        // If this is a pk field, transform the Key into its String representation.
        return datastoreEntity.getKey();
      } else if(ammd.getType().equals(Long.class)) {
        return datastoreEntity.getKey().getId();
      }
      throw exceptionForUnexpectedKeyType("Primary key", fieldNumber);
    } else if (MetaDataUtils.isParentPKField(getClassMetaData(), fieldNumber)) {
      if (ammd.getType().equals(Key.class)) {
        return datastoreEntity.getKey().getParent();
      }
      throw exceptionForUnexpectedKeyType("Parent key", fieldNumber);
    } else if (MetaDataUtils.isPKIdField(getClassMetaData(), fieldNumber)) {
      return fetchPKIdField();
    } else {
      Object value = datastoreEntity.getProperty(getPropertyName(fieldNumber));
      ClassLoaderResolver clr = getClassLoaderResolver();
      if (ammd.isSerialized()) {
        if (value != null) {
          // If the field is serialized we know it's a Blob that we can deserialize without any conversion necessary.
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

        // Perform any conversions from the stored-type to the field type
        TypeManager typeMgr = op.getExecutionContext().getNucleusContext().getTypeManager();
        value = getConversionUtils().datastoreValueToPojoValue(typeMgr, clr, value, ammd);

        if (value != null && !(value instanceof SCO)) {
          value = getObjectProvider().wrapSCOField(fieldNumber, value, false, false, true);
        }
      }
      return value;
    }
  }

  Object fetchRelationField(ClassLoaderResolver clr, AbstractMemberMetaData ammd) {
    DatastoreTable dt = getDatastoreTable();
    JavaTypeMapping mapping = dt.getMemberMappingInDatastoreClass(ammd);
    // Based on ResultSetGetter
    Object value;
    if (mapping instanceof EmbeddedPCMapping ||
        mapping instanceof SerialisedPCMapping ||
        mapping instanceof SerialisedReferenceMapping) {
      value = mapping.getObject(getExecutionContext(), datastoreEntity,
          NOT_USED, getObjectProvider(), ammd.getAbsoluteFieldNumber());
    } else {
      int relationType = ammd.getRelationType(clr);
      if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) {
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
        DatastoreTable table = getDatastoreTable();
        if (table.isParentKeyProvider(ammd)) {
          // bidir 1 to 1 and we are the child
          value = lookupParent(ammd, mapping, false);
        } else {
          // bidir 1 to 1 and we are the parent
          value = lookupOneToOneChild(ammd, clr);
        }
      } else if (relationType == Relation.MANY_TO_ONE_BI) {
        // Do not complain about a non existing parent if we have a self referencing relation 
        // and are on the top of the hierarchy.
        MetaData other = ammd.getRelatedMemberMetaData(clr)[0].getParent();
        MetaData parent = ammd.getParent();
        boolean allowNullParent =
            other == parent && datastoreEntity.getKey().getParent() == null;
        value = lookupParent(ammd, mapping, allowNullParent);
      } else {
        value = null;
      }
    }
    // Return the field value (as a wrapper if wrappable)
    return getObjectProvider().wrapSCOField(ammd.getAbsoluteFieldNumber(), value, false, false, false);
  }

  private Object lookupParent(AbstractMemberMetaData ammd, JavaTypeMapping mapping, boolean allowNullParent) {
    Key parentKey = datastoreEntity.getParent();
    if (parentKey == null) {
      if (!allowNullParent) {
        String childClass = getObjectProvider().getClassMetaData().getFullClassName();
        throw new NucleusFatalUserException("Field " + ammd.getFullFieldName() + " should be able to "
            + "provide a reference to its parent but the entity does not have a parent.  "
            + "Did you perhaps try to establish an instance of " + childClass  +  " as "
            + "the child of an instance of " + ammd.getTypeName() + " after the child had already been "
            + "persisted?");
      } else {
        return null;
      }
    }

    return mapping.getObject(getObjectProvider().getExecutionContext(), parentKey, NOT_USED);
  }

  private Object lookupOneToOneChild(AbstractMemberMetaData ammd, ClassLoaderResolver clr) {
    ExecutionContext ec = getObjectProvider().getExecutionContext();
    AbstractClassMetaData childCmd = ec.getMetaDataManager().getMetaDataForClass(ammd.getType(), clr);
    String kind = getStoreManager().getIdentifierFactory().newDatastoreContainerIdentifier(childCmd).getIdentifierName();
    if (getStoreManager().storageVersionAtLeast(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS)) {
      // Use the child key stored in the parent (if present)
      String propName = EntityUtils.getPropertyName(getStoreManager().getIdentifierFactory(), ammd);
      if (datastoreEntity.hasProperty(propName)) {
        Object value = datastoreEntity.getProperty(propName);
        if (value instanceof Key) {
          DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
          DatastoreService datastoreService = DatastoreServiceFactoryInternal.getDatastoreService(config);
          try {
            Entity childEntity = datastoreService.get((Key)value);
            return EntityUtils.entityToPojo(childEntity, childCmd, clr, ec, false, ec.getFetchPlan());
          } catch (EntityNotFoundException enfe) {
            // TODO Handle child key pointing to non-existent Entity
          }
        }
      }
    }

    // We're going to issue a query for all entities of the given kind with
    // the parent entity's key as their parent.  There should be only 1.
    Entity parentEntity = datastoreEntity;
    Query q = new Query(kind, parentEntity.getKey());
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    DatastoreService datastoreService = DatastoreServiceFactoryInternal.getDatastoreService(config);
    // We have to pull back all children because the datastore does not let us
    // filter ancestors by depth and an indirect child could come back before a
    // direct child.  eg:
    // a/b/c
    // a/c
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
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    String propertyName = getPropertyName(fieldNumber);
    final String msg = String.format(ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT,
        datastoreEntity.getKind(), datastoreEntity.getKey(), propertyName,
        ammd.getFullFieldName());
    throw new NullPointerException(msg);
  }

  /**
   * We can't trust the fieldNumber on the ammd provided because some embedded
   * fields don't have this set.  That's why we pass it in as a separate param.
   */
  private Object fetchEmbeddedField(AbstractMemberMetaData ammd, int fieldNumber) {
    ObjectProvider eop = getEmbeddedObjectProvider(ammd, fieldNumber, null);
    // We need to build a mapping consumer for the embedded class so that we get correct 
    // fieldIndex --> metadata mappings for the class in the proper embedded context
    // TODO(maxr) Consider caching this
    InsertMappingConsumer mappingConsumer = buildMappingConsumer(
        eop.getClassMetaData(), getClassLoaderResolver(),
        eop.getClassMetaData().getAllMemberPositions(),
        ammd.getEmbeddedMetaData());
    // TODO Create own FieldManager instead of reusing this one
    AbstractMemberMetaDataProvider ammdProvider = getEmbeddedAbstractMemberMetaDataProvider(mappingConsumer);
    fieldManagerStateStack.addFirst(new FieldManagerState(eop, ammdProvider, mappingConsumer, true));
    try {
      AbstractClassMetaData acmd = eop.getClassMetaData();
      eop.replaceFields(acmd.getAllMemberPositions(), this);

      if (ammd.getEmbeddedMetaData() != null && ammd.getEmbeddedMetaData().getNullIndicatorColumn() != null) {
        String nullColumn = ammd.getEmbeddedMetaData().getNullIndicatorColumn();
        String nullValue = ammd.getEmbeddedMetaData().getNullIndicatorValue();
        AbstractMemberMetaData[] embMmds = ammd.getEmbeddedMetaData().getMemberMetaData();
        AbstractMemberMetaData nullMmd = null;
        for (int i=0;i<embMmds.length;i++) {
          ColumnMetaData[] colmds = embMmds[i].getColumnMetaData();
          if (colmds != null && colmds[0].getName() != null && colmds[0].getName().equals(nullColumn)) {
            nullMmd = embMmds[i];
            break;
          }
        }
        if (nullMmd != null) {
          int nullFieldPos = eop.getClassMetaData().getAbsolutePositionOfMember(nullMmd.getName());
          Object val = eop.provideField(nullFieldPos);
          if (val == null && nullValue == null) {
            return null;
          } else if (val != null && nullValue != null && val.equals(nullValue)) {
            return null;
          }
        }
        return eop.getObject();
      }
      else {
        return eop.getObject();
      }
    } finally {
      fieldManagerStateStack.removeFirst();
    }
  }

  private Object deserializeFieldValue(
      Object value, ClassLoaderResolver clr, AbstractMemberMetaData ammd) {
    if (!(value instanceof Blob)) {
      throw new NucleusException(
          "Datastore value is of type " + value.getClass().getName() + " (must be Blob).").setFatal();
    }
    return getStoreManager().getSerializationManager().deserialize(clr, ammd, (Blob) value);
  }

  private String fetchPKNameField() {
    Key key = datastoreEntity.getKey();
    if (key.getName() == null) {
      throw new NucleusFatalUserException(
          "Attempting to fetch field with \"" + DatastoreManager.PK_NAME + "\" extension but the "
          + "entity is identified by an id, not a name.");
    }
    return datastoreEntity.getKey().getName();
  }

  private long fetchPKIdField() {
    Key key = datastoreEntity.getKey();
    if (key.getName() != null) {
      throw new NucleusFatalUserException(
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
    if (MetaDataUtils.isEncodedPKField(getClassMetaData(), fieldNumber)) {
      // If this is an encoded pk field, transform the Key into its String
      // representation.
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
  }

  protected String getPropertyName(int fieldNumber) {
    AbstractMemberMetaData ammd = getMetaData(fieldNumber);
    return EntityUtils.getPropertyName(getStoreManager().getIdentifierFactory(), ammd);
  }
}