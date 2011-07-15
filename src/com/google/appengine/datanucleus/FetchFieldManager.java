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
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ObjectProvider;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;

/**
 * FieldManager to handle the fetching of fields from an Entity into a managed object.
 */
public class FetchFieldManager extends DatastoreFieldManager
{
    private static final String ILLEGAL_NULL_ASSIGNMENT_ERROR_FORMAT =
        "Datastore entity with kind %s and key %s has a null property named %s.  This property is "
            + "mapped to %s, which cannot accept null values.";

    /**
     * @param op ObjectProvider of the object being fetched
     * @param storeManager StoreManager for this object
     * @param datastoreEntity The Entity to extract the results from
     * @param fieldNumbers The field numbers being extracted
     */
    public FetchFieldManager(ObjectProvider op, DatastoreManager storeManager, Entity datastoreEntity, 
            int[] fieldNumbers)
    {
        super(op, storeManager, datastoreEntity, fieldNumbers);
    }

    /**
     * @param op ObjectProvider for the object being fetched
     * @param storeManager StoreManager for this object
     * @param datastoreEntity The Entity to extract results from
     */
    public FetchFieldManager(ObjectProvider op, DatastoreManager storeManager, Entity datastoreEntity)
    {
        super(op, storeManager, datastoreEntity);
    }

    public boolean fetchBooleanField(int fieldNumber) {
      return (Boolean) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public byte fetchByteField(int fieldNumber) {
      return (Byte) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public char fetchCharField(int fieldNumber) {
      return (Character) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public double fetchDoubleField(int fieldNumber) {
      return (Double) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public float fetchFloatField(int fieldNumber) {
      return (Float) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public int fetchIntField(int fieldNumber) {
      return (Integer) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public long fetchLongField(int fieldNumber) {
      return (Long) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public short fetchShortField(int fieldNumber) {
      return (Short) checkAssignmentToNotNullField(fetchObjectField(fieldNumber), fieldNumber);
    }

    public String fetchStringField(int fieldNumber) {
      if (isPK(fieldNumber)) {
        return fetchStringPKField(fieldNumber);
      } else if (isParentPK(fieldNumber)) {
        return fetchParentStringPKField(fieldNumber);
      } else if (isPKNameField(fieldNumber)) {
        if (!fieldIsOfTypeString(fieldNumber)) {
          throw new NucleusFatalUserException(
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

          // Perform any conversions from the stored-type to the field type
          value = getConversionUtils().datastoreValueToPojoValue(clr, value, getObjectProvider(), ammd);
        }
        return value;
      }
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
      // We need to build a mapping consumer for the embedded class so that we
      // get correct fieldIndex --> metadata mappings for the class in the proper
      // embedded context
      // TODO(maxr) Consider caching this
      InsertMappingConsumer mappingConsumer = buildMappingConsumer(
          eop.getClassMetaData(), getClassLoaderResolver(),
          eop.getClassMetaData().getAllMemberPositions(),
          ammd.getEmbeddedMetaData());
      AbstractMemberMetaDataProvider ammdProvider = getEmbeddedAbstractMemberMetaDataProvider(mappingConsumer);
      fieldManagerStateStack.addFirst(new FieldManagerState(eop, ammdProvider, mappingConsumer, true));
      AbstractClassMetaData acmd = eop.getClassMetaData();
      eop.replaceFields(acmd.getAllMemberPositions(), this);
      fieldManagerStateStack.removeFirst();
      return eop.getObject();
    }

    private Object deserializeFieldValue(
        Object value, ClassLoaderResolver clr, AbstractMemberMetaData ammd) {
      if (!(value instanceof Blob)) {
        throw new NucleusException(
            "Datastore value is of type " + value.getClass().getName() + " (must be Blob).").setFatal();
      }
      return storeManager.getSerializationManager().deserialize(clr, ammd, (Blob) value);
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
      if (DatastoreManager.isEncodedPKField(getClassMetaData(), fieldNumber)) {
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
      return EntityUtils.getPropertyName(storeManager.getIdentifierFactory(), ammd);
    }
}