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
package com.google.appengine.datanucleus.mapping;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastoreRelationFieldManager;
import com.google.appengine.datanucleus.EntityUtils;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;

/**
 * Represents a mapping for the datastore.  We throw
 * {@link UnsupportedOperationException} for quite a few of these operations
 * because this class should only be used for storing foreign keys.  These are
 * guaranteed to be of type String or {@link Key}.  We deny service for
 * everything else.
 *
 * I'd prefer not to export this class but Datanucleus requires it be public.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFKMapping implements org.datanucleus.store.mapped.mapping.DatastoreMapping {

  private final JavaTypeMapping mapping;
  private final DatastoreManager storeMgr;
  private final DatastoreProperty field;

  public DatastoreFKMapping(JavaTypeMapping mapping, MappedStoreManager storeMgr, DatastoreField field) {
    this.mapping = mapping;
    this.storeMgr = (DatastoreManager) storeMgr;
    this.field = (DatastoreProperty) field;
    if (mapping != null) {
      // Register this datastore mapping with the owning JavaTypeMapping
      mapping.addDatastoreMapping(this);
    }
  }

  public boolean isNullable() {
    // all fks are currently not nullable
    return false;
  }

  public DatastoreProperty getDatastoreField() {
    return field;
  }

  public JavaTypeMapping getJavaTypeMapping() {
    return mapping;
  }

  // TODO(maxr): Figure out what we should be returning for these methods
  public boolean isDecimalBased() {
    return false;
  }

  public boolean isIntegerBased() {
    return false;
  }

  public boolean isStringBased() {
    return false;
  }

  public boolean isBitBased() {
    return false;
  }

  public boolean isBooleanBased() {
    return false;
  }


  public void setBoolean(Object datastoreEntity, int paramIndex, boolean value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setChar(Object datastoreEntity, int paramIndex, char value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setByte(Object datastoreEntity, int paramIndex, byte value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setShort(Object datastoreEntity, int paramIndex, short value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setInt(Object datastoreEntity, int paramIndex, int value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setLong(Object datastoreEntity, int paramIndex, long value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setFloat(Object datastoreEntity, int paramIndex, float value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setDouble(Object datastoreEntity, int paramIndex, double value) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public void setString(Object datastoreEntity, int paramIndex, String value) {
    setObject(datastoreEntity, paramIndex, value);
  }

  public void setObject(Object datastoreEntity, int paramIndex, Object value) {
    // This is awful.  In the case of a bidirectional one-to-many, the pk of the
    // child object needs to have the pk of the parent object as its parent.
    // We can get the pk of the parent object from the parent instance that
    // is set on the child, but since you can only specify a parent key
    // when you create an Entity, we need to rebuild the Entity with this
    // new key.  There's no easy way to rebuild the Entity down in this function,
    // so we instead set a magic property on the entity whose value is the parent
    // key with the expectation that someone upstream will see it, remove it,
    // and then recreate the entity on our behalf.  Like I said, this is awful.
    if (paramIndex == DatastoreRelationFieldManager.IS_PARENT_VALUE) {
      if (value != null) {
        ((Entity) datastoreEntity).setProperty(
            DatastoreRelationFieldManager.PARENT_KEY_PROPERTY, value);
      }
    } else if (paramIndex != DatastoreRelationFieldManager.IS_FK_VALUE) {
      // Similar madness here.  Most of the time we want to just set the
      // given value on the entity, but if this is a foreign key value we
      // want to just swallow the update.  The reason is that we only
      // maintain fks as parents in the key itself.  The updates we'll
      // swallow are DataNucleus adding "hidden" back pointers to parent
      // objects.  We don't want these.  The back pointer is the parent
      // of the key itself.
      String propName = getPropertyName();
      ((Entity) datastoreEntity).setProperty(propName, value);
    }
  }

  private String getPropertyName() {
    AbstractMemberMetaData ammd = field.getMemberMetaData();
    if (ammd != null) {
      return EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ammd);
    } else {
      return field.getColumnMetaData().getName();
    }
  }

  public boolean getBoolean(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public char getChar(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public byte getByte(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public short getShort(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public int getInt(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public long getLong(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public float getFloat(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public double getDouble(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public String getString(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }

  public Object getObject(Object datastoreEntity, int exprIndex) {
    String propName = getPropertyName();
    return ((Entity) datastoreEntity).getProperty(propName);
  }
}
