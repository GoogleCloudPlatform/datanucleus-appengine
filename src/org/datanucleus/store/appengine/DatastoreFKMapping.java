// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

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
  private final MappedStoreManager storeMgr;
  private final DatastoreProperty field;

  public DatastoreFKMapping(JavaTypeMapping mapping, MappedStoreManager storeMgr, DatastoreField field) {
    this.mapping = mapping;
    this.storeMgr = storeMgr;
    this.field = (DatastoreProperty) field;
    if (mapping != null) {
      // Register this datastore mapping with the owning JavaTypeMapping
      mapping.addDataStoreMapping(this);
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
    // Currently only using the mapping for relations, so we know this
    // value can be converted to a key.
    setObject(datastoreEntity, paramIndex, KeyFactory.stringToKey(value));
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
      AbstractMemberMetaData ammd = field.getMemberMetaData();
      String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ammd);
      ((Entity) datastoreEntity).setProperty(propName, value);
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
    AbstractMemberMetaData ammd = field.getMemberMetaData();
    String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ammd);
    return ((Entity) datastoreEntity).getProperty(propName);
  }
}
