// Copyright 2008 Google Inc. All Rightss Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;

/**
 * Represents a mapping for the datastore.  We throw
 * {@link UnsupportedOperationException} for quite a few of these operations
 * because we expect this class to only be used for storing pks.  Pks are
 * guaranteed to be of type String or {@link Key}.  We deny service for
 * everything else.
 *
 * I'd prefer not to export this class but Datanucleus requires it be public.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreMapping implements org.datanucleus.store.mapped.mapping.DatastoreMapping {

  private final JavaTypeMapping mapping;
  private final MappedStoreManager storeMgr;
  private final DatastoreField field;

  public DatastoreMapping(JavaTypeMapping mapping, MappedStoreManager storeMgr, DatastoreField field) {
    this.mapping = mapping;
    this.storeMgr = storeMgr;
    this.field = field;
    if (mapping != null) {
      // Register this datastore mapping with the owning JavaTypeMapping
      mapping.addDataStoreMapping(this);
    }
  }

  public boolean isNullable() {
    // all fields are nullable
    return true;
  }

  public DatastoreField getDatastoreField() {
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
    setObject(datastoreEntity, paramIndex, KeyFactory.decodeKey(value));
  }

  public void setObject(Object datastoreEntity, int paramIndex, Object value) {
    AbstractMemberMetaData ammd = field.getFieldMetaData();
    String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ammd);
    ((Entity) datastoreEntity).setProperty(propName, value);
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

  public Object getObject(Object resultSet, int exprIndex) {
    throw new UnsupportedOperationException("Should only be using this for keys.");
  }
}
