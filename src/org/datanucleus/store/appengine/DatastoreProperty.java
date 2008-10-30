// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.DatastoreIdentifier;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.DatastoreMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;

/**
 * Describes a property in the datastore.
 *
 * Mostly copied from Column.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreProperty implements DatastoreField {

  /** Identifier for the column in the datastore. */
  private DatastoreIdentifier identifier;

  /** ColumnMetaData for this column. */
  private final ColumnMetaData columnMetaData;

  /** Table containing this column in the datastore. */
  private final DatastoreContainerObject table;

  /** Datastore mapping for this column. */
  private DatastoreMapping datastoreMapping = null;

  /** Java type that this column is storing. (can we just get this from the mapping above ?) */
  private final String storedJavaType;

  /** Manager for the store into which we are persisting. */
  private final MappedStoreManager storeMgr;

  /** Flag indicated whether or not this is a pk */
  private boolean isPrimaryKey;

  public DatastoreProperty(DatastoreContainerObject table, String javaType,
      DatastoreIdentifier identifier, ColumnMetaData colmd) {
    this.table = table;
    this.storedJavaType = javaType;
    this.storeMgr = table.getStoreManager();

    setIdentifier(identifier);
    if (colmd == null) {
      // Create a default ColumnMetaData since none provided
      columnMetaData = new ColumnMetaData(null, (String) null);
    } else {
      columnMetaData = colmd;
    }

    // Uniqueness
    if (columnMetaData.getUnique()) {
      // MetaData requires it to be unique
      throw new UnsupportedOperationException("No support for uniqueness constraints");
    }
  }

  public String getStoredJavaType() {
    return storedJavaType;
  }

  public void setAsPrimaryKey() {
    isPrimaryKey = true;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public boolean isNullable() {
    // all properties all nullable
    return true;
  }

  public DatastoreMapping getDatastoreMapping() {
    return datastoreMapping;
  }

  public void setDatastoreMapping(DatastoreMapping mapping) {
    this.datastoreMapping = mapping;
  }

  public JavaTypeMapping getMapping() {
    return datastoreMapping.getJavaTypeMapping();
  }

  public DatastoreContainerObject getDatastoreContainerObject() {
    return table;
  }

  public String applySelectFunction(String replacementValue) {
    throw new UnsupportedOperationException("Select function not supported");
  }

  public void copyConfigurationTo(DatastoreField field) {
    DatastoreProperty prop = (DatastoreProperty) field;
    prop.isPrimaryKey = this.isPrimaryKey;
  }

  public DatastoreField setNullable() {
    // all properties are nullable
    return this;
  }

  public DatastoreField setDefaultable() {
    throw new UnsupportedOperationException("Default values not supported");
  }

  public void setIdentifier(DatastoreIdentifier identifier) {
    this.identifier = identifier;
  }

  public MetaData getMetaData() {
    return columnMetaData;
  }

  public void setMetaData(MetaData md) {
    if (md == null) {
      // Nothing to do since no definition of requirements
      return;
    }

    ColumnMetaData colmd = (ColumnMetaData) md;
    if (colmd.getName() != null) {
      columnMetaData.setName(colmd.getName());
    }

    // other properties of columnMetaData don't apply to us
  }

  public AbstractMemberMetaData getFieldMetaData() {
    if (columnMetaData != null && columnMetaData.getParent() instanceof AbstractMemberMetaData) {
      return (AbstractMemberMetaData)columnMetaData.getParent();
    }
    return null;
  }

  public MappedStoreManager getStoreManager() {
    return storeMgr;
  }

  public DatastoreIdentifier getIdentifier() {
    return identifier;
  }
}
