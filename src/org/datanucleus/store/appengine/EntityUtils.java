// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;

import org.datanucleus.ObjectManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;

/**
 * Utility methods for determining entity property names and kinds.
 *
 * @author Max Ross <maxr@google.com>
 */
final class EntityUtils {

  public static String getPropertyName(
      IdentifierFactory idFactory, AbstractMemberMetaData ammd) {
    // TODO(maxr): See if there is a better way than field name comparison to
    // determine if this is a version field
    AbstractClassMetaData acmd = ammd.getAbstractClassMetaData();
    if (acmd.hasVersionStrategy() &&
        ammd.getName().equals(acmd.getVersionMetaData().getFieldName())) {
      return getVersionPropertyName(idFactory, acmd.getVersionMetaData());
    }

    // If a column name was explicitly provided, use that as the property name.
    if (ammd.getColumn() != null) {
      return ammd.getColumn();
    }

    // If we're dealing with embeddables, the column name override
    // will show up as part of the column meta data.
    if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length > 0) {
      if (ammd.getColumnMetaData().length != 1) {
        // TODO(maxr) throw something more appropriate
        throw new UnsupportedOperationException();
      }
      return ammd.getColumnMetaData()[0].getName();
    }
    // Use the IdentifierFactory to convert from the name of the field into
    // a property name.
    return idFactory.newDatastoreFieldIdentifier(ammd.getName()).getIdentifierName();
  }

  public static Object getVersionFromEntity(
      IdentifierFactory idFactory, VersionMetaData vmd, Entity entity) {
    return entity.getProperty(getVersionPropertyName(idFactory, vmd));
  }

  public static String getVersionPropertyName(
      IdentifierFactory idFactory, VersionMetaData vmd) {
    ColumnMetaData[] columnMetaData = vmd.getColumnMetaData();
    if (columnMetaData == null || columnMetaData.length == 0) {
      return idFactory.newVersionFieldIdentifier().getIdentifierName();
    }
    if (columnMetaData.length != 1) {
      throw new IllegalArgumentException(
          "Please specify 0 or 1 column name for the version property.");
    }
    return columnMetaData[0].getName();
  }

  public static String determineKind(AbstractClassMetaData acmd, ObjectManager om) {
    MappedStoreManager storeMgr = (MappedStoreManager) om.getStoreManager();
    return determineKind(acmd, storeMgr.getIdentifierFactory());
  }

  public static String determineKind(AbstractClassMetaData acmd, IdentifierFactory idFactory) {
    if (acmd.getTable() != null) {
      // User specified a table name as part of the mapping so use that as the
      // kind.
      return acmd.getTable();
    }
    // No table name provided so use the identifier factory to convert the
    // class name into the kind.
    return idFactory.newDatastoreContainerIdentifier(acmd).getIdentifierName();
  }

}
