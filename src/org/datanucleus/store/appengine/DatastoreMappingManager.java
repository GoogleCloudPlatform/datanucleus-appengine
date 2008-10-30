// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.DatastoreIdentifier;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.AbstractMappingManager;
import org.datanucleus.store.mapped.mapping.DatastoreMappingFactory;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.rdbms.sqlidentifier.RDBMSIdentifierFactory;

/**
 * MappingManager for the datastore.  Most of this code is taken from
 * RDBMSMappingManager.
 *
 * TODO(maxr): Refactor RDBMSMappingManager so that we can reuse more.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreMappingManager extends AbstractMappingManager {

  public org.datanucleus.store.mapped.mapping.DatastoreMapping createDatastoreMapping(JavaTypeMapping mapping,
      AbstractMemberMetaData fmd, int index, MappedStoreManager srm, DatastoreField prop) {
    return createDatastoreMapping(mapping, srm, prop);
  }

  public org.datanucleus.store.mapped.mapping.DatastoreMapping createDatastoreMapping(
      JavaTypeMapping mapping, MappedStoreManager storeMgr, DatastoreField prop, String javaType) {
    return createDatastoreMapping(mapping, storeMgr, prop);
  }

  private org.datanucleus.store.mapped.mapping.DatastoreMapping createDatastoreMapping(
      JavaTypeMapping mapping, MappedStoreManager srm, DatastoreField prop) {
    // for now we're just usting a single DatastoreMapping impl for everything.
    org.datanucleus.store.mapped.mapping.DatastoreMapping datastoreMapping =
        DatastoreMappingFactory.createMapping(DatastoreMapping.class, mapping, srm, prop);
    if (prop != null) {
      prop.setDatastoreMapping(datastoreMapping);
    }
    return datastoreMapping;
  }

  // Mostly copied from RDBMSMappingManager.createDatastoreField
  public DatastoreField createDatastoreField(JavaTypeMapping mapping, String javaType,
      int datastoreFieldIndex) {
    AbstractMemberMetaData fmd = mapping.getFieldMetaData();
    int roleForField = mapping.getRoleForField();
    DatastoreContainerObject datastoreContainer = mapping.getDatastoreContainer();

    // Take the column MetaData from the component that this mappings role relates to
    ColumnMetaData colmd;
    ColumnMetaDataContainer columnContainer = fmd;
    if (roleForField == JavaTypeMapping.MAPPING_COLLECTION_ELEMENT ||
        roleForField == JavaTypeMapping.MAPPING_ARRAY_ELEMENT) {
      columnContainer = fmd.getElementMetaData();
    } else if (roleForField == JavaTypeMapping.MAPPING_MAP_KEY) {
      columnContainer = fmd.getKeyMetaData();
    } else if (roleForField == JavaTypeMapping.MAPPING_MAP_VALUE) {
      columnContainer = fmd.getValueMetaData();
    }

    DatastoreProperty prop;
    ColumnMetaData[] colmds;
    if (columnContainer != null
        && columnContainer.getColumnMetaData().length > datastoreFieldIndex) {
      colmd = columnContainer.getColumnMetaData()[datastoreFieldIndex];
      colmds = columnContainer.getColumnMetaData();
    } else {
      // If column specified add one (use any column name specified on field element)
      colmd = new ColumnMetaData(fmd, fmd.getColumn());
      if (columnContainer != null) {
        columnContainer.addColumn(colmd);
        colmds = columnContainer.getColumnMetaData();
      } else {
        colmds = new ColumnMetaData[1];
        colmds[0] = colmd;
      }
    }

    // Generate the column identifier
    MappedStoreManager storeMgr = datastoreContainer.getStoreManager();
    IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
    DatastoreIdentifier identifier = null;
    if (colmd.getName() == null) {
      // No name specified, so generate the identifier from the field name
      if (roleForField == JavaTypeMapping.MAPPING_FIELD) {
        identifier = idFactory.newIdentifier(IdentifierFactory.COLUMN, fmd.getName());
        int i = 0;
        while (datastoreContainer.hasDatastoreField(identifier)) {
          identifier = idFactory.newIdentifier(IdentifierFactory.COLUMN, fmd.getName() + "_" + i);
          i++;
        }
      } else if (roleForField == JavaTypeMapping.MAPPING_COLLECTION_ELEMENT) {
        // Join table collection element
        identifier = ((RDBMSIdentifierFactory) idFactory)
            .newJoinTableFieldIdentifier(fmd, null, null,
                true, FieldRole.ROLE_COLLECTION_ELEMENT);
      } else if (roleForField == JavaTypeMapping.MAPPING_ARRAY_ELEMENT) {
        // Join table array element
        identifier = ((RDBMSIdentifierFactory) idFactory)
            .newJoinTableFieldIdentifier(fmd, null, null,
                true, FieldRole.ROLE_ARRAY_ELEMENT);
      } else if (roleForField == JavaTypeMapping.MAPPING_MAP_KEY) {
        // Join table map key
        identifier = ((RDBMSIdentifierFactory) idFactory)
            .newJoinTableFieldIdentifier(fmd, null, null,
                true, FieldRole.ROLE_MAP_KEY);
      } else if (roleForField == JavaTypeMapping.MAPPING_MAP_VALUE) {
        // Join table map value
        identifier = ((RDBMSIdentifierFactory) idFactory)
            .newJoinTableFieldIdentifier(fmd, null, null,
                true, FieldRole.ROLE_MAP_VALUE);
      }

      colmd.setName(identifier.getIdentifier());
    } else {
      // User has specified a name, so try to keep this unmodified
      identifier = idFactory.newDatastoreFieldIdentifier(colmds[datastoreFieldIndex].getName(),
          storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(fmd.getType()),
          FieldRole.ROLE_CUSTOM);
    }

    // Create the column
    prop = (DatastoreProperty) datastoreContainer.addDatastoreField(javaType, identifier, mapping, colmd);

    if (fmd.isPrimaryKey()) {
      prop.setAsPrimaryKey();
    }

//    setDatastoreFieldNullability(fmd, colmd, col);
    if (fmd.getNullValue() == NullValue.DEFAULT) {
      // Users default should be applied if a null is to be inserted
      prop.setDefaultable();
      if (colmd.getDefaultValue() != null) {
        throw new UnsupportedOperationException("User-defined default not supported.");
      }
    }

    return prop;
  }

  public DatastoreField createDatastoreField(JavaTypeMapping mapping, String javaType,
      ColumnMetaData colmd) {
    AbstractMemberMetaData fmd = mapping.getFieldMetaData();
    DatastoreContainerObject datastoreContainer = mapping.getDatastoreContainer();
    MappedStoreManager storeMgr = datastoreContainer.getStoreManager();

    DatastoreField prop;
    if (colmd == null) {
      // If column specified add one (use any column name specified on field element)
      colmd = new ColumnMetaData(fmd, fmd.getColumn());
      fmd.addColumn(colmd);
    }

    IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
    if (colmd.getName() == null) {
      // No name specified, so generate the identifier from the field name
      DatastoreIdentifier identifier = idFactory
          .newIdentifier(IdentifierFactory.COLUMN, fmd.getName());
      int i = 0;
      while (datastoreContainer.hasDatastoreField(identifier)) {
        identifier = idFactory.newIdentifier(IdentifierFactory.COLUMN, fmd.getName() + "_" + i);
        i++;
      }

      colmd.setName(identifier.getIdentifier());
      prop = datastoreContainer.addDatastoreField(javaType, identifier, mapping, colmd);
    } else {
      // User has specified a name, so try to keep this unmodified
      prop = datastoreContainer.addDatastoreField(
          javaType,
          idFactory.newDatastoreFieldIdentifier(colmd.getName(),
          storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(fmd.getType()),
          FieldRole.ROLE_CUSTOM),
          mapping,
          colmd);
    }

//    setDatastoreFieldNullability(fmd, colmd, prop);
    if (fmd.getNullValue() == NullValue.DEFAULT) {
      // Users default should be applied if a null is to be inserted
      prop.setDefaultable();
      if (colmd.getDefaultValue() != null) {
        throw new UnsupportedOperationException("User-defined default not supported.");
      }
    }
    return prop;
  }

  public DatastoreField createDatastoreField(AbstractMemberMetaData fmd,
      DatastoreContainerObject datastoreContainer, JavaTypeMapping mapping, ColumnMetaData colmd,
      DatastoreField reference, ClassLoaderResolver clr) {
    MappedStoreManager storeMgr = datastoreContainer.getStoreManager();
    IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
    DatastoreIdentifier identifier = null;
    if (colmd.getName() == null) {
      // No name specified, so generate the identifier from the field name
      AbstractMemberMetaData[] relatedMmds = fmd.getRelatedMemberMetaData(clr);
      identifier = ((RDBMSIdentifierFactory) idFactory).newForeignKeyFieldIdentifier(
          relatedMmds != null ? relatedMmds[0] : null,
          fmd, reference.getIdentifier(),
          storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(fmd.getType()),
          FieldRole.ROLE_OWNER);
      colmd.setName(identifier.getIdentifier());
    } else {
      // User has specified a name, so try to keep this unmodified
      identifier = idFactory
          .newDatastoreFieldIdentifier(colmd.getName(), false, FieldRole.ROLE_CUSTOM);
    }
    DatastoreField prop = datastoreContainer
        .addDatastoreField(fmd.getType().getName(), identifier, mapping, colmd);

    // Copy the characteristics of the reference column to this one
    reference.copyConfigurationTo(prop);

    if (fmd.isPrimaryKey()) {
      prop.setAsPrimaryKey();
    }

//    if (storeMgr.isStrategyDatastoreAttributed(fmd.getValueStrategy(), false)) {
//      if ((fmd.isPrimaryKey() && ((DatastoreClass) datastoreContainer).isBaseDatastoreClass())
//          || !fmd.isPrimaryKey()) {
//        // Increment any PK field if we are in base class, and increment any other field
//        prop.setAutoIncrement(true);
//      }
//    }

//    setDatastoreFieldNullability(fmd, colmd, prop);
    if (fmd.getNullValue() == NullValue.DEFAULT) {
      // Users default should be applied if a null is to be inserted
      prop.setDefaultable();
      if (colmd.getDefaultValue() != null) {
        throw new UnsupportedOperationException("User-defined default not supported.");
      }
    }
    return prop;
  }

  @Override
  public void loadDatastoreMapping(PluginManager mgr, ClassLoaderResolver clr, String vendorId) {
  }

  @Override
  public void registerDatastoreMapping(String javaTypeName, Class datastoreMappingType,
      String jdbcType, String sqlType, boolean dflt) {
  }
}
