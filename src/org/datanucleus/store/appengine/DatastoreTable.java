// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.sco.SCOUtils;
import org.datanucleus.store.exceptions.NoSuchPersistentFieldException;
import org.datanucleus.store.exceptions.NoTableManagedException;
import org.datanucleus.store.mapped.DatastoreAdapter;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.DatastoreIdentifier;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.mapped.mapping.OIDMapping;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes a 'table' in the datastore.  We don't actually have tables, but
 * we need to represent this logically in order to take care of all the nice
 * mapping logic that datanucleus does for us.
 *
 * This code is largely copied AbstractClassTable, ClassTable, TableImpl,
 * and AbstractTable
 *
 * TODO(maxr): Refactor the RDBMS classes into the mapped package so we can
 * refactor this.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreTable implements DatastoreClass {

  /** Localiser for messages. */
  protected static final Localiser LOCALISER =
      Localiser.getInstance("org.datanucleus.store.appengine.Localisation",
      DatastoreManager.class.getClassLoader());

  private final MappedStoreManager storeMgr;
  private final AbstractClassMetaData cmd;
  private final ClassLoaderResolver clr;
  private final DatastoreAdapter dba;
  protected final DatastoreIdentifier identifier;

  /**
   * Mappings for fields mapped to this table, keyed by the FieldMetaData.
   */
  private final Map<AbstractMemberMetaData, JavaTypeMapping> fieldMappingsMap = Maps.newHashMap();

  /**
   * All the properties in the table.  Even though the datastore is schemaless,
   * the mappings provided by the ORM effectively impose a schema.  This allows
   * us to know, up front, what properties we can expect.
   */
  private final List<DatastoreProperty> datastoreProperties = Lists.newArrayList();

  /**
   * Index to the props, keyed by name.
   */
  protected Map<DatastoreIdentifier, DatastoreProperty> datastorePropertiesByName =
      Maps.newHashMap();

  /**
   * Mapping for datastore identity (optional).
   *
   */
  private JavaTypeMapping datastoreIDMapping;

  /**
   * Mappings for application identity (optional).
   */
  private JavaTypeMapping[] pkMappings;

  /**
   * Mapping for the id of the table.
   */
  private JavaTypeMapping idMapping;

  /**
   * Highest absolute field number managed by this table
   */
  private int highestFieldNumber = 0;

  /**
   * Dependent fields.  Unlike pretty much the rest of this class, this member and the
   * code that populates is specific to the appengine plugin.
   */
  private final List<AbstractMemberMetaData> dependentMemberMetaData = Lists.newArrayList();

  DatastoreTable(MappedStoreManager storeMgr, AbstractClassMetaData cmd,
      ClassLoaderResolver clr, DatastoreAdapter dba) {
    this.storeMgr = storeMgr;
    this.cmd = cmd;
    this.clr = clr;
    this.dba = dba;
    this.identifier = new DatastoreKind(cmd);
  }

  public String getType() {
    return cmd.getFullClassName();
  }

  public IdentityType getIdentityType() {
    return cmd.getIdentityType();
  }

  public boolean isObjectIDDatastoreAttributed() {
    return true;
  }

  public boolean isBaseDatastoreClass() {
    return true;
  }

  public DatastoreClass getBaseDatastoreClassWithField(AbstractMemberMetaData fmd) {
    if (fieldMappingsMap.get(fmd) != null) {
      return this;
    }
    return null;
  }

  public DatastoreClass getSuperDatastoreClass() {
    return null;
  }

  public Collection getSecondaryDatastoreClasses() {
    return null;
  }

  public boolean managesClass(String className) {
    return cmd.getFullClassName().equals(className);
  }

  public JavaTypeMapping getDataStoreObjectIdMapping() {
    return datastoreIDMapping;
  }

  public JavaTypeMapping getFieldMapping(String fieldName) {
    AbstractMemberMetaData fmd = getFieldMetaData(fieldName);
    JavaTypeMapping m = getFieldMapping(fmd);
    if (m == null) {
        throw new NoSuchPersistentFieldException(cmd.getFullClassName(), fieldName);
    }
    return m;
  }

  private AbstractMemberMetaData getFieldMetaData(String fieldName) {
    return cmd.getMetaDataForMember(fieldName);
  }

  public JavaTypeMapping getFieldMapping(AbstractMemberMetaData mmd) {
    if (mmd == null) {
      return null;
    }

    // Check if we manage this field
    JavaTypeMapping m = fieldMappingsMap.get(mmd);
    if (m != null) {
        return m;
    }
    return null;
  }

  public JavaTypeMapping getFieldMappingInDatastoreClass(AbstractMemberMetaData mmd) {
    return getFieldMapping(mmd);
  }

  // Mostly copied from AbstractTable.addDatastoreField
  public DatastoreField addDatastoreField(String storedJavaType, DatastoreIdentifier name,
      JavaTypeMapping mapping, MetaData colmd) {

    if (hasColumnName(name)) {
      throw new NucleusException("Duplicate property name: " + name);
    }

    // Create the column
    DatastoreProperty prop =
        new DatastoreProperty(this, mapping.getJavaType().getName(), name, (ColumnMetaData) colmd);

    DatastoreIdentifier colName = prop.getIdentifier();

    datastoreProperties.add(prop);
    datastorePropertiesByName.put(colName, prop);
    return prop;
  }

  protected boolean hasColumnName(DatastoreIdentifier colName) {
    return datastorePropertiesByName.get(colName) != null;
  }


  public boolean hasDatastoreField(DatastoreIdentifier identifier) {
    return (hasColumnName(identifier));
  }

  public DatastoreField getDatastoreField(DatastoreIdentifier identifier) {
    return datastorePropertiesByName.get(identifier);
  }

  public JavaTypeMapping getIDMapping() {
    return idMapping;
  }

  public MappedStoreManager getStoreManager() {
    return storeMgr;
  }

  public DatastoreIdentifier getIdentifier() {
    return identifier;
  }

  public boolean isSuperDatastoreClass(DatastoreClass datastoreClass) {
    return false;
  }

  public boolean managesMapping(JavaTypeMapping javaTypeMapping) {
    return true;
  }

  public DatastoreField[] getDatastoreFields() {
    return datastoreProperties.toArray(new DatastoreField[datastoreProperties.size()]);
  }

  public void buildMapping() {
    initializePK();
    initializeNonPK();
  }

  private void initializeNonPK() {
    AbstractMemberMetaData[] fields = cmd.getManagedMembers();
    // Go through the fields for this class and add columns for them
    for (AbstractMemberMetaData fmd : fields) {
      // Primary key fields are added by the initialisePK method
      if (!fmd.isPrimaryKey()) {
        if (managesField(fmd.getFullFieldName())) {
          if (!fmd.getClassName(true).equals(cmd.getFullClassName())) {
            throw new UnsupportedOperationException("Overrides not currently supported.");
          }
        } else {
          // Manage the field if not already managed (may already exist if overriding a superclass field)
          if (fmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT) {
            boolean isPrimary = true;
            if (fmd.getTable() != null && fmd.getJoinMetaData() == null) {
              // Field has a table specified and is not a 1-N with join table
              // so is mapped to a secondary table
              isPrimary = false;
            }
            if (isPrimary) {
              // Add the field to this table
              JavaTypeMapping mapping = dba.getMappingManager().getMapping(
                  this, fmd, dba, clr, JavaTypeMapping.MAPPING_FIELD);
              addFieldMapping(mapping);
            } else {
              throw new UnsupportedOperationException("No support for secondary tables.");
            }
          } else if (fmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL) {
//            throw new NucleusException(LOCALISER.msg("057006", fmd.getName())).setFatal();
          }

          // Calculate if we need a FK adding due to a 1-N (FK) relationship
          boolean needsFKToContainerOwner = false;
          int relationType = fmd.getRelationType(clr);
          if (relationType == Relation.ONE_TO_MANY_BI) {
            AbstractMemberMetaData[] relatedMmds = fmd.getRelatedMemberMetaData(clr);
            if (fmd.getJoinMetaData() == null && relatedMmds[0].getJoinMetaData() == null) {
              needsFKToContainerOwner = true;
            }
          } else if (relationType == Relation.ONE_TO_MANY_UNI) {
            if (fmd.getJoinMetaData() == null) {
              needsFKToContainerOwner = true;
            }
          }

          if (needsFKToContainerOwner) {
            // 1-N uni/bidirectional using FK, so update the element side with a FK
            if ((fmd.getCollection() != null && !SCOUtils.collectionHasSerialisedElements(fmd)) ||
                (fmd.getArray() != null && !SCOUtils.arrayIsStoredInSingleColumn(fmd))) {
              // 1-N ForeignKey collection/array, so add FK to element table
              AbstractClassMetaData elementCmd;
              if (fmd.hasCollection()) {
                // Collection
                elementCmd = storeMgr.getOMFContext().getMetaDataManager()
                    .getMetaDataForClass(fmd.getCollection().getElementType(), clr);
              } else {
                // Array
                elementCmd = storeMgr.getOMFContext().getMetaDataManager()
                    .getMetaDataForClass(fmd.getType().getComponentType(), clr);
              }
              if (elementCmd == null) {
                // Elements that are reference types or non-PC will come through here
              } else {
              }
            } else if (fmd.getMap() != null && !SCOUtils.mapHasSerialisedKeysAndValues(fmd)) {
              // 1-N ForeignKey map, so add FK to value table
              if (fmd.getKeyMetaData() != null && fmd.getKeyMetaData().getMappedBy() != null) {
                // Key is stored in the value table so add the FK to the value table
                AbstractClassMetaData valueCmd = storeMgr.getOMFContext().getMetaDataManager()
                    .getMetaDataForClass(fmd.getMap().getValueType(), clr);
                if (valueCmd == null) {
                  // Interface elements will come through here and java.lang.String and others as well
                } else {
                }
              } else if (fmd.getValueMetaData() != null && fmd.getValueMetaData().getMappedBy() != null) {
                // Value is stored in the key table so add the FK to the key table
                AbstractClassMetaData keyCmd = storeMgr.getOMFContext().getMetaDataManager()
                    .getMetaDataForClass(fmd.getMap().getKeyType(), clr);
                if (keyCmd == null) {
                  // Interface elements will come through here and java.lang.String and others as well
                } else {
                }
              }
            }
          }
        }
      }
    }
  }

  protected void addFieldMapping(JavaTypeMapping fieldMapping) {
    AbstractMemberMetaData fmd = fieldMapping.getFieldMetaData();
    fieldMappingsMap.put(fmd, fieldMapping);
    // Update highest field number if this is higher
    int absoluteFieldNumber = fmd.getAbsoluteFieldNumber();
    if (absoluteFieldNumber > highestFieldNumber) {
      highestFieldNumber = absoluteFieldNumber;
    }
    if (fmd.isDependent()) {
      dependentMemberMetaData.add(fmd);
    }
  }

  public boolean managesField(String fieldName) {
    return fieldName != null && getMappingForFieldName(fieldName) != null;

  }

  /**
   * Accessor for the JavaTypeMapping that is handling the field of the specified name. Returns the
   * first one that matches.
   *
   * @param fieldName Name of the field
   * @return The java type mapping
   */
  protected JavaTypeMapping getMappingForFieldName(String fieldName) {
    Set fields = fieldMappingsMap.keySet();
    for (Object field : fields) {
      AbstractMemberMetaData fmd = (AbstractMemberMetaData) field;
      if (fmd.getFullFieldName().equals(fieldName)) {
        return fieldMappingsMap.get(fmd);
      }
    }
    return null;
  }

  void addDatastoreId(ColumnMetaDataContainer columnContainer, DatastoreClass refTable,
      AbstractClassMetaData cmd) {
    datastoreIDMapping = new OIDMapping();
    datastoreIDMapping.initialize(dba, cmd.getFullClassName());

    // Create a ColumnMetaData in the container if none is defined
    ColumnMetaData colmd;
    if (columnContainer == null) {
      colmd = new ColumnMetaData(null, (String) null);
    } else if (columnContainer.getColumnMetaData().length < 1) {
      colmd = new ColumnMetaData((MetaData) columnContainer, (String) null);
    } else {
      colmd = columnContainer.getColumnMetaData()[0];
    }
    if (colmd.getName() == null) {
      // Provide default column naming if none is defined
      if (refTable != null) {
        colmd.setName(storeMgr.getIdentifierFactory()
            .newDatastoreFieldIdentifier(refTable.getIdentifier().getIdentifier(),
                this.storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(OID.class),
                FieldRole.ROLE_OWNER).getIdentifier());
      } else {
        colmd.setName(
            storeMgr.getIdentifierFactory().newDatastoreFieldIdentifier(identifier.getIdentifier(),
                this.storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(OID.class),
                FieldRole.ROLE_NONE).getIdentifier());
      }
    }

    // Add the datastore identity column as the PK
    DatastoreField idColumn = addDatastoreField(OID.class.getName(),
        storeMgr.getIdentifierFactory().newIdentifier(IdentifierFactory.COLUMN, colmd.getName()),
        datastoreIDMapping, colmd);
    idColumn.setAsPrimaryKey();

    // Set the identity column type based on the IdentityStrategy
    String strategyName = cmd.getIdentityMetaData().getValueStrategy().toString();
    if (cmd.getIdentityMetaData().getValueStrategy().equals(IdentityStrategy.CUSTOM)) {
      strategyName = cmd.getIdentityMetaData().getValueStrategy().getCustomName();
    }

    // Check the POID type being stored
    Class poidClass = Long.class;
    ConfigurationElement elem =
        storeMgr.getOMFContext().getPluginManager().getConfigurationElementForExtension(
            "org.datanucleus.store_valuegenerator",
            new String[]{"name", "unique"}, new String[]{strategyName, "true"});
    if (elem == null) {
      // Not datastore-independent, so try for this datastore
      elem = storeMgr.getOMFContext().getPluginManager().getConfigurationElementForExtension(
          "org.datanucleus.store_valuegenerator",
          new String[]{"name", "datastore"},
          new String[]{strategyName, storeMgr.getStoreManagerKey()});
    }
    if (elem != null) {
      // Set the generator name (for use by the PoidManager)
      String generatorClassName = elem.getAttribute("class-name");
      Class generatorClass =
          getStoreManager().getOMFContext().getClassLoaderResolver(null)
              .classForName(generatorClassName);
      try {
        poidClass = (Class) generatorClass.getMethod("getStorageClass").invoke(null);
      }
      catch (Exception e) {
        // Unable to get the storage class from the PoidGenerator class
        NucleusLogger.VALUEGENERATION
            .warn("Error retrieving storage class for POID generator " + generatorClassName +
                " " + e.getMessage());
      }
    }

    dba.getMappingManager().createDatastoreMapping(datastoreIDMapping, storeMgr, idColumn,
        poidClass.getName());

    // Handle any auto-increment requirement
    if (isObjectIDDatastoreAttributed()) {
      // Only the base class can be autoincremented
//      idColumn.setAutoIncrement(true);
    }

    // Check if auto-increment and that it is supported by this RDBMS
//    if (idColumn.isAutoIncrement() && !dba
//        .supportsOption(org.datanucleus.store.mapped.DatastoreAdapter.IDENTITY_COLUMNS)) {
//      throw new NucleusException(LOCALISER.msg("057020",
//          cmd.getFullClassName(), "datastore-identity")).setFatal();
//    }
  }

  private void initializePK() {
    AbstractMemberMetaData[] fieldsToAdd = new AbstractMemberMetaData[cmd
        .getNoOfPrimaryKeyMembers()];

    // Initialise Primary Key mappings for application id with PK fields in this class
    int pkFieldNum = 0;
    int fieldCount = cmd.getNoOfManagedMembers();
    boolean hasPrimaryKeyInThisClass = false;
    if (cmd.getNoOfPrimaryKeyMembers() > 0) {
      pkMappings = new JavaTypeMapping[cmd.getNoOfPrimaryKeyMembers()];
      if (cmd.getInheritanceMetaData().getStrategyValue() == InheritanceStrategy.COMPLETE_TABLE) {
        // COMPLETE-TABLE so use root class metadata and add PK members
        // TODO Does this allow for overridden PK field info ?
        AbstractClassMetaData baseCmd = cmd.getBaseAbstractClassMetaData();
        fieldCount = baseCmd.getNoOfManagedMembers();
        for (int relFieldNum = 0; relFieldNum < fieldCount; ++relFieldNum) {
          AbstractMemberMetaData mmd = baseCmd.getMetaDataForManagedMemberAtPosition(relFieldNum);
          if (mmd.isPrimaryKey()) {
            if (mmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT) {
              fieldsToAdd[pkFieldNum++] = mmd;
              hasPrimaryKeyInThisClass = true;
            } else if (mmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL) {
              throw new NucleusException(LOCALISER.msg("057006", mmd.getName())).setFatal();
            }

            // Check if auto-increment and that it is supported by this RDBMS
            if ((mmd.getValueStrategy() == IdentityStrategy.IDENTITY) &&
                !dba.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS)) {
              throw new NucleusException(LOCALISER.msg("057020",
                  cmd.getFullClassName(), mmd.getName())).setFatal();
            }
          }
        }
      } else {
        for (int relFieldNum = 0; relFieldNum < fieldCount; ++relFieldNum) {
          AbstractMemberMetaData fmd = cmd.getMetaDataForManagedMemberAtPosition(relFieldNum);
          if (fmd.isPrimaryKey()) {
            if (fmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT) {
              fieldsToAdd[pkFieldNum++] = fmd;
              hasPrimaryKeyInThisClass = true;
            } else if (fmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL) {
              throw new NucleusException(LOCALISER.msg("057006", fmd.getName())).setFatal();
            }

            // Check if auto-increment and that it is supported by this datastore
            if ((fmd.getValueStrategy() == IdentityStrategy.IDENTITY) &&
                !dba.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS)) {
              throw new NucleusException(LOCALISER.msg("057020",
                  cmd.getFullClassName(), fmd.getName())).setFatal();
            }
          }
        }
      }
    }

    // No Primary Key defined, so search for superclass or handle datastore id
    if (!hasPrimaryKeyInThisClass) {
      if (cmd.getIdentityType() == IdentityType.APPLICATION) {
        // application-identity
        DatastoreClass elementCT =
            storeMgr.getDatastoreClass(cmd.getPersistenceCapableSuperclass(), clr);
        if (elementCT != null) {
          // Superclass has a table so copy its PK mappings
          ColumnMetaDataContainer colContainer = null;
          if (cmd.getInheritanceMetaData() != null) {
            // Try via <inheritance><join>...</join></inheritance>
            colContainer = cmd.getInheritanceMetaData().getJoinMetaData();
          }
          if (colContainer == null) {
            // Try via <primary-key>...</primary-key>
            colContainer = cmd.getPrimaryKeyMetaData();
          }

          addApplicationIdUsingClassTableId(colContainer, elementCT, clr, cmd);
        } else {
          // Superclass has no table so create new mappings and columns
          AbstractClassMetaData pkCmd =
              storeMgr.getClassWithPrimaryKeyForClass(cmd.getSuperAbstractClassMetaData(), clr);
          if (pkCmd != null) {
            pkMappings = new JavaTypeMapping[pkCmd.getNoOfPrimaryKeyMembers()];
            pkFieldNum = 0;
            fieldCount = pkCmd.getNoOfInheritedManagedMembers() + pkCmd.getNoOfManagedMembers();
            for (int absFieldNum = 0; absFieldNum < fieldCount; ++absFieldNum) {
              AbstractMemberMetaData fmd = pkCmd
                  .getMetaDataForManagedMemberAtAbsolutePosition(absFieldNum);
              if (fmd.isPrimaryKey()) {
                AbstractMemberMetaData overriddenFmd = cmd.getOverriddenMember(fmd.getName());
                if (overriddenFmd != null) {
                  // PK field is overridden so use the overriding definition
                  fmd = overriddenFmd;
                }

                if (fmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT) {
                  fieldsToAdd[pkFieldNum++] = fmd;
                } else if (fmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL) {
                  throw new NucleusException(LOCALISER.msg("057006", fmd.getName())).setFatal();
                }
              }
            }
          }
        }
      } else if (cmd.getIdentityType() == IdentityType.DATASTORE) {
        // datastore-identity
        ColumnMetaDataContainer colContainer = null;
        if (cmd.getIdentityMetaData() != null
            && cmd.getIdentityMetaData().getColumnMetaData() != null &&
            cmd.getIdentityMetaData().getColumnMetaData().length > 0) {
          // Try via <datastore-identity>...</datastore-identity>
          colContainer = cmd.getIdentityMetaData();
        }
        if (colContainer == null) {
          // Try via <primary-key>...</primary-key>
          colContainer = cmd.getPrimaryKeyMetaData();
        }
        addDatastoreId(colContainer, null, cmd);
      } else if (cmd.getIdentityType() == IdentityType.NONDURABLE) {
        // Do nothing since no identity!
      }
    }

    //add field mappings in the end, so we compute all columns after the post initialize
    for (int i = 0; i < fieldsToAdd.length; i++) {
      if (fieldsToAdd[i] != null) {
        try {
          DatastoreClass datastoreClass = getStoreManager()
              .getDatastoreClass(fieldsToAdd[i].getType().getName(), clr);
          if (datastoreClass.getIDMapping() == null) {
            throw new NucleusException(
                "Unsupported relationship with field " + fieldsToAdd[i].getFullFieldName())
                .setFatal();
          }
        }
        catch (NoTableManagedException ex) {
          //do nothing
        }
        JavaTypeMapping fieldMapping = dba.getMappingManager()
            .getMapping(this, fieldsToAdd[i], dba, clr, JavaTypeMapping.MAPPING_FIELD);
        addFieldMapping(fieldMapping);
        pkMappings[i] = fieldMapping;
      }
    }
    initializeIDMapping();
  }

  /**
   * Initialize the ID Mapping
   */
  private void initializeIDMapping() {
    if (idMapping != null) {
      return;
    }

    final PersistenceCapableMapping mapping = new PersistenceCapableMapping();
    mapping.initialize(getStoreManager().getDatastoreAdapter(), cmd.getFullClassName());
    if (getIdentityType() == IdentityType.DATASTORE) {
      mapping.addJavaTypeMapping(datastoreIDMapping);
    } else if (getIdentityType() == IdentityType.APPLICATION) {
      for (JavaTypeMapping pkMapping : pkMappings) {
        mapping.addJavaTypeMapping(pkMapping);
      }
    } else {
      // Nothing to do for nondurable since no identity
    }

    idMapping = mapping;
  }

  final void addApplicationIdUsingClassTableId(ColumnMetaDataContainer columnContainer,
      DatastoreClass refTable, ClassLoaderResolver clr, AbstractClassMetaData cmd) {
    ColumnMetaData[] userdefinedCols = null;
    int nextUserdefinedCol = 0;
    if (columnContainer != null) {
      userdefinedCols = columnContainer.getColumnMetaData();
    }

    pkMappings = new JavaTypeMapping[cmd.getPKMemberPositions().length];
    for (int i = 0; i < cmd.getPKMemberPositions().length; i++) {
      AbstractMemberMetaData fmd = cmd
          .getMetaDataForManagedMemberAtAbsolutePosition(cmd.getPKMemberPositions()[i]);
      JavaTypeMapping mapping = refTable.getFieldMapping(fmd);
      if (mapping == null) {
        //probably due to invalid metadata defined by the user
        throw new NucleusUserException("Cannot find mapping for field " + fmd.getFullFieldName() +
            " in table " + refTable.toString() + " " +
            StringUtils.objectArrayToString(refTable.getDatastoreFields()));
      }

      JavaTypeMapping masterMapping = dba.getMapping(clr.classForName(mapping.getType()), storeMgr);
      masterMapping.setFieldInformation(fmd, this); // Update field info in mapping
      pkMappings[i] = masterMapping;

      // Loop through each id column in the reference table and add the same here
      // applying the required names from the columnContainer
      for (int j = 0; j < mapping.getNumberOfDatastoreFields(); j++) {
        JavaTypeMapping m = masterMapping;
        DatastoreField refColumn = mapping.getDataStoreMapping(j).getDatastoreField();
        if (mapping instanceof PersistenceCapableMapping) {
          m = dba.getMapping(clr.classForName(refColumn.getMapping().getType()), storeMgr);
          ((PersistenceCapableMapping) masterMapping).addJavaTypeMapping(m);
        }

        ColumnMetaData userdefinedColumn = null;
        if (userdefinedCols != null) {
          for (ColumnMetaData userdefinedCol : userdefinedCols) {
            if (refColumn.getIdentifier().toString().equals(userdefinedCol.getTarget())) {
              userdefinedColumn = userdefinedCol;
              break;
            }
          }
          if (userdefinedColumn == null && nextUserdefinedCol < userdefinedCols.length) {
            userdefinedColumn = userdefinedCols[nextUserdefinedCol++];
          }
        }

        // Add this application identity column
        DatastoreField idColumn;
        if (userdefinedColumn != null) {
          // User has provided a name for this column
          // Currently we only use the column namings from the users definition but we could easily
          // take more of their details.
          idColumn = addDatastoreField(refColumn.getStoredJavaType(),
              storeMgr.getIdentifierFactory().newIdentifier(IdentifierFactory.COLUMN,
                  userdefinedColumn.getName()),
              m, refColumn.getMetaData());
        } else {
          // No name provided so take same as superclass
          idColumn = addDatastoreField(refColumn.getStoredJavaType(), refColumn.getIdentifier(),
              m, refColumn.getMetaData());
        }
        if (mapping.getDataStoreMapping(j).getDatastoreField().getMetaData() != null) {
          refColumn.copyConfigurationTo(idColumn);
        }
        idColumn.setAsPrimaryKey();

        // Set the column type based on the field.getType()
        getStoreManager().getMappingManager().createDatastoreMapping(m, storeMgr, idColumn,
            refColumn.getMapping().getType());
      }

      // Update highest field number if this is higher
      int absoluteFieldNumber = fmd.getAbsoluteFieldNumber();
      if (absoluteFieldNumber > highestFieldNumber) {
        highestFieldNumber = absoluteFieldNumber;
      }
    }
  }

  public void provideDatastoreIdMappings(MappingConsumer consumer) {
    consumer.preConsumeMapping(highestFieldNumber + 1);

    if (getIdentityType() == IdentityType.DATASTORE) {
      consumer.consumeMapping(getDataStoreObjectIdMapping(), MappingConsumer.MAPPING_TYPE_DATASTORE_ID);
    }
  }

  public void providePrimaryKeyMappings(MappingConsumer consumer) {
    consumer.preConsumeMapping(highestFieldNumber + 1);

    if (pkMappings != null) {
      // Application identity
      int[] primaryKeyFieldNumbers = cmd.getPKMemberPositions();
      for (int i = 0; i < pkMappings.length; i++) {
        // Make the assumption that the pkMappings are in the same order as the absolute field numbers
        AbstractMemberMetaData fmd = cmd
            .getMetaDataForManagedMemberAtAbsolutePosition(primaryKeyFieldNumbers[i]);
        consumer.consumeMapping(pkMappings[i], fmd);
      }
    } else {
      // Datastore identity
      int[] primaryKeyFieldNumbers = cmd.getPKMemberPositions();
      int countPkFields = cmd.getNoOfPrimaryKeyMembers();
      for (int i = 0; i < countPkFields; i++) {
        AbstractMemberMetaData pkfmd = cmd
            .getMetaDataForManagedMemberAtAbsolutePosition(primaryKeyFieldNumbers[i]);
        consumer.consumeMapping(getFieldMapping(pkfmd), pkfmd);
      }
    }
  }

  public void provideNonPrimaryKeyMappings(MappingConsumer consumer) {
    consumer.preConsumeMapping(highestFieldNumber + 1);

    Set fieldNumbersSet = fieldMappingsMap.keySet();
    for (Object aFieldNumbersSet : fieldNumbersSet) {
      AbstractMemberMetaData fmd = (AbstractMemberMetaData) aFieldNumbersSet;
      JavaTypeMapping fieldMapping = fieldMappingsMap.get(fmd);
      if (fieldMapping != null) {
        if (!fmd.isPrimaryKey()) {
          consumer.consumeMapping(fieldMapping, fmd);
        }
      }
    }
  }

  public void provideMappingsForFields(MappingConsumer consumer,
      AbstractMemberMetaData[] fieldMetaData, boolean includeSecondaryTables) {
  }

  public void provideVersionMappings(MappingConsumer consumer) {
  }

  public void provideDiscriminatorMappings(MappingConsumer consumer) {
  }

  public void provideUnmappedDatastoreFields(MappingConsumer consumer) {
  }

  public void provideExternalMappings(MappingConsumer consumer, int mappingType) {
  }

  public JavaTypeMapping getExternalMapping(AbstractMemberMetaData fmd, int mappingType) {
    return null;
  }

  public AbstractMemberMetaData getMetaDataForExternalMapping(JavaTypeMapping mapping,
      int mappingType) {
    return null;
  }

  public DiscriminatorMetaData getDiscriminatorMetaData() {
    return null;
  }

  public JavaTypeMapping getDiscriminatorMapping(boolean allowSuperclasses) {
    return null;
  }

  public VersionMetaData getVersionMetaData() {
    return null;
  }

  public JavaTypeMapping getVersionMapping(boolean allowSuperclasses) {
    return null;
  }

  public List<AbstractMemberMetaData> getDependentMemberMetaData() {
    return dependentMemberMetaData;
  }
}
