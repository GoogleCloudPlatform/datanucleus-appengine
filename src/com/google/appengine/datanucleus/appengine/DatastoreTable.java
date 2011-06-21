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
package com.google.appengine.datanucleus.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.ColumnMetaDataContainer;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.PropertyMetaData;
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
import org.datanucleus.store.mapped.IdentifierType;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.exceptions.DuplicateDatastoreFieldException;
import org.datanucleus.store.mapped.mapping.CorrespondentColumnsMapper;
import org.datanucleus.store.mapped.mapping.DatastoreMapping;
import org.datanucleus.store.mapped.mapping.DiscriminatorLongMapping;
import org.datanucleus.store.mapped.mapping.DiscriminatorStringMapping;
import org.datanucleus.store.mapped.mapping.IndexMapping;
import org.datanucleus.store.mapped.mapping.IntegerMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.LongMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.mapped.mapping.OIDMapping;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;
import org.datanucleus.util.MultiMap;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes a 'table' in the datastore.  We don't actually have tables, but
 * we need to represent this logically in order to take care of all the nice
 * mapping logic that datanucleus does for us.
 *
 * This code is largely copied from AbstractClassTable, ClassTable, TableImpl,
 * and AbstractTable
 *
 * TODO(maxr): Refactor the RDBMS classes into the mapped package so we can
 * refactor this.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreTable implements DatastoreClass {

  /** All callbacks for class tables waiting to be performed. */
  private static final MultiMap callbacks = new MultiMap();

  private final DatastoreManager storeMgr;
  private final AbstractClassMetaData cmd;
  private final ClassLoaderResolver clr;
  private final DatastoreAdapter dba;
  protected final DatastoreIdentifier identifier;
  private final Map<String, AbstractClassMetaData> owningClassMetaData = Utils.newHashMap();

  /**
   * Mappings for fields mapped to this table, keyed by the FieldMetaData.
   * Supports fast lookup but also preserves order.
   */
  private final Map<AbstractMemberMetaData, JavaTypeMapping> fieldMappingsMap =
      new LinkedHashMap<AbstractMemberMetaData, JavaTypeMapping>();

  /**
   * Similar to {@link #fieldMappingsMap} except primary key fields are added as
   * well.  This is needed to support persistence capable classes that can be
   * persisted as top-level classes and as embedded classes.
   */
  private final Map<AbstractMemberMetaData, JavaTypeMapping> embeddedFieldMappingsMap =
      new LinkedHashMap<AbstractMemberMetaData, JavaTypeMapping>();

  /**
   * All the properties in the table.  Even though the datastore is schemaless,
   * the mappings provided by the ORM effectively impose a schema.  This allows
   * us to know, up front, what properties we can expect.
   */
  private final List<DatastoreProperty> datastoreProperties = Utils.newArrayList();

  /**
   * Index to the props, keyed by name.
   */
  protected Map<String, DatastoreProperty> datastorePropertiesByName = Utils.newHashMap();

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

  /** MetaData for discriminator for objects stored in this kind. */
  protected DiscriminatorMetaData discriminatorMetaData;

  /** Mapping for any discriminator column. */
  private JavaTypeMapping discriminatorMapping;
  
  /**
   * Highest absolute field number managed by this table
   */
  private int highestFieldNumber = 0;

  /**
   * Dependent fields.  Unlike pretty much the rest of this class, this member and the
   * code that populates it is specific to the appengine plugin.
   */
  private final List<AbstractMemberMetaData> sameEntityGroupMemberMetaData = Utils.newArrayList();
  private final Map<AbstractMemberMetaData, JavaTypeMapping> externalFkMappings = Utils.newHashMap();
  private final Map<AbstractMemberMetaData, JavaTypeMapping> externalOrderMappings = Utils.newHashMap();
  private AbstractMemberMetaData parentMappingField;
  
  /** MetaData for all classes being managed here. */
  private final Collection<AbstractClassMetaData> managedClassMetaData = new HashSet<AbstractClassMetaData>();
  
  DatastoreTable(String kind, DatastoreManager storeMgr, AbstractClassMetaData cmd,
      ClassLoaderResolver clr, DatastoreAdapter dba) {
    this.storeMgr = storeMgr;
    this.cmd = cmd;
    this.clr = clr;
    this.dba = dba;
    this.identifier = new DatastoreKind(kind);
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

  public DatastoreClass getBaseDatastoreClassWithMember(AbstractMemberMetaData fmd) {
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

  public JavaTypeMapping getMemberMapping(String fieldName) {
    AbstractMemberMetaData fmd = getFieldMetaData(fieldName);
    JavaTypeMapping m = getMemberMapping(fmd);
    if (m == null) {
        throw new NoSuchPersistentFieldException(cmd.getFullClassName(), fieldName);
    }
    return m;
  }

  private AbstractMemberMetaData getFieldMetaData(String fieldName) {
    return cmd.getMetaDataForMember(fieldName);
  }

  public JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd) {
    if (mmd == null) {
      return null;
    }

    return fieldMappingsMap.get(mmd);
  }

  public JavaTypeMapping getMemberMappingInDatastoreClass(AbstractMemberMetaData mmd) {
    return getMemberMapping(mmd);
  }

  // Mostly copied from AbstractTable.addDatastoreField
  public DatastoreProperty addDatastoreField(String storedJavaType, DatastoreIdentifier name,
      JavaTypeMapping mapping, MetaData colmd) {

    // Create the column
    DatastoreProperty prop =
        new DatastoreProperty(this, mapping.getJavaType().getName(), name, (ColumnMetaData) colmd);

    if (hasColumnName(name)) {
      if (colmd == null || !(colmd instanceof ColumnMetaData)) {
        throw new NucleusException(
            "invalid column meta data property name on class " + getType() + " : " + colmd);
      }

      if (((ColumnMetaData) colmd).getInsertable() || ((ColumnMetaData) colmd).getUpdateable()) {
        // duplicate property names are ok if the field is neither insertable nor updatable
        if (!isSuperclassColumn(prop, mapping, colmd)) {
          throw new NucleusException(
              "Duplicate property name on class " + getType() + " : " + name);
        }
      }
    }
    DatastoreIdentifier colName = prop.getIdentifier();

    datastoreProperties.add(prop);
    datastorePropertiesByName.put(colName.getIdentifierName(), prop);
    return prop;
  }

  protected boolean hasColumnName(DatastoreIdentifier colName) {
    return getDatastoreField(colName) != null;
  }


  public boolean hasDatastoreField(DatastoreIdentifier identifier) {
    return (hasColumnName(identifier));
  }

  public boolean isSuperclassColumn(DatastoreField col, JavaTypeMapping mapping, MetaData colmd) {
    // Verify if a duplicate column is valid. A duplicate column name is (currently) valid when :-
    // 1. subclasses defining the duplicated column are using "super class table" strategy
    //
    // Find the MetaData for the existing column
    DatastoreIdentifier name = col.getIdentifier();
    DatastoreField existingCol = getDatastoreField(name);
    MetaData md = existingCol.getColumnMetaData().getParent();
    while (!(md instanceof AbstractClassMetaData)) {
      if (md == null) {
        // ColumnMetaData for existing column has no parent class somehow!
        throw new NucleusUserException(MessageFormat.format(
            "The property \"{0}\" exists in entity \"{1}\" and has invalid metadata. The existing property is \"{2}\"",
            name, this.identifier, colmd.toString()));
      }
      md = md.getParent();
    }

    // Find the MetaData for the column to be added
    MetaData dupMd = colmd.getParent();
    while (!(dupMd instanceof AbstractClassMetaData)) {
      dupMd = dupMd.getParent();
      if (dupMd == null) {
        // ColumnMetaData for required column has no parent class somehow!
        throw new NucleusUserException(MessageFormat.format(
            "The column \"{0}\" exists in table \"{1}\" and cannot be validated because a duplicated column has been specified and the metadata is invalid. The column is \"{2}\"",
            name, this.identifier, colmd.toString()));
      }
    }
    if (((AbstractClassMetaData) md).getFullClassName().equals(
        ((AbstractClassMetaData) dupMd).getFullClassName())) {
      // compare the current column defining class and the duplicated column defining class. if the same class,
      // we raise an exception when within one class it is defined a column twice
      // in some cases it could still be possible to have these duplicated columns, but does not make too
      // much sense in most of the cases. (this whole block of duplicated column check, could be optional, like a pmf property)
      throw new DuplicateDatastoreFieldException(this.toString(), existingCol, col);
    }

    // Make sure the field JavaTypeMappings are compatible
    if (mapping != null &&
        !mapping.getClass().isAssignableFrom(existingCol.getJavaTypeMapping().getClass()) &&
        !existingCol.getJavaTypeMapping().getClass().isAssignableFrom(mapping.getClass())) {
      // the mapping class must be the same (not really required, but to avoid user mistakes)
      throw new DuplicateDatastoreFieldException(this.toString(), existingCol, col);
    }

    // Make sure the field java types are compatible
    Class<?> fieldStoredJavaTypeClass = null;
    Class<?> existingColStoredJavaTypeClass = null;
    try {
      ClassLoaderResolver clr = storeMgr.getOMFContext().getClassLoaderResolver(null);
      fieldStoredJavaTypeClass = clr.classForName(col.getStoredJavaType());
      existingColStoredJavaTypeClass = clr.classForName(col.getStoredJavaType());
    }
    catch (RuntimeException cnfe) {
      // Do nothing
    }
    if (fieldStoredJavaTypeClass != null && existingColStoredJavaTypeClass != null &&
        !fieldStoredJavaTypeClass.isAssignableFrom(existingColStoredJavaTypeClass) &&
        !existingColStoredJavaTypeClass.isAssignableFrom(fieldStoredJavaTypeClass)) {
      // the stored java type must be the same (not really required, but to avoid user mistakes)
      throw new DuplicateDatastoreFieldException(this.toString(), existingCol, col);
    }
    return true;
  }

  DatastoreField getDatastoreField(String colName) {
    return datastorePropertiesByName.get(colName);
  }
  public DatastoreField getDatastoreField(DatastoreIdentifier identifier) {
    return getDatastoreField(identifier.getIdentifierName());
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
    initializeDescriminatorMapping();
    runCallBacks();
    this.managedClassMetaData.add(cmd);
  }

  public void manageClass(AbstractClassMetaData cmd) {
    //	We demand DiscriminatorMetaData, because in cases where an object of the
    //	top class in the inheritance hierarchy will be persisted and the subclasses
    //	are not yet known to datanucleus, we already need to know about the discriminator
    //	property. At this point here the exception can be too late, if an object of
    //	the top class has been persisted already. But at least the developer is
    //	now informed, that he should add the DiscriminatorMetaData.
    if (getDiscriminatorMetaData() == null) {
      throw new NucleusUserException("Descriminator meta data for " +
                                     cmd.getFullClassName() +
                                     " is missing. Please specify at least the discriminator column.");
    }

    // Go through the fields for this class and add columns for them
    for (AbstractMemberMetaData fmd : cmd.getManagedMembers()) {
      addFieldMapping(cmd, fmd);
    }
    this.managedClassMetaData.add(cmd);
  }

  /**
   * returns the names of all classes managed by this table.
   *
   * @return Names of the classes managed (stored) here
   */
  public List<String> getManagedClasses() {
    return Utils
        .transform(managedClassMetaData, new Utils.Function<AbstractClassMetaData, String>() {
          public String apply(AbstractClassMetaData cmd) {
            return cmd.getFullClassName();
          }
        });
  }

  private void initializeNonPK() {
    // We only support inheritance strategies that resolve to all fields for
    // a class plus all its subclasses living in every "table," so we'll
    // iterate over all managed fields in the entire chain, topmost classes
    // first so that overrides work properly.
    List<AbstractClassMetaData> cmdl = buildClassMetaDataList();
    
    // get the class level overridden members
    // simply reversing the order we iterate through the hierarchy
    // of ClassMetaData disorders the field numbers 
    Map<String, AbstractMemberMetaData> overriddenFieldMap = new HashMap<String, AbstractMemberMetaData>();
    for (AbstractClassMetaData curCmd : cmdl) {
      for (AbstractMemberMetaData fmd : curCmd.getOverriddenMembers()) {
        overriddenFieldMap.put(fmd.getFullFieldName(), fmd);
      }
    }    
    
    for (AbstractClassMetaData curCmd : cmdl) {
      // Go through the fields for this class and add columns for them
      for (AbstractMemberMetaData fmd : curCmd.getManagedMembers()) {
        if (overriddenFieldMap.containsKey(fmd.getFullFieldName())) {
          fmd = overriddenFieldMap.get(fmd.getFullFieldName());
        }
        addFieldMapping(curCmd, fmd);
      }
    }

    Class curClass = clr.classForName(cmd.getFullClassName()).getSuperclass();
    // see if any of our superclasses have a parentMappingField
    while (!Object.class.equals(curClass)) {
      DatastoreTable dt = null;
      try {
        dt = (DatastoreTable) storeMgr.getDatastoreClass(curClass.getName(), clr);
      } catch (NoTableManagedException ntme) {
        // this is ok, not all parent classes need to be managed
      }
      // sometimes the parent table is not yet initialized
      if (dt != null) {
        // inherit the parentMappingField
        if (parentMappingField == null) {
          if (dt.parentMappingField != null) {
            parentMappingField = dt.parentMappingField;
            break;
          }
        }
        // inherit any external fk mappings
        if (!dt.externalFkMappings.isEmpty()) {
          externalFkMappings.putAll(dt.externalFkMappings);
        }
      }
      curClass = curClass.getSuperclass();
    }
  }

  private void addFieldMapping(AbstractClassMetaData curCmd,
                               AbstractMemberMetaData fmd) {
    // Primary key fields are added by the initialisePK method
    if (fmd.isPrimaryKey()) {
      // We need to know about this mapping when accessing the class as an
      // embedded field.
      embeddedFieldMappingsMap.put(fmd, pkMappings[0]);
    } else {
      if (managesField(fmd.getFullFieldName())) {
        if (!fmd.getClassName(true).equals(curCmd.getFullClassName())) {
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
            JavaTypeMapping mapping = dba.getMappingManager(storeMgr).getMapping(
                this, fmd, dba, clr, FieldRole.ROLE_FIELD);
            addFieldMapping(mapping);
            embeddedFieldMappingsMap.put(fmd, fieldMappingsMap.get(fmd));
          } else {
            throw new UnsupportedOperationException("No support for secondary tables.");
          }
        } else if (fmd.getPersistenceModifier() != FieldPersistenceModifier.TRANSACTIONAL) {
          throw new NucleusException("Invalid persistence-modifier for field ").setFatal();
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
        } else if (relationType == Relation.ONE_TO_ONE_BI) {
          if (fmd.getMappedBy() != null) {
            // This element type has a many-to-one pointing back.
            // We assume that our pk is part of the pk of the element type.
            DatastoreTable dt = storeMgr
                    .getDatastoreClass(fmd.getAbstractClassMetaData().getFullClassName(), clr);
            dt.runCallBacks();
            dt.markFieldAsParentKeyProvider(fmd.getName());
          }
        } else if (relationType == Relation.MANY_TO_ONE_BI) {
          DatastoreTable dt = storeMgr
                  .getDatastoreClass(fmd.getAbstractClassMetaData().getFullClassName(), clr);
          AbstractClassMetaData acmd =
              storeMgr.getMetaDataManager().getMetaDataForClass(fmd.getType(), clr);
          dt.addOwningClassMetaData(fmd.getColumnMetaData()[0].getName(), acmd);
        }

        if (needsFKToContainerOwner) {
          // 1-N uni/bidirectional using FK, so update the element side with a FK
          if ((fmd.getCollection() != null && !SCOUtils.collectionHasSerialisedElements(fmd)) ||
              (fmd.getArray() != null && !SCOUtils.arrayIsStoredInSingleColumn(fmd))) {
            // 1-N ForeignKey collection/array, so add FK to element table
            AbstractClassMetaData elementCmd;
            if (fmd.hasCollection()) {
              // Collection
              elementCmd = storeMgr.getMetaDataManager()
                  .getMetaDataForClass(fmd.getCollection().getElementType(), clr);
            } else {
              // Array
              elementCmd = storeMgr.getMetaDataManager()
                  .getMetaDataForClass(fmd.getType().getComponentType(), clr);
            }
            if (elementCmd == null) {
              // Elements that are reference types or non-PC will come through here
            } else {
              AbstractClassMetaData[] elementCmds;
              // TODO : Cater for interface elements, and get the metadata for the implementation classes here
              if (elementCmd.getInheritanceMetaData().getStrategy()
                  == InheritanceStrategy.SUBCLASS_TABLE) {
                elementCmds = storeMgr.getClassesManagingTableForClass(elementCmd, clr);
              } else {
                elementCmds = new ClassMetaData[1];
                elementCmds[0] = elementCmd;
              }

              // Run callbacks for each of the element classes.
              for (AbstractClassMetaData elementCmd1 : elementCmds) {
                callbacks.put(elementCmd1.getFullClassName(),
                              new CallBack(fmd, cmd.getFullClassName()));
                DatastoreTable dt =
                    (DatastoreTable) storeMgr
                        .getDatastoreClass(elementCmd1.getFullClassName(), clr);
                dt.runCallBacks();
                if (fmd.getMappedBy() != null) {
                  // This element type has a many-to-one pointing back.
                  // We assume that our pk is part of the pk of the element type.
                  dt.markFieldAsParentKeyProvider(fmd.getMappedBy());
                }
              }
            }
          } else if (fmd.getMap() != null && !SCOUtils.mapHasSerialisedKeysAndValues(fmd)) {
            // 1-N ForeignKey map, so add FK to value table
            if (fmd.getKeyMetaData() != null && fmd.getKeyMetaData().getMappedBy() != null) {
              // Key is stored in the value table so add the FK to the value table
              AbstractClassMetaData valueCmd = storeMgr.getMetaDataManager()
                  .getMetaDataForClass(fmd.getMap().getValueType(), clr);
              if (valueCmd == null) {
                // Interface elements will come through here and java.lang.String and others as well
              }
            } else if (fmd.getValueMetaData() != null
                       && fmd.getValueMetaData().getMappedBy() != null) {
              // Value is stored in the key table so add the FK to the key table
              AbstractClassMetaData keyCmd = storeMgr.getMetaDataManager()
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

  /**
   * Constructs a list containing the class meta data of {@link #cmd} and all
   * its superclasses.  We add to the beginning of the list as we iterate up so
   * the class at the top of the hierarchy is returned first. 
   */
  private LinkedList<AbstractClassMetaData> buildClassMetaDataList() {
    LinkedList<AbstractClassMetaData> stack = Utils.newLinkedList();
    AbstractClassMetaData curCmd = cmd;
    while (curCmd != null) {
      stack.addFirst(curCmd);
      curCmd = curCmd.getSuperAbstractClassMetaData();
    }
    return stack;
  }

  private void markFieldAsParentKeyProvider(String mappedBy) {

    AbstractMemberMetaData newParentMappingField = getFieldMetaData(mappedBy);
    if (parentMappingField == null) {
      parentMappingField = newParentMappingField;
    } else if (parentMappingField != newParentMappingField) { // intentional reference compare
      throw new NucleusException(
          "App Engine ORM does not support multiple parent key provider fields.");
    }
  }

  protected void addFieldMapping(JavaTypeMapping fieldMapping) {
    AbstractMemberMetaData fmd = fieldMapping.getMemberMetaData();
    fieldMappingsMap.put(fmd, fieldMapping);
    // Update highest field number if this is higher
    int absoluteFieldNumber = fmd.getAbsoluteFieldNumber();
    if (absoluteFieldNumber > highestFieldNumber) {
      highestFieldNumber = absoluteFieldNumber;
    }

    if (isInSameEntityGroup(fmd)) {
      sameEntityGroupMemberMetaData.add(fmd);
    }
  }

  private boolean isInSameEntityGroup(AbstractMemberMetaData ammd) {
    return ammd.getRelationType(clr) != Relation.NONE;
  }

  public boolean managesField(String fieldName) {
    return fieldName != null && getMappingForFullFieldName(fieldName) != null;

  }

  /**
   * Accessor for the JavaTypeMapping that is handling the field of the specified name. Returns the
   * first one that matches.
   *
   * @param fieldName Name of the field
   * @return The java type mapping
   */
  public JavaTypeMapping getMappingForFullFieldName(String fieldName) {
    Set fields = fieldMappingsMap.keySet();
    for (Object field : fields) {
      AbstractMemberMetaData fmd = (AbstractMemberMetaData) field;
      if (fmd.getFullFieldName().equals(fieldName)) {
        return fieldMappingsMap.get(fmd);
      }
    }
    return null;
  }

  public JavaTypeMapping getMappingForSimpleFieldName(String fieldName) {
    Set fields = fieldMappingsMap.keySet();
    for (Object field : fields) {
      AbstractMemberMetaData fmd = (AbstractMemberMetaData) field;
      if (fmd.getName().equals(fieldName)) {
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
      colmd = new ColumnMetaData();
    } else if (columnContainer.getColumnMetaData().length < 1) {
      colmd = new ColumnMetaData();
    } else {
      colmd = columnContainer.getColumnMetaData()[0];
    }
    if (colmd.getName() == null) {
      // Provide default column naming if none is defined
      if (refTable != null) {
        colmd.setName(storeMgr.getIdentifierFactory()
            .newDatastoreFieldIdentifier(refTable.getIdentifier().getIdentifierName(),
                this.storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(OID.class),
                FieldRole.ROLE_OWNER).getIdentifierName());
      } else {
        colmd.setName(
            storeMgr.getIdentifierFactory().newDatastoreFieldIdentifier(identifier.getIdentifierName(),
                this.storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(OID.class),
                FieldRole.ROLE_NONE).getIdentifierName());
      }
    }

    // Add the datastore identity column as the PK
    DatastoreField idColumn = addDatastoreField(OID.class.getName(),
        storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN, colmd.getName()),
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

    dba.getMappingManager(storeMgr)
        .createDatastoreMapping(datastoreIDMapping, idColumn, poidClass.getName());

    // Handle any auto-increment requirement
    if (isObjectIDDatastoreAttributed()) {
      // Only the base class can be autoincremented
//      idColumn.setAutoIncrement(true);
    }
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
      if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE) {
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
              throw new NucleusException("Invalid persistence-modifier for field " + mmd.getName()).setFatal();
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
              throw new NucleusException("Invalid persistence-modifier for field" + fmd.getName()).setFatal();
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
                  throw new NucleusException("Invalid persistence-modifier for field " + fmd.getName()).setFatal();
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
        JavaTypeMapping fieldMapping = dba.getMappingManager(storeMgr)
            .getMapping(this, fieldsToAdd[i], dba, clr, FieldRole.ROLE_FIELD);
        addFieldMapping(fieldMapping);
        pkMappings[i] = fieldMapping;
      }
    }
    initializeIDMapping();
  }

  private void initializeDescriminatorMapping() {
    initializeDescriminatorMapping(cmd.getDiscriminatorMetaDataForTable());
  }

  private void initializeDescriminatorMapping(DiscriminatorMetaData dismd) {
    if (dismd != null) {
      discriminatorMetaData = dismd;
      if (dismd.getStrategy() == DiscriminatorStrategy.CLASS_NAME) {
        discriminatorMapping = new DiscriminatorStringMapping(
            dba, this, dba.getMappingManager(storeMgr).getMapping(String.class));
      } else if (dismd.getStrategy() == DiscriminatorStrategy.VALUE_MAP) {
        ColumnMetaData disColmd = dismd.getColumnMetaData();
        if (disColmd != null && disColmd.getJdbcType() != null) {
          if (disColmd.getJdbcType().equalsIgnoreCase("INTEGER")
              || disColmd.getJdbcType().equalsIgnoreCase("BIGINT")
              || disColmd.getJdbcType().equalsIgnoreCase("NUMERIC")) {
            discriminatorMapping = new DiscriminatorLongMapping(dba, this, dba
                .getMappingManager(storeMgr).getMapping(Long.class));
          } else {
            discriminatorMapping = new DiscriminatorStringMapping(
                dba, this, dba.getMappingManager(storeMgr).getMapping(String.class));
          }
        } else {
          discriminatorMapping = new DiscriminatorStringMapping(
              dba, this, dba.getMappingManager(storeMgr).getMapping(String.class));
        }
      }
    }
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
      JavaTypeMapping mapping = refTable.getMemberMapping(fmd);
      if (mapping == null) {
        //probably due to invalid metadata defined by the user
        throw new NucleusUserException("Cannot find mapping for field " + fmd.getFullFieldName() +
            " in table " + refTable.toString() + " " +
            StringUtils.objectArrayToString(refTable.getDatastoreFields()));
      }

      JavaTypeMapping masterMapping = storeMgr.getMappingManager()
          .getMapping(clr.classForName(mapping.getType()));
      masterMapping.setMemberMetaData(fmd); // Update field info in mapping
      pkMappings[i] = masterMapping;

      // Loop through each id column in the reference table and add the same here
      // applying the required names from the columnContainer
      for (int j = 0; j < mapping.getNumberOfDatastoreFields(); j++) {
        JavaTypeMapping m = masterMapping;
        DatastoreField refColumn = mapping.getDataStoreMapping(j).getDatastoreField();
        if (mapping instanceof PersistenceCapableMapping) {
          m = storeMgr.getMappingManager()
              .getMapping(clr.classForName(refColumn.getJavaTypeMapping().getType()));
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
              storeMgr.getIdentifierFactory().newIdentifier(IdentifierType.COLUMN,
                  userdefinedColumn.getName()),
              m, refColumn.getColumnMetaData());
        } else {
          // No name provided so take same as superclass
          idColumn = addDatastoreField(refColumn.getStoredJavaType(), refColumn.getIdentifier(),
              m, refColumn.getColumnMetaData());
        }
        if (mapping != null
            && mapping.getDataStoreMapping(j).getDatastoreField().getColumnMetaData() != null) {
          refColumn.copyConfigurationTo(idColumn);
        }
        idColumn.setAsPrimaryKey();

        // Set the column type based on the field.getType()
        getStoreManager().getMappingManager()
            .createDatastoreMapping(m, idColumn, refColumn.getJavaTypeMapping().getType());
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
        consumer.consumeMapping(getMemberMapping(pkfmd), pkfmd);
      }
    }
  }

  public void provideNonPrimaryKeyMappings(MappingConsumer consumer) {
    provideNonPrimaryKeyMappings(consumer, false);
  }

  void provideNonPrimaryKeyMappings(MappingConsumer consumer, boolean isEmbedded) {
    consumer.preConsumeMapping(highestFieldNumber + 1);

    Set<Map.Entry<AbstractMemberMetaData, JavaTypeMapping>> entries =
        isEmbedded ? embeddedFieldMappingsMap.entrySet() : fieldMappingsMap.entrySet();
    for (Map.Entry<AbstractMemberMetaData, JavaTypeMapping> entry : entries) {
      if (entry.getValue() != null) {
        if (!entry.getKey().isPrimaryKey() || isEmbedded) {
          consumer.consumeMapping(entry.getValue(), entry.getKey());
        }
      }
    }
  }

  public void provideMappingsForMembers(MappingConsumer consumer, AbstractMemberMetaData[] mmds,
      boolean includeSecondaryTables) {
    for (AbstractMemberMetaData aFieldMetaData : mmds) {
      JavaTypeMapping fieldMapping = fieldMappingsMap.get(aFieldMetaData);
      if (fieldMapping != null) {
        if (!aFieldMetaData.isPrimaryKey()) {
          consumer.consumeMapping(fieldMapping, aFieldMetaData);
        }
      }
    }
  }

  public void provideVersionMappings(MappingConsumer consumer) {
  }

  public void provideDiscriminatorMappings(MappingConsumer consumer) {
    consumer.preConsumeMapping(highestFieldNumber + 1);
    if (getDiscriminatorMapping(false) != null) {
      consumer.consumeMapping(getDiscriminatorMapping(false),
                              MappingConsumer.MAPPING_TYPE_DISCRIMINATOR);
    }
  }

  public void provideUnmappedDatastoreFields(MappingConsumer consumer) {
  }

  public void provideExternalMappings(MappingConsumer consumer, int mappingType) {
    if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_FK) {
      for (AbstractMemberMetaData fmd : externalFkMappings.keySet()) {
        JavaTypeMapping fieldMapping = externalFkMappings.get(fmd);
        if (fieldMapping != null) {
          consumer.consumeMapping(fieldMapping, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
        }
      }
    } else if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX) {
      for (AbstractMemberMetaData fmd : externalOrderMappings.keySet()) {
        JavaTypeMapping fieldMapping = externalOrderMappings.get(fmd);
        if (fieldMapping != null) {
          consumer.consumeMapping(fieldMapping, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
        }
      }
    }
  }

  public JavaTypeMapping getExternalMapping(AbstractMemberMetaData fmd, int mappingType) {
    if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_FK) {
      return getExternalFkMappings().get(fmd);
    } else if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_FK_DISCRIM) {
      return null; //getExternalFkDiscriminatorMappings().get(fmd);
    } else if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX) {
      return getExternalOrderMappings().get(fmd);
    } else {
      return null;
    }
  }

  public AbstractMemberMetaData getMetaDataForExternalMapping(JavaTypeMapping mapping,
      int mappingType) {
    if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_FK) {
      Set entries = getExternalFkMappings().entrySet();
      for (Object entry1 : entries) {
        Map.Entry entry = (Map.Entry) entry1;
        if (entry.getValue() == mapping) {
          return (AbstractMemberMetaData) entry.getKey();
        }
      }
    }
    return null;
  }

  public DiscriminatorMetaData getDiscriminatorMetaData() {
    return discriminatorMetaData;
  }

  public JavaTypeMapping getDiscriminatorMapping(boolean allowSuperclasses) {
    return discriminatorMapping;
  }

  public VersionMetaData getVersionMetaData() {
    return null;
  }

  public JavaTypeMapping getVersionMapping(boolean allowSuperclasses) {
    return null;
  }

  public List<AbstractMemberMetaData> getSameEntityGroupMemberMetaData() {
    return sameEntityGroupMemberMetaData;
  }

  private Map<AbstractMemberMetaData, JavaTypeMapping> getExternalFkMappings() {
    return externalFkMappings;
  }

  /**
   * Accessor for all of the order mappings (used by FK Lists, Collections, Arrays)
   * @return The mappings for the order columns.
   **/
  private Map<AbstractMemberMetaData, JavaTypeMapping> getExternalOrderMappings() {
    return externalOrderMappings;
  }

  /**
   * Execute the callbacks for the classes that this table maps to.
   */
  private void runCallBacks() {
    Collection c = (Collection) callbacks.remove(cmd.getFullClassName());
    if (c == null) {
      return;
    }
    for (Object aC : c) {
      CallBack callback = (CallBack) aC;

      if (callback.fmd.getJoinMetaData() == null) {
        // 1-N FK relationship
        AbstractMemberMetaData ownerFmd = callback.fmd;
        if (ownerFmd.getMappedBy() != null) {
          // Bidirectional (element has a PC mapping to the owner)
          // Check that the "mapped-by" field in the other class actually exists
          AbstractMemberMetaData fmd = cmd.getMetaDataForMember(ownerFmd.getMappedBy());
          if (fmd == null) {
            throw new NucleusUserException(
                String.format(
                    "Unable to find the field \"{0}\" in the class \"{1}\" with a relationship to the field \"{2}\"",
                    ownerFmd.getMappedBy(),
                    cmd.getFullClassName(),
                    ownerFmd.getFullFieldName()));
          }
          // Add the order mapping as necessary
          addOrderMapping(ownerFmd, null);
        } else {
          // Unidirectional (element knows nothing about the owner)
          String ownerClassName = callback.ownerClassName;
          JavaTypeMapping fkMapping = new PersistenceCapableMapping();
          fkMapping.initialize(dba, ownerClassName);
          JavaTypeMapping orderMapping = null;

          // Get the owner id mapping of the "1" end
          JavaTypeMapping ownerIdMapping =
              storeMgr.getDatastoreClass(ownerClassName, clr).getIDMapping();
          ColumnMetaDataContainer colmdContainer = null;
          if (ownerFmd.hasCollection() || ownerFmd.hasArray()) {
            // 1-N Collection/array
            colmdContainer = ownerFmd.getElementMetaData();
          } else if (ownerFmd.hasMap() && ownerFmd.getKeyMetaData() != null
                     && ownerFmd.getKeyMetaData().getMappedBy() != null) {
            // 1-N Map with key stored in the value
            colmdContainer = ownerFmd.getValueMetaData();
          } else if (ownerFmd.hasMap() && ownerFmd.getValueMetaData() != null
                     && ownerFmd.getValueMetaData().getMappedBy() != null) {
            // 1-N Map with value stored in the key
            colmdContainer = ownerFmd.getKeyMetaData();
          }
          CorrespondentColumnsMapper correspondentColumnsMapping =
              new CorrespondentColumnsMapper(colmdContainer, ownerIdMapping, true);
          int countIdFields = ownerIdMapping.getNumberOfDatastoreFields();
          for (int i = 0; i < countIdFields; i++) {
            DatastoreMapping refDatastoreMapping = ownerIdMapping.getDataStoreMapping(i);
            JavaTypeMapping mapping = storeMgr.getMappingManager()
                    .getMapping(refDatastoreMapping.getJavaTypeMapping().getJavaType());
            ColumnMetaData colmd = correspondentColumnsMapping.getColumnMetaDataByIdentifier(
                    refDatastoreMapping.getDatastoreField().getIdentifier());
            if (colmd == null) {
              throw new FatalNucleusUserException(
                  String.format("Primary Key column \"%s\" for table \"%s\" is not mapped.",
                  refDatastoreMapping.getDatastoreField().getIdentifier(),
                  toString()));
            }

            DatastoreIdentifier identifier;
            IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
            if (colmd.getName() == null || colmd.getName().length() < 1) {
              // No user provided name so generate one
              identifier = idFactory.newForeignKeyFieldIdentifier(
                  ownerFmd, null, refDatastoreMapping.getDatastoreField().getIdentifier(),
                  storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(mapping.getJavaType()),
                  FieldRole.ROLE_OWNER);
            } else {
              // User-defined name
              identifier = idFactory.newDatastoreFieldIdentifier(colmd.getName());
            }
            // When we have an inherited relationship we end up
            // trying to add an owner property twice - once from the super-class
            // and once from the sub-class.  This generates an exception for
            // duplicate property names.  To avoid this we check to see if
            // the table already has a property with this name before attempting
            // to add the mapping
            if (!datastorePropertiesByName.containsKey(identifier.getIdentifierName())) {
              DatastoreProperty refColumn =
                  addDatastoreField(mapping.getJavaType().getName(), identifier, mapping, colmd);
              refDatastoreMapping.getDatastoreField().copyConfigurationTo(refColumn);

              if ((colmd.getAllowsNull() == null) ||
                  (colmd.getAllowsNull() != null && colmd.isAllowsNull())) {
                // User either wants it nullable, or havent specified anything, so make it nullable
                refColumn.setNullable();
              }
              AbstractClassMetaData acmd =
                  storeMgr.getMetaDataManager().getMetaDataForClass(ownerIdMapping.getType(), clr);
              // this is needed for one-to-many sets
              addOwningClassMetaData(colmd.getName(), acmd);
              fkMapping.addDataStoreMapping(getStoreManager().getMappingManager()
                  .createDatastoreMapping(mapping, refColumn,
                                          refDatastoreMapping.getJavaTypeMapping().getJavaType().getName()));
              ((PersistenceCapableMapping) fkMapping).addJavaTypeMapping(mapping);
            }
          }

          // Save the external FK
          getExternalFkMappings().put(ownerFmd, fkMapping);

          // Add the order mapping as necessary
          addOrderMapping(ownerFmd, orderMapping);
        }
      }
    }
  }

  private JavaTypeMapping addOrderMapping(AbstractMemberMetaData fmd, JavaTypeMapping orderMapping) {
    boolean needsOrderMapping = false;
    OrderMetaData omd = fmd.getOrderMetaData();
    if (fmd.hasArray()) {
      // Array field always has the index mapping
      needsOrderMapping = true;
    } else if (List.class.isAssignableFrom(fmd.getType())) {
      // List field
      needsOrderMapping = !(omd != null && !omd.isIndexedList());
    } else if (java.util.Collection.class.isAssignableFrom(fmd.getType()) &&
               omd != null && omd.isIndexedList() && omd.getMappedBy() == null) {
      // Collection field with <order> and is indexed list so needs order mapping
      needsOrderMapping = true;
    }

    if (needsOrderMapping) {
      // if the field is list or array type, add index column
      if (orderMapping == null) {
        // Create new order mapping since we need one and we aren't using a shared FK
        orderMapping = addOrderColumn(fmd);
      }
      getExternalOrderMappings().put(fmd, orderMapping);
    }

    return orderMapping;
  }

  /**
   * Adds an ordering column to the element table (this) in inverse list relationships. Used to
   * store the position of the element in the List. If the &lt;order&gt; provides a mapped-by, this
   * will return the existing column mapping.
   *
   * @param fmd The MetaData for the column to map to
   * @return The Mapping for the order column
   */
  private JavaTypeMapping addOrderColumn(AbstractMemberMetaData fmd) {
    Class indexType = Integer.class;
    JavaTypeMapping indexMapping = new IndexMapping();
    indexMapping.initialize(dba, indexType.getName());
    IdentifierFactory idFactory = storeMgr.getIdentifierFactory();
    DatastoreIdentifier indexColumnName = null;
    ColumnMetaData colmd = null;

    // Allow for any user definition in OrderMetaData
    OrderMetaData omd = fmd.getOrderMetaData();
    if (omd != null) {
      colmd =
          (omd.getColumnMetaData() != null && omd.getColumnMetaData().length > 0 ? omd
              .getColumnMetaData()[0] : null);
      if (omd.getMappedBy() != null) {
        // User has defined ordering using the column(s) of an existing field.
        JavaTypeMapping orderMapping = getMemberMapping(omd.getMappedBy());
        if (orderMapping == null) {
          throw new NucleusUserException(String.format(
              "Field \"{0}\" has an <order> defined to be persisted into the columns in the element table for element field \"{1}\". This field is not found in the element class.",
              fmd.getFullFieldName(), omd.getMappedBy()));
        }
        if (!(orderMapping instanceof IntegerMapping) && !(orderMapping instanceof LongMapping)) {
          throw new NucleusUserException(
              String.format(
                  "Field \"{0}\" has an <order> defined to be persisted into the column of field \"{1}\". This field is of an invalid type. Must be an int/Integer.",
                  fmd.getFullFieldName(), omd.getMappedBy()));
        }
        return orderMapping;
      }

      String colName;
      if (omd.getColumnMetaData() != null && omd.getColumnMetaData().length > 0
          && omd.getColumnMetaData()[0].getName() != null) {
        // User-defined name so create an identifier using it
        colName = omd.getColumnMetaData()[0].getName();
        indexColumnName = idFactory.newDatastoreFieldIdentifier(colName);
      }
    }
    if (indexColumnName == null) {
      // No name defined so generate one
      indexColumnName = idFactory.newForeignKeyFieldIdentifier(
          fmd, null, null,
          storeMgr.getOMFContext().getTypeManager().isDefaultEmbeddedType(indexType),
          FieldRole.ROLE_INDEX);
    }

    // if the relationship is in a base class with multiple subclasses, each
    // subclass will try to add the index column.  We need to avoid adding
    // the same column twice.
    DatastoreField column = datastorePropertiesByName.get(indexColumnName.getIdentifierName());
    if (column == null) {
      column = addDatastoreField(indexType.getName(), indexColumnName, indexMapping, colmd);
    }
    if (colmd == null || (colmd.getAllowsNull() == null) ||
        (colmd.getAllowsNull() != null && colmd.isAllowsNull())) {
      // User either wants it nullable, or havent specified anything, so make it nullable
      column.setNullable();
    }

    DatastoreFKMapping fkMapping =
        (DatastoreFKMapping) storeMgr.getMappingManager().createDatastoreMapping(
            indexMapping, column, indexType.getName());
    DatastoreProperty field = fkMapping.getDatastoreField();
    DatastoreTable elementTable = field.getDatastoreContainerObject();
    PropertyMetaData pmd = new PropertyMetaData(elementTable.getClassMetaData(), indexColumnName.getIdentifierName());
    field.setMemberMetaData(pmd);
    return indexMapping;
  }

  public boolean isParentKeyProvider(AbstractMemberMetaData ammd) {
    return ammd.equals(parentMappingField);
  }

  void provideParentMappingField(InsertMappingConsumer consumer) {
    if (parentMappingField != null) {
      consumer.setParentMappingField(parentMappingField);
    }
  }

  /**
   * Callbacks is used for inverse relationships to run some operation in the target table. The
   * operation is creation of columns, indexes, or whatever needed.
   */
  private static class CallBack {

    final AbstractMemberMetaData fmd;
    final String ownerClassName;

    /**
     * Default constructor
     *
     * @param fmd The FieldMetaData
     * @param ownerClassName The concrete type of the relationship
     */
    public CallBack(AbstractMemberMetaData fmd, String ownerClassName) {
      this.fmd = fmd;
      this.ownerClassName = ownerClassName;
    }
  }

  public String toString() {
    return cmd.toString();
  }

  public AbstractClassMetaData getClassMetaData() {
    return cmd;
  }

  void addOwningClassMetaData(String columnName, AbstractClassMetaData acmd) {
    owningClassMetaData.put(columnName, acmd);
  }

  AbstractClassMetaData getOwningClassMetaDataForColumn(String col) {
    return owningClassMetaData.get(col);
  }
}
