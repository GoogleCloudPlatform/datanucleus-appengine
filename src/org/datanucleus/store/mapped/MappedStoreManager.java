/**********************************************************************
Copyright (c) 2007 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.mapped;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.datanucleus.ClassConstants;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.BackedSCOStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.exceptions.NoTableManagedException;
import org.datanucleus.store.mapped.mapping.ArrayMapping;
import org.datanucleus.store.mapped.mapping.CollectionMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MapMapping;
import org.datanucleus.store.mapped.mapping.MappingManager;
import org.datanucleus.store.mapped.mapping.PersistableMapping;
import org.datanucleus.store.scostore.ArrayStore;
import org.datanucleus.store.scostore.CollectionStore;
import org.datanucleus.store.scostore.ListStore;
import org.datanucleus.store.scostore.MapStore;
import org.datanucleus.store.scostore.PersistableRelationStore;
import org.datanucleus.store.scostore.SetStore;
import org.datanucleus.store.scostore.Store;
import org.datanucleus.store.types.IncompatibleFieldTypeException;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * Manager for a datastore that has a schema and maps classes to associated objects in the datastore.
 * Datastores such as RDBMS will extend this type of StoreManager.
 * <p>
 * In a "mapped" datastore, a class is associated with a DatastoreClass. Similarly a field of a class is associated
 * with a DatastoreField. Where a relation is stored separately this is associated with a DatastoreContainerObject.
 * In an RDBMS datastore this will be
 * <ul>
 * <li>class <-> table</li>
 * <li>field <-> column</li>
 * <li>relation <-> join-table / foreign-key</li>
 * </ul>
 * </p>
 * <p>
 * Includes a "schemaLock" with the intention of restricting access to managed schema information whilst
 * the schema is being updated (new class being added, which potentially adds external FKs and ordering columns
 * to existing tables).
 * </p>
 */
public abstract class MappedStoreManager extends AbstractStoreManager implements BackedSCOStoreManager
{
    /** Adapter for the datastore being used. */
    protected DatastoreAdapter dba;

    /** Factory for identifiers for this datastore. */
    protected IdentifierFactory identifierFactory;

    /** Catalog name for the database (if supported). */
    protected String catalogName = null;

    /** Schema name for the database (if supported). */
    protected String schemaName = null;

    /**
     * Map of all managed datastore containers (tables) keyed by the datastore identifier.
     * Only currently used for storing SequenceTable.
     */
    protected Map<DatastoreIdentifier, DatastoreContainerObject> datastoreContainerByIdentifier = new ConcurrentHashMap();

    /** TypeManager for mapped information. */
    protected MappedTypeManager mappedTypeMgr = null;

    /** Manager for the mapping between Java and datastore types. */
    protected MappingManager mappingManager;

    /**
     * Map of DatastoreClass keyed by StateManager, for objects currently being inserted.
     * Defines to what level an object is inserted in the datastore.
     */
    protected Map<ObjectProvider, DatastoreClass> insertedDatastoreClassByStateManager = new ConcurrentHashMap();

    /** 
     * Lock object aimed at providing a lock on the schema definition managed here, preventing
     * reads while it is being updated etc.
     */
    protected ReadWriteLock schemaLock = new ReentrantReadWriteLock();

    /**
     * Constructor. Stores the basic information required for the datastore management.
     * @param key Key for this StoreManager
     * @param clr the ClassLoaderResolver
     * @param nucleusContext The corresponding context.
     * @param props Properties for the datastore
     */
    protected MappedStoreManager(String key, ClassLoaderResolver clr, NucleusContext nucleusContext, Map<String, Object> props)
    {
        super(key, clr, nucleusContext, props);

        mappedTypeMgr = new MappedTypeManager(nucleusContext);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#close()
     */
    public void close()
    {
        dba = null;
        super.close();
    }

    /**
     * Method to create the IdentifierFactory to be used by this store.
     * Relies on the datastore adapter existing before creation
     * @param nucleusContext context
     */
    protected void initialiseIdentifierFactory(NucleusContext nucleusContext)
    {
        if (dba == null)
        {
            throw new NucleusException("DatastoreAdapter not yet created so cannot create IdentifierFactory!");
        }

        String idFactoryName = getStringProperty("datanucleus.identifierFactory");
        String idFactoryClassName = nucleusContext.getPluginManager().getAttributeValueForExtension("org.datanucleus.store_identifierfactory", 
            "name", idFactoryName, "class-name");
        if (idFactoryClassName == null)
        {
            throw new NucleusUserException(LOCALISER.msg("039003", idFactoryName)).setFatal();
        }

        try
        {
            // Create the control properties for identifier generation
            Map props = new HashMap();
            if (catalogName != null)
            {
                props.put("DefaultCatalog", catalogName);
            }
            if (schemaName != null)
            {
                props.put("DefaultSchema", schemaName);
            }
            String val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_CASE);
            if (val != null)
            {
                props.put("RequiredCase", val);
            }
            else
            {
                props.put("RequiredCase", getDefaultIdentifierCase());
            }
            val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_WORD_SEPARATOR);
            if (val != null)
            {
                props.put("WordSeparator", val);
            }
            val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_TABLE_PREFIX);
            if (val != null)
            {
                props.put("TablePrefix", val);
            }
            val = getStringProperty(PropertyNames.PROPERTY_IDENTIFIER_TABLE_SUFFIX);
            if (val != null)
            {
                props.put("TableSuffix", val);
            }

            // Create the IdentifierFactory
            Class[] argTypes = new Class[] {DatastoreAdapter.class, ClassConstants.CLASS_LOADER_RESOLVER, Map.class};
            Object[] args = new Object[] {dba, nucleusContext.getClassLoaderResolver(null), props};
            identifierFactory = (IdentifierFactory)nucleusContext.getPluginManager().createExecutableExtension(
                "org.datanucleus.store_identifierfactory", "name", idFactoryName, "class-name", 
                argTypes, args);
        }
        catch (ClassNotFoundException cnfe)
        {
            throw new NucleusUserException(LOCALISER.msg("039004", idFactoryName, idFactoryClassName), cnfe).setFatal();
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception creating IdentifierFactory", e);
            throw new NucleusException(LOCALISER.msg("039005", idFactoryClassName), e).setFatal();
        }
    }

    /**
     * Accessor for whether this value strategy is supported.
     * Overrides the setting in the superclass for identity/sequence if the adapter doesn't 
     * support them.
     * @param strategy The strategy
     * @return Whether it is supported.
     */
    public boolean supportsValueStrategy(String strategy)
    {
        // "identity" doesn't have an explicit entry in plugin since uses datastore capabilities
        if (strategy.equalsIgnoreCase("IDENTITY") || super.supportsValueStrategy(strategy))
        {
            if (strategy.equalsIgnoreCase("IDENTITY") && !dba.supportsOption(DatastoreAdapter.IDENTITY_COLUMNS))
            {
                return false; // adapter doesn't support identity so we don't
            }
            else if (strategy.equalsIgnoreCase("SEQUENCE") && !dba.supportsOption(DatastoreAdapter.SEQUENCES))
            {
                return false; // adapter doesn't support sequences so we don't
            }
            return true;
        }
        return false;
    }

    /**
     * Accessor for the manager of mapped type information.
     * @return MappedTypeManager
     */
    public MappedTypeManager getMappedTypeManager()
    {
        return mappedTypeMgr;
    }

    /**
     * Accessor for the factory for creating identifiers (table/column names etc).
     * @return Identifier factory
     */
    public IdentifierFactory getIdentifierFactory()
    {
        return identifierFactory;
    }

    /**
     * Gets the DatastoreAdapter to use for this store.
     * @return Returns the DatastoreAdapter
     */
    public DatastoreAdapter getDatastoreAdapter()
    {
        return dba;
    }

    /**
     * Gets the MappingManager to use for this store.
     * @return Returns the MappingManager.
     */
    public MappingManager getMappingManager()
    {
        if (mappingManager == null)
        {
            mappingManager = dba.getMappingManager(this);
        }
        return mappingManager;
    }

    /**
     * Called by Mapping objects to request the creation of a DatastoreObject (table).
     * @param mmd The metadata describing the member
     * @param clr The ClassLoaderResolver
     * @return The DatastoreContainerObject
     */
    public abstract DatastoreContainerObject newJoinDatastoreContainerObject(AbstractMemberMetaData mmd, 
            ClassLoaderResolver clr);

    /**
     * Utility to return all StoreData for a Datastore Container identifier.
     * Returns StoreData with this table identifier and where the class is the owner of the table.
     * @param tableIdentifier Identifier for the table
     * @return The StoreData for this table (if managed).
     */
    public synchronized StoreData[] getStoreDataForDatastoreContainerObject(DatastoreIdentifier tableIdentifier)
    {
        return storeDataMgr.getStoreDataForProperties("tableId", tableIdentifier, "table-owner", "true");
    }

    /**
     * Returns the datastore container (table) for the specified field. 
     * Returns 'null' if the field is not (yet) known to the store manager.
     * @param mmd The metadata for the field.
     * @return The corresponding datastore container, or 'null'.
     */
    public DatastoreContainerObject getDatastoreContainerObject(AbstractMemberMetaData mmd)
    {
        schemaLock.readLock().lock();
        try
        {
            StoreData sd = storeDataMgr.get(mmd);
            if (sd != null && sd instanceof MappedStoreData)
            {
                return ((MappedStoreData)sd).getDatastoreContainerObject();
            }
            else
            {
                return null;
            }
        }
        finally
        {
            schemaLock.readLock().unlock();
        }
    }

    /**
     * Method to add a datastore container to the managed datastore classes.
     * @param table The datastore container
     */
    public void addDatastoreContainer(DatastoreContainerObject table)
    {
        if (table != null && datastoreContainerByIdentifier.get(table.getIdentifier()) == null)
        {
            datastoreContainerByIdentifier.put(table.getIdentifier(), table);
        }
    }

    /**
     * Returns the primary datastore table serving as backing for the given class. 
     * If the class is not yet known to the store manager, {@link #addClass}is called
     * to add it. Classes which have inheritance strategy of "new-table" and
     * "superclass-table" will return a table here, whereas "subclass-table" will
     * return null since it doesn't have a table as such.
     * <p>
     * @param className Name of the class whose table is be returned.
     * @param clr The ClassLoaderResolver
     * @return The corresponding class table.
     * @exception NoTableManagedException If the given class has no table managed in the database.
     */
    public DatastoreClass getDatastoreClass(String className, ClassLoaderResolver clr)
    {
        DatastoreClass ct = null;
        if (className == null)
        {
            NucleusLogger.PERSISTENCE.error(LOCALISER.msg("032015"));
            return null;
        }

        schemaLock.readLock().lock();
        try
        {
            StoreData sd = storeDataMgr.get(className);
            if (sd != null && sd instanceof MappedStoreData)
            {
                ct = (DatastoreClass) ((MappedStoreData)sd).getDatastoreContainerObject();
                if (ct != null)
                {
                    // Class known about
                    return ct;
                }
            }
        }
        finally
        {
            schemaLock.readLock().unlock();
        }

        // Class not known so consider adding it to our list of supported classes.
        // Currently we only consider PC classes
        boolean toBeAdded = false;
        if (clr != null)
        {
            Class cls = clr.classForName(className);
            ApiAdapter api = getApiAdapter();
            if (cls != null && !cls.isInterface() && api.isPersistable(cls))
            {
                toBeAdded = true;
            }
        }
        else
        {
            toBeAdded = true;
        }

        boolean classKnown = false;
        if (toBeAdded)
        {
            // Add the class to our supported list
            addClass(className, clr);

            // Retry
            schemaLock.readLock().lock();
            try
            {
                StoreData sd = storeDataMgr.get(className);
                if (sd != null && sd instanceof MappedStoreData)
                {
                    classKnown = true;
                    ct = (DatastoreClass) ((MappedStoreData)sd).getDatastoreContainerObject();
                }
            }
            finally
            {
                schemaLock.readLock().unlock();
            }
        }

        // Throw an exception if class still not known and no table
        // Note : "subclass-table" inheritance strategies will return null from this method
        if (!classKnown && ct == null)
        {
            throw new NoTableManagedException(className);
        }

        return ct;
    }

    /**
     * Returns the datastore table having the given identifier.
     * Returns 'null' if no such table is (yet) known to the store manager.
     * @param name The identifier name of the table.
     * @return The corresponding table, or 'null'
     */
    public DatastoreClass getDatastoreClass(DatastoreIdentifier name)
    {
        schemaLock.readLock().lock();
        try
        {
            Iterator iterator = storeDataMgr.getManagedStoreData().iterator();
            while (iterator.hasNext())
            {
                StoreData sd = (StoreData) iterator.next();
                if (sd instanceof MappedStoreData)
                {
                    MappedStoreData tsd = (MappedStoreData)sd;
                    if (tsd.hasTable() && tsd.getDatastoreIdentifier().equals(name))
                    {
                        return (DatastoreClass) tsd.getDatastoreContainerObject();
                    }
                }
            }
            return null;
        }
        finally
        {
            schemaLock.readLock().unlock();
        }
    }

    /**
     * Utility to navigate the inheritance hierarchy to find the base class that defines the primary keys
     * for this tree. This will either go up to the next class in the hierarchy that has a table
     * OR go up to the base class, whichever is first.
     * @param cmd AbstractClassMetaData for this class
     * @param clr The ClassLoaderResolver
     * @return The AbstractClassMetaData for the class defining the primary keys
     */
    public AbstractClassMetaData getClassWithPrimaryKeyForClass(AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        if (cmd == null)
        {
            return null;
        }

        // Base class will have primary key fields
        if (cmd.getSuperAbstractClassMetaData() == null)
        {
            return cmd;
        }
        // Class has its own table so has the PK fields already
        else if (getDatastoreClass(cmd.getFullClassName(), clr) != null)
        {
            return cmd;
        }

        return getClassWithPrimaryKeyForClass(cmd.getSuperAbstractClassMetaData(), clr);
    }

    /**
     * Method to return the class(es) that has a table managing the persistence of
     * the fields of the supplied class. For the 3 inheritance strategies, the following
     * occurs :-
     * <UL>
     * <LI>new-table : will return the same ClassMetaData</LI>
     * <LI>subclass-table : will return all subclasses that have a table managing its fields</LI>
     * <LI>superclass-table : will return the next superclass that has a table</LI>
     * </UL> 
     * @param cmd The supplied class.
     * @param clr ClassLoader resolver
     * @return The ClassMetaData's managing the fields of the supplied class
     */
    public AbstractClassMetaData[] getClassesManagingTableForClass(AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        // Null input, so just return null;
        if (cmd == null)
        {
            return null;
        }

        if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.COMPLETE_TABLE ||
            cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.NEW_TABLE)
        {
            // Class manages a table so return the classes metadata.
            return new AbstractClassMetaData[] {cmd};
        }
        else if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)
        {
            // Check the subclasses that we have metadata for and make sure they are managed before proceeding
            String[] subclasses = getMetaDataManager().getSubclassesForClass(cmd.getFullClassName(), true);
            if (subclasses != null)
            {
                for (int i=0;i<subclasses.length;i++)
                {
                    if (!storeDataMgr.managesClass(subclasses[i]))
                    {
                        addClass(subclasses[i], clr);
                    }
                }
            }

            // Find subclasses who manage the tables winto which our class is persisted
            HashSet managingClasses=new HashSet();
            Iterator managedClassesIter = storeDataMgr.getManagedStoreData().iterator();
            while (managedClassesIter.hasNext())
            {
                StoreData data = (StoreData)managedClassesIter.next();
                if (data.isFCO() && ((AbstractClassMetaData)data.getMetaData()).getSuperAbstractClassMetaData() != null &&
                    ((AbstractClassMetaData)data.getMetaData()).getSuperAbstractClassMetaData().getFullClassName().equals(cmd.getFullClassName()))
                {
                    AbstractClassMetaData[] superCmds = getClassesManagingTableForClass((AbstractClassMetaData)data.getMetaData(), clr);
                    if (superCmds != null)
                    {
                        for (int i=0;i<superCmds.length;i++)
                        {
                            managingClasses.add(superCmds[i]);
                        }
                    }
                }
            }

            Iterator managingClassesIter = managingClasses.iterator();
            AbstractClassMetaData managingCmds[] = new AbstractClassMetaData[managingClasses.size()];
            int i=0;
            while (managingClassesIter.hasNext())
            {
                managingCmds[i++] = (AbstractClassMetaData)(managingClassesIter.next());
            }
            return managingCmds;
        }
        else if (cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUPERCLASS_TABLE)
        {
            // Fields managed by superclass, so recurse to that
            return getClassesManagingTableForClass(cmd.getSuperAbstractClassMetaData(), clr);
        }
        return null;
    }

    /**
     * Accessor for whether the specified field of the object is inserted in the datastore yet.
     * @param sm StateManager for the object
     * @param fieldNumber (Absolute) field number for the object
     * @return Whether it is persistent
     */
    public boolean isObjectInserted(ObjectProvider sm, int fieldNumber)
    {
        if (sm == null)
        {
            return false;
        }
        if (!sm.isInserting())
        {
            // StateManager isn't inserting so must be persistent
            return true;
        }

        DatastoreClass latestTable = insertedDatastoreClassByStateManager.get(sm);
        if (latestTable == null)
        {
            // Not yet inserted anything
            return false;
        }

        AbstractMemberMetaData mmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd == null)
        {
            // Specified field doesn't exist for this object type!
            return false;
        }

        String className = mmd.getClassName();
        if (mmd.isPrimaryKey())
        {
            // PK field so need to check if the latestTable manages the actual class here
            className = sm.getObject().getClass().getName();
        }

        DatastoreClass datastoreCls = latestTable;
        while (datastoreCls != null)
        {
            if (datastoreCls.managesClass(className))
            {
                return true; // This datastore class manages the specified class so it is inserted
            }
            datastoreCls = datastoreCls.getSuperDatastoreClass();
        }
        return false;
    }

    /**
     * Returns whether this object is inserted in the datastore far enough to be considered to be the
     * supplied type. For example if we have base class A, B extends A and this object is a B, and we 
     * pass in A here then this returns whether the A part of the object is now inserted.
     * @param sm StateManager for the object
     * @param className Name of class that we want to check the insertion level for.
     * @return Whether the object is inserted in the datastore to this level
     */
    public boolean isObjectInserted(ObjectProvider sm, String className)
    {
        if (sm == null)
        {
            return false;
        }
        if (!sm.isInserting())
        {
            return false;
        }

        DatastoreClass latestTable = insertedDatastoreClassByStateManager.get(sm);
        if (latestTable != null)
        {
            DatastoreClass datastoreCls = latestTable;
            while (datastoreCls != null)
            {
                if (datastoreCls.managesClass(className))
                {
                    return true; // This datastore class manages the specified class so it is inserted
                }
                datastoreCls = datastoreCls.getSuperDatastoreClass();
            }
        }

        return false;
    }

    /**
     * Accessor for the backing store for the specified member.
     * @param clr The ClassLoaderResolver
     * @param mmd metadata for the member to be persisted by this Store
     * @param type instantiated type or prefered type
     * @return The backing store
     */
    public Store getBackingStoreForField(ClassLoaderResolver clr, AbstractMemberMetaData mmd, Class type)
    {
        if (mmd == null || (mmd != null && mmd.isSerialized()))
        {
            return null;
        }

        if (mmd.getMap() != null)
        {
            assertCompatibleFieldType(mmd, clr, type, MapMapping.class);
            return getBackingStoreForMap(mmd, clr);
        }
        else if (mmd.getArray() != null)
        {
            assertCompatibleFieldType(mmd, clr, type, ArrayMapping.class);
            return getBackingStoreForArray(mmd, clr);
        }
        else if (mmd.getCollection() != null)
        {
            assertCompatibleFieldType(mmd, clr, type, CollectionMapping.class);
            return getBackingStoreForCollection(mmd, clr, type);
        }
        else
        {
            assertCompatibleFieldType(mmd, clr, type, PersistableMapping.class);
            return getBackingStoreForPersistableRelation(mmd, clr, type);
        }
    }

    /**
     * Asserts the current mapping for the member is the one expected.
     * @param mmd MetaData for the member
     * @param clr ClassLoader resolver
     * @param type Type of object
     * @param expectedMappingType Mapping type expected
     */
    private void assertCompatibleFieldType(AbstractMemberMetaData mmd, ClassLoaderResolver clr, Class type,
            Class expectedMappingType)
    {
        DatastoreClass ownerTable = getDatastoreClass(mmd.getClassName(), clr);
        if (ownerTable == null)
        {
            // Class doesn't manage its own table (uses subclass-table, or superclass-table?)
            AbstractClassMetaData fieldTypeCmd = getMetaDataManager().getMetaDataForClass(mmd.getClassName(), clr);
            AbstractClassMetaData[] tableOwnerCmds = getClassesManagingTableForClass(fieldTypeCmd, clr);
            if (tableOwnerCmds != null && tableOwnerCmds.length == 1)
            {
                ownerTable = getDatastoreClass(tableOwnerCmds[0].getFullClassName(), clr);
            }
        }

        if (ownerTable != null)
        {
            JavaTypeMapping m = ownerTable.getMemberMapping(mmd);
            if (!expectedMappingType.isAssignableFrom(m.getClass()))
            {
                throw new IncompatibleFieldTypeException(mmd.getFullFieldName(),
                    type.getName(), mmd.getTypeName());
            }
        }
    }

    /**
     * Method to return a backing store for a Collection, consistent with this store and the instantiated type.
     * @param mmd MetaData for the field that has this collection
     * @param clr ClassLoader resolver
     * @return The backing store of this collection in this store
     */
    private CollectionStore getBackingStoreForCollection(AbstractMemberMetaData mmd,
            ClassLoaderResolver clr, Class type)
    {
        CollectionStore store = null;
        DatastoreContainerObject datastoreTable = getDatastoreContainerObject(mmd);
        if (type == null)
        {
            // No type to base it on so create it based on the field declared type
            if (datastoreTable == null)
            {
                // We need a "FK" relation.
                if (List.class.isAssignableFrom(mmd.getType()))
                {
                    store = newFKListStore(mmd, clr);
                }
                else
                {
                    store = newFKSetStore(mmd, clr);
                }
            }
            else
            {
                // We need a "JoinTable" relation.
                if (List.class.isAssignableFrom(mmd.getType()))
                {
                    store = newJoinListStore(mmd, clr, datastoreTable);
                }
                else
                {
                    store = newJoinSetStore(mmd, clr, datastoreTable);
                }
            }
        }
        else
        {
            // Instantiated type specified so use it to pick the associated backing store
            if (datastoreTable == null)
            {
                if (SCOUtils.isListBased(type))
                {
                    // List required
                    store = newFKListStore(mmd, clr);
                }
                else
                {
                    // Set required
                    store = newFKSetStore(mmd, clr);
                }
            }
            else
            {
                if (SCOUtils.isListBased(type))
                {
                    // List required
                    store = newJoinListStore(mmd, clr, datastoreTable);
                }
                else
                {
                    // Set required
                    store = newJoinSetStore(mmd, clr, datastoreTable);
                }
            }
        }
        return store;
    }

    /**
     * Method to return a backing store for a Map, consistent with this store and the instantiated type.
     * @param mmd MetaData for the field that has this map
     * @param clr ClassLoader resolver
     * @return The backing store of this map in this store
     */
    private MapStore getBackingStoreForMap(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        MapStore store = null;
        DatastoreContainerObject datastoreTable = getDatastoreContainerObject(mmd);
        if (datastoreTable == null)
        {
            store = newFKMapStore(mmd, clr);
        }
        else
        {
            store = newJoinMapStore(mmd, clr, datastoreTable);
        }
        return store;
    }

    /**
     * Method to return a backing store for an array, consistent with this store and the instantiated type.
     * @param mmd MetaData for the field/property that has this array
     * @param clr ClassLoader resolver
     * @return The backing store of this array in this store
     */
    private ArrayStore getBackingStoreForArray(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        ArrayStore store;
        DatastoreContainerObject datastoreTable = getDatastoreContainerObject(mmd);
        if (datastoreTable != null)
        {
            store = newJoinArrayStore(mmd, clr, datastoreTable);
        }
        else
        {
            store = newFKArrayStore(mmd, clr);
        }
        return store;
    }

    /**
     * Method to return a backing store for a persistable relation (N-1 uni via join).
     * @param mmd MetaData for the member being stored
     * @param clr ClassLoader resolver
     * @return The backing store of this persistable relation in this store
     */
    private PersistableRelationStore getBackingStoreForPersistableRelation(AbstractMemberMetaData mmd,
            ClassLoaderResolver clr, Class type)
    {
        PersistableRelationStore store = null;
        DatastoreContainerObject datastoreTable = getDatastoreContainerObject(mmd);
        store = newPersistableRelationStore(mmd, clr, datastoreTable);
        return store;
    }

    /**
     * Method to return a FieldManager for extracting information from the supplied results.
     * @param sm StateManager for the object
     * @param resultSet The results
     * @param resultMappings Mappings of the results for this class
     * @return FieldManager to use
     */
    public abstract FieldManager getFieldManagerForResultProcessing(ObjectProvider sm, Object resultSet, 
            StatementClassMapping resultMappings);

    /**
     * Method to return a FieldManager for extracting information from the supplied results.
     * @param ec Execution Context
     * @param resultSet The results
     * @param resultMappings Mappings of the results for this class
     * @param cmd Metadata for the class of the object being created for this row
     * @return FieldManager to use
     */
    public abstract FieldManager getFieldManagerForResultProcessing(ExecutionContext ec, Object resultSet, 
            StatementClassMapping resultMappings, AbstractClassMetaData cmd);

    /**
     * Method to return the value from the results at the specified position.
     * @param resultSet The results
     * @param mapping The mapping
     * @param position The position
     * @return The value at that position
     */
    public abstract Object getResultValueAtPosition(Object resultSet, JavaTypeMapping mapping, int position);

    /**
     * Convenience method to return if the datastore supports batching and the user wants batching.
     * @return If batching of statements is permissible
     */
    public abstract boolean allowsBatching();

    /**
     * Method to create a backing store for an array managed via FK.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @return The backing store
     */
    protected ArrayStore newFKArrayStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        throw new UnsupportedOperationException("FK Arrays not supported.");
    }

    /**
     * Method to create a backing store for a list managed via FK.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @return The backing store
     */
    protected ListStore newFKListStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        throw new UnsupportedOperationException("FK Lists not supported.");
    }

    /**
     * Method to create a backing store for a set managed via FK.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @return The backing store
     */
    protected SetStore newFKSetStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        throw new UnsupportedOperationException("FK Sets not supported.");
    }

    /**
     * Method to create a backing store for a map managed via FK.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @return The backing store
     */
    protected MapStore newFKMapStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        throw new UnsupportedOperationException("FK Maps not supported.");
    }

    /**
     * Method to create a backing store for an array managed via join.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @param joinTable The join table
     * @return The backing store
     */
    protected ArrayStore newJoinArrayStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr, 
            DatastoreContainerObject joinTable)
    {
        throw new UnsupportedOperationException("Join Arrays not supported.");
    }

    /**
     * Method to create a backing store for a map managed via join.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @param joinTable The join table
     * @return The backing store
     */
    protected MapStore newJoinMapStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr, 
            DatastoreContainerObject joinTable) 
    {
        throw new UnsupportedOperationException("Join Maps not supported.");
    }

    /**
     * Method to create a backing store for a list managed via join.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @param joinTable The join table
     * @return The backing store
     */
    protected ListStore newJoinListStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr,
            DatastoreContainerObject joinTable)
    {
        throw new UnsupportedOperationException("Join Lists not supported.");
    }

    /**
     * Method to create a backing store for a set managed via join.
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @param joinTable The join table
     * @return The backing store
     */
    protected SetStore newJoinSetStore(AbstractMemberMetaData mmd, ClassLoaderResolver clr,
            DatastoreContainerObject joinTable)
    {
        throw new UnsupportedOperationException("Join Sets not supported.");
    }

    /**
     * Method to create a backing store for a "persistable relation" (N-1 uni via join).
     * @param mmd Metadata for the member
     * @param clr ClassLoader resolver
     * @param joinTable The join table
     * @return The backing store
     */
    protected PersistableRelationStore newPersistableRelationStore(AbstractMemberMetaData mmd, 
            ClassLoaderResolver clr, DatastoreContainerObject joinTable)
    {
        throw new UnsupportedOperationException("Join N-1 relations not supported.");
    }

    /**
     * Method to return the default identifier case.
     * @return Identifier case to use if not specified by the user
     */
    public String getDefaultIdentifierCase()
    {
        return "UPPERCASE";
    }
}