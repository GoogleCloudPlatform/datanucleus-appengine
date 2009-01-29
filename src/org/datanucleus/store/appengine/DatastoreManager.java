// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ConnectionFactory;
import org.datanucleus.ConnectionFactoryRegistry;
import org.datanucleus.FetchPlan;
import org.datanucleus.ManagedConnection;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.sco.IncompatibleFieldTypeException;
import org.datanucleus.sco.SCOUtils;
import org.datanucleus.store.Extent;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.NucleusConnectionImpl;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.exceptions.NoExtentException;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.FetchStatement;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreData;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.StatementMappingForClass;
import org.datanucleus.store.mapped.StatementMappingIndex;
import org.datanucleus.store.mapped.mapping.ArrayMapping;
import org.datanucleus.store.mapped.mapping.CollectionMapping;
import org.datanucleus.store.mapped.mapping.DatastoreMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MapMapping;
import org.datanucleus.store.mapped.scostore.FKListStore;
import org.datanucleus.store.query.ResultObjectFactory;
import org.datanucleus.store.scostore.ArrayStore;
import org.datanucleus.store.scostore.CollectionStore;
import org.datanucleus.store.scostore.MapStore;
import org.datanucleus.store.scostore.Store;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreManager extends MappedStoreManager {

  /**
   * Construct a DatsatoreManager
   *
   * @param clr The ClassLoaderResolver
   * @param omfContext The OMFContext
   */
  public DatastoreManager(ClassLoaderResolver clr, OMFContext omfContext) {
    // Make sure we add required property values before we invoke
    // out parent's constructor.
    super("appengine", clr, addDefaultPropertyValues(omfContext));

    // Check if datastore api is in CLASSPATH.  Don't let the hard-coded
    // jar name upset you, it's just used for error messages.  The check will
    // succeed so long as the class is available on the classpath
    ClassUtils.assertClassForJarExistsInClasspath(
        clr, "com.google.appengine.api.datastore.DatastoreService", "appengine-api.jar");

    // Handler for persistence process
    persistenceHandler = new DatastorePersistenceHandler(this);
    dba = new DatastoreAdapter();
    initialiseIdentifierFactory(omfContext);
    logConfiguration();
  }

  private static OMFContext addDefaultPropertyValues(OMFContext omfContext) {

    PersistenceConfiguration conf = omfContext.getPersistenceConfiguration();
    // There is only one datastore so set this to true no matter what.
    conf.setProperty("datanucleus.attachSameDatastore", Boolean.TRUE.toString());
    // Only set this if a value has not been provided
    if (conf.getProperty(DatastoreConnectionFactoryImpl.AUTO_CREATE_TXNS_PROPERTY) == null) {
      conf.setProperty(
          DatastoreConnectionFactoryImpl.AUTO_CREATE_TXNS_PROPERTY, Boolean.TRUE.toString());
    }
    return omfContext;
  }

  @Override
  public NucleusConnection getNucleusConnection(ObjectManager om) {
    ConnectionFactory cf = getOMFContext().getConnectionFactoryRegistry()
        .lookupConnectionFactory(txConnectionFactoryName);

    final ManagedConnection mc;
    final boolean enlisted;
    enlisted = om.getTransaction().isActive();
    mc = cf.getConnection(enlisted ? om : null, null); // Will throw exception if already locked

    // Lock the connection now that it is in use by the user
    mc.lock();

    Runnable closeRunnable = new Runnable() {
      public void run() {
        // Unlock the connection now that the user has finished with it
        mc.unlock();
        if (!enlisted) {
          // TODO Anything to do here?
        }
      }
    };
    return new NucleusConnectionImpl(mc.getConnection(), closeRunnable);
  }

  @Override
  public Date getDatastoreDate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Extent getExtent(ObjectManager om, Class c, boolean subclasses) {
    AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(c, om.getClassLoaderResolver());
    if (!cmd.isRequiresExtent()) {
        throw new NoExtentException(c.getName());
    }
    return new DatastoreExtent(om, c, subclasses, cmd);
  }
/**
   * Method to create the IdentifierFactory to be used by this store.
   * Relies on the datastore adapter existing before creation
   * @param omfContext ObjectManagerFactory context
   */
  protected void initialiseIdentifierFactory(OMFContext omfContext) {
    PersistenceConfiguration conf = omfContext.getPersistenceConfiguration();
    String idFactoryName = conf.getStringProperty("datanucleus.identifierFactory");
    String idFactoryClassName = omfContext.getPluginManager()
        .getAttributeValueForExtension("org.datanucleus.store_identifierfactory",
            "name", idFactoryName, "class-name");
    if (idFactoryClassName == null) {
      throw new NucleusUserException(idFactoryName).setFatal();
    }
    Map<String, String> props = new HashMap<String, String>();
    addStringPropIfNotNull(conf, props, "datanucleus.mapping.Catalog", "DefaultCatalog");
    addStringPropIfNotNull(conf, props, "datanucleus.mapping.Schema", "DefaultSchema");
    addStringPropIfNotNull(conf, props, "datanucleus.identifier.case", "RequiredCase");
    addStringPropIfNotNull(conf, props, "datanucleus.identifier.wordSeparator", "WordSeparator");
    addStringPropIfNotNull(conf, props, "datanucleus.identifier.tablePrefix", "TablePrefix");
    addStringPropIfNotNull(conf, props, "datanucleus.identifier.tableSuffix", "TableSuffix");
    try {
      // Create the IdentifierFactory
      Class cls = Class.forName(idFactoryClassName);
      Class[] argTypes = new Class[]
          {org.datanucleus.store.mapped.DatastoreAdapter.class, ClassLoaderResolver.class, Map.class};
      Object[] args = {dba, omfContext.getClassLoaderResolver(null), props};
      identifierFactory = (IdentifierFactory) ClassUtils.newInstance(cls, argTypes, args);
    }
    catch (ClassNotFoundException cnfe) {
      throw new NucleusUserException(
          idFactoryName + ":" + idFactoryClassName, cnfe).setFatal();
    }
    catch (Exception e) {
      NucleusLogger.PERSISTENCE.error(e);
      throw new NucleusException(idFactoryClassName, e).setFatal();
    }
  }

  private void addStringPropIfNotNull(
      PersistenceConfiguration conf, Map<String, String> map, String propName, String mapName) {
    String val = conf.getStringProperty(propName);
    if (val != null) {
      map.put(mapName, val);
    }
  }
  @Override
  public Collection<String> getSupportedOptions() {
    Set<String> opts = new HashSet<String>();
    opts.add("TransactionIsolationLevel.read-committed");
    opts.add("BackedSCO");
    return opts;
  }

  @Override
  public FetchStatement getFetchStatement(DatastoreContainerObject table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatastoreContainerObject newJoinDatastoreContainerObject(AbstractMemberMetaData fmd,
      ClassLoaderResolver clr) {
    return null;
  }

  @Override
  protected StoreData newStoreData(ClassMetaData cmd, ClassLoaderResolver clr) {
    DatastoreTable table = new DatastoreTable(this, cmd, clr, dba);
    StoreData sd = new MappedStoreData(cmd, table, true);
    registerStoreData(sd);
    // needs to be called after we register the store data to avoid stack
    // overflow
    table.buildMapping();
    return sd;
  }

  @Override
  public FieldManager getFieldManagerForResultProcessing(StateManager sm, Object obj,
      StatementMappingForClass resultMappings) {
    return new KeyOnlyFieldManager((Key) obj);
  }

  @Override
  public Object getResultValueAtPosition(Object key, JavaTypeMapping mapping, int position) {
    // this is the key, and we're only using this for keys, so just return it.
    return key;
  }

  @Override
  public FieldManager getFieldManagerForStatementGeneration(StateManager sm, Object stmt,
      StatementMappingIndex[] stmtMappings, boolean checkNonNullable) {
    // TODO(maxr)
    return null;
  }

  @Override
  public boolean insertValuesOnInsert(DatastoreMapping datastoreMapping) {
    return true;
  }

  @Override
  public boolean allowsBatching() {
    return false;
  }

  @Override
  public ResultObjectFactory newResultObjectFactory(DatastoreClass table,
      AbstractClassMetaData acmd, StatementMappingForClass mappingDefinition, boolean ignoreCache,
      boolean discriminator, boolean hasMetaDataInResults, FetchPlan fetchPlan,
      Class persistentClass) {
    return new DatastoreResultObjectFactory();
  }

  // TODO(maxr) This method is very similar to a method of the same name in
  // RDBMSManager.  Push it up into MappedStoreManager so we don't have so
  // much duplication.
  private void assertCompatibleFieldType(AbstractMemberMetaData fmd, ClassLoaderResolver clr, Class type,
          Class expectedMappingType) {
    DatastoreClass ownerTable = getDatastoreClass(fmd.getClassName(), clr);
    if (ownerTable == null) {
      // Class doesn't manage its own table (uses subclass-table, or superclass-table?)
      AbstractClassMetaData fieldTypeCmd =
          getMetaDataManager().getMetaDataForClass(fmd.getClassName(), clr);
      AbstractClassMetaData[] tableOwnerCmds = getClassesManagingTableForClass(fieldTypeCmd, clr);
      if (tableOwnerCmds != null && tableOwnerCmds.length == 1) {
        ownerTable = getDatastoreClass(tableOwnerCmds[0].getFullClassName(), clr);
      }
    }

    if (ownerTable != null) {
      JavaTypeMapping m = ownerTable.getMemberMapping(fmd);
      if (!expectedMappingType.isAssignableFrom(m.getClass())) {
        throw new IncompatibleFieldTypeException(fmd.getFullFieldName(),
                                                 type.getName(), fmd.getTypeName());
      }
    }
  }

  // TODO(maxr) This method is very similar to a method of the same name in
  // RDBMSManager.  Push it up into MappedStoreManager so we don't have so
  // much duplication.
  public Store getBackingStoreForField(ClassLoaderResolver clr, AbstractMemberMetaData fmd,
                                       Class type) {
    if (fmd != null && fmd.isSerialized()) {
      return null;
    }

    if (fmd.getMap() != null) {
      assertCompatibleFieldType(fmd, clr, type, MapMapping.class);
      return getBackingStoreForMap(fmd, clr);
    }
    if (fmd.getArray() != null) {
      assertCompatibleFieldType(fmd, clr, type, ArrayMapping.class);
      return getBackingStoreForArray(fmd, clr);
    }
    assertCompatibleFieldType(fmd, clr, type, CollectionMapping.class);

    return getBackingStoreForCollection(fmd, clr, type);
  }

  // TODO(maxr) This method is very similar to a method of the same name in
  // RDBMSManager.  Push it up into MappedStoreManager so we don't have so
  // much duplication.
  /**
   * Method to return a backing store for a Collection, consistent with this store and the
   * instantiated type.
   *
   * @param fmd MetaData for the field that has this collection
   * @param clr ClassLoader resolver
   * @return The backing store of this collection in this store
   */
  private CollectionStore getBackingStoreForCollection(AbstractMemberMetaData fmd,
                                                       ClassLoaderResolver clr, Class type) {
    CollectionStore store = null;
    DatastoreContainerObject datastoreTable = getDatastoreContainerObject(fmd);
    if (type == null) {
      // No type to base it on so create it based on the field declared type
      if (datastoreTable == null) {
        // We need a "FK" relation.
        if (List.class.isAssignableFrom(fmd.getType())) {
          store =
              new FKListStore(fmd, this, clr,
                              new DatastoreFKListStoreSpecialization(LOCALISER, clr, this));
        } else {
          // store = new FKSetStore(fmd, this, clr);
        }
      } else {
//              CollectionTable collTable = (CollectionTable) datastoreTable;
//              // We need a "JoinTable" relation.
//              if (List.class.isAssignableFrom(fmd.getType()))
//              {
//                  store = new JoinListStore(fmd, clr, collTable, collTable.getOwnerMapping(),
//                      collTable.getElementMapping(), collTable.getOrderMapping(),
//                      collTable.getRelationDiscriminatorMapping(), collTable.getRelationDiscriminatorValue(),
//                      collTable.isEmbeddedElement(), collTable.isSerialisedElement(),
//                      this, new RDBMSJoinListStoreSpecialization(LOCALISER, clr, this));
//              }
//              else
//              {
//                  store = new JoinSetStore(fmd, collTable, clr);
//              }
      }
    } else {
      // Instantiated type specified so use it to pick the associated backing store
      if (datastoreTable == null) {
        if (SCOUtils.isListBased(type)) {
          // List required
          store =
              new FKListStore(fmd, this, clr,
                              new DatastoreFKListStoreSpecialization(LOCALISER, clr, this));
        } else {
          // Set required
          // store = new FKSetStore(fmd, this, clr);
        }
      } else {
//              CollectionTable collTable = (CollectionTable) datastoreTable;
//              if (SCOUtils.isListBased(type))
//              {
//                  // List required
//                  store = new JoinListStore(fmd, clr, collTable, collTable.getOwnerMapping(),
//                      collTable.getElementMapping(), collTable.getOrderMapping(),
//                      collTable.getRelationDiscriminatorMapping(), collTable.getRelationDiscriminatorValue(),
//                      collTable.isEmbeddedElement(), collTable.isSerialisedElement(),
//                      this, new RDBMSJoinListStoreSpecialization(LOCALISER, clr, this));
//              }
//              else
//              {
//                  // Set required
//                  store = new JoinSetStore(fmd, collTable, clr);
//              }
      }
    }
    return store;
  }

  // TODO(maxr) This method is very similar to a method of the same name in
  // RDBMSManager.  Push it up into MappedStoreManager so we don't have so
  // much duplication.
  private MapStore getBackingStoreForMap(AbstractMemberMetaData fmd, ClassLoaderResolver clr) {
    return null;
//      MapStore store = null;
//      DatastoreContainerObject datastoreTable = getDatastoreContainerObject(fmd);
//      if (datastoreTable == null)
//      {
//          store = new FKMapStore(fmd, this, clr);
//      }
//      else
//      {
//          store = new JoinMapStore((MapTable)datastoreTable, clr);
//      }
//      return store;
  }

  // TODO(maxr) This method is very similar to a method of the same name in
  // RDBMSManager.  Push it up into MappedStoreManager so we don't have so
  // much duplication.
  private ArrayStore getBackingStoreForArray(AbstractMemberMetaData fmd, ClassLoaderResolver clr) {
    return null;
//      ArrayStore store = null;
//      DatastoreContainerObject datastoreTable = getDatastoreContainerObject(fmd);
//      if (datastoreTable != null)
//      {
//          ArrayTable arrayTable = (ArrayTable) datastoreTable;
//          store = new JoinArrayStore(arrayTable, arrayTable.getOwnerFieldMetaData(), arrayTable.getOwnerMapping(),
//              arrayTable.getElementMapping(), arrayTable.getOrderMapping(), arrayTable.getRelationDiscriminatorMapping(),
//              arrayTable.getRelationDiscriminatorValue(), arrayTable.getElementType(), arrayTable.isEmbeddedElement(),
//              arrayTable.isSerialisedElement(), clr, new RDBMSJoinArrayStoreSpecialization(LOCALISER, clr, this));
//      }
//      else
//      {
//          store = new FKArrayStore(fmd, this, clr, new RDBMSFKArrayStoreSpecialization(LOCALISER, clr, this));
//      }
//      return store;
  }

  /**
   * A {@link FieldManager} implementation that can only be used for managing
   * keys.  Everything else throws {@link UnsupportedOperationException}.
   */
  private static class KeyOnlyFieldManager implements FieldManager {
    private final Key key;

    private KeyOnlyFieldManager(Key key) {
      this.key = key;
    }

    public void storeBooleanField(int fieldNumber, boolean value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeByteField(int fieldNumber, byte value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeCharField(int fieldNumber, char value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeDoubleField(int fieldNumber, double value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeFloatField(int fieldNumber, float value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeIntField(int fieldNumber, int value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeLongField(int fieldNumber, long value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeShortField(int fieldNumber, short value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeStringField(int fieldNumber, String value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public void storeObjectField(int fieldNumber, Object value) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public boolean fetchBooleanField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public byte fetchByteField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public char fetchCharField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public double fetchDoubleField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public float fetchFloatField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public int fetchIntField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public long fetchLongField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public short fetchShortField(int fieldNumber) {
      throw new UnsupportedOperationException("Should only be using this for keys.");
    }

    public String fetchStringField(int fieldNumber) {
      return KeyFactory.encodeKey(key);
    }

    public Object fetchObjectField(int fieldNumber) {
      return key;
    }
  }

  @Override
  public DatastorePersistenceHandler getPersistenceHandler() {
    return (DatastorePersistenceHandler) super.getPersistenceHandler();
  }

  // For testing
  String getTxConnectionFactoryName() {
    return txConnectionFactoryName;
  }

  // For testing
  String getNonTxConnectionFactoryName() {
    return nontxConnectionFactoryName;
  }

  /**
   * Helper method to determine if the connection factory associated with this
   * manager is transactional.
   */
  boolean connectionFactoryIsTransactional() {
    ConnectionFactoryRegistry registry = getOMFContext().getConnectionFactoryRegistry();
    DatastoreConnectionFactoryImpl connFactory =
        (DatastoreConnectionFactoryImpl) registry.lookupConnectionFactory(txConnectionFactoryName);
    return connFactory.isTransactional();
  }
}
