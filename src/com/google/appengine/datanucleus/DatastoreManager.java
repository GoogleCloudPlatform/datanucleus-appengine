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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.ReadPolicy;
import com.google.appengine.api.datastore.ReadPolicy.Consistency;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.plugin.PluginManager;
import org.datanucleus.plugin.PluginRegistry;
import org.datanucleus.store.connection.ConnectionFactory;
import org.datanucleus.FetchPlan;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.NucleusContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.store.DefaultCandidateExtent;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.Extent;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.NucleusConnectionImpl;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.exceptions.NoExtentException;
import org.datanucleus.store.exceptions.NoTableManagedException;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.MappedStoreData;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.StatementClassMapping;
import org.datanucleus.store.mapped.mapping.DatastoreMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.scostore.FKListStore;
import org.datanucleus.store.mapped.scostore.FKSetStore;
import org.datanucleus.store.query.ResultObjectFactory;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreManager extends MappedStoreManager {

    protected static final Localiser GAE_LOCALISER = Localiser.getInstance(
        "com.google.appengine.datanucleus.Localisation", DatastoreManager.class.getClassLoader());

  /**
   * Classes whose metadata we've validated.  This set gets hit on every
   * insert, update, and fetch.  I don't expect it to be a bottleneck but
   * if we're seeing contention we should look here.
   */
  private final Set<String> validatedClasses = Collections.synchronizedSet(new HashSet<String>());

  private static final String EXTENSION_PREFIX = "gae.";

  /** The name of the annotation extension that marks a field as an parent. */
  public static final String PARENT_PK = EXTENSION_PREFIX + "parent-pk";

  /** The name of the annotation extension that marks a field as an encoded pk. */
  public static final String ENCODED_PK = EXTENSION_PREFIX + "encoded-pk";

  /** The name of the annotation extension that marks a field as a primary key name. */
  public static final String PK_NAME = EXTENSION_PREFIX + "pk-name";

  /** The name of the annotation extension that marks a field as a primary key id. */
  public static final String PK_ID = EXTENSION_PREFIX + "pk-id";

  /** The name of the annotation extension that marks a field as unindexed. */
  public static final String UNINDEXED_PROPERTY = EXTENSION_PREFIX + "unindexed";

  /** The name of the extension that indicates the query should be excluded from the current transaction. */
  public static final String EXCLUDE_QUERY_FROM_TXN = EXTENSION_PREFIX + "exclude-query-from-txn";

  /**
   * If the user sets javax.jdo.option.DatastoreReadTimeoutMillis or javax.persistence.query,timeout it will be
   * available via a config property with this name.
   */
  public static final String READ_TIMEOUT_PROPERTY = "datanucleus.datastoreReadTimeout";

  /**
   * If the user sets javax.jdo.option.DatastoreWriteTimeoutMillis it will be
   * available via a config property with this name.
   */
  public static final String WRITE_TIMEOUT_PROPERTY = "datanucleus.datastoreWriteTimeout";

  public static final String DATASTORE_READ_CONSISTENCY_PROPERTY =
      "datanucleus.appengine.datastoreReadConsistency";

  /**
   * The name of the extension that indicates the return value of the batch
   * delete query should be as accurate as possible at the expense of
   * fulfilling the query less efficiently. If this extension is set and
   * {@code true}, the query will be fulfilled by first fetching the entities
   * and then deleting those that are returned.  This involves an additional
   * roundtrip to the datastore but allows us to return a more accurate count
   * of the number of records that were deleted (we say more accurate as
   * opposed to accurate because it's possible that entity was deleted in
   * between the fetch and the delete and we wouldn't have any way of knowing).
   * If this extension is not set or is set with a value of {@code false},
   * we'll just execute a batch delete directly and use the number of entities
   * we were asked to delete as the return value.
   */
  public static final String SLOW_BUT_MORE_ACCURATE_JPQL_DELETE_QUERY =
      EXTENSION_PREFIX + "slow-but-more-accurate-jpql-delete-query";

  public static final String GET_EXTENT_CAN_RETURN_SUBCLASSES_PROPERTY =
      "datanucleus.appengine.getExtentCanReturnSubclasses";

  private final BatchPutManager batchPutManager = new BatchPutManager();
  private final BatchDeleteManager batchDeleteManager = new BatchDeleteManager();
  private final StorageVersion storageVersion;
  private final DatastoreServiceConfig defaultDatastoreServiceConfigPrototypeForReads;
  private final DatastoreServiceConfig defaultDatastoreServiceConfigPrototypeForWrites;

  protected SerializationManager serializationMgr = null;

  /**
   * Construct a DatastoreManager.
   * @param clr The ClassLoaderResolver
   * @param nucContext The NucleusContext
   * @param props Properties to store on this StoreManager
   */
  public DatastoreManager(ClassLoaderResolver clr, NucleusContext nucContext, Map<String, Object> props)
      throws NoSuchFieldException, IllegalAccessException {
    super("appengine", clr, nucContext, props);

    // Override some of the default property values for AppEngine
    PersistenceConfiguration conf = nucContext.getPersistenceConfiguration();
    conf.setProperty("datanucleus.attachSameDatastore", Boolean.TRUE.toString()); // Always only one datastore
    // We'd like to respect the user's selection here, but the default value is 1.
    // This is problematic for us in the situation where, for example, an embedded object
    // gets updated more than once in a txn because we end up putting the same entity twice.
    // TODO(maxr) Remove this once we support multiple puts
    conf.setProperty("datanucleus.datastoreTransactionFlushLimit", Integer.MAX_VALUE);
    // Install our key translator
    conf.setProperty("datanucleus.identityKeyTranslatorType", "appengine");

    // Check if datastore api is in CLASSPATH.  Don't let the hard-coded
    // jar name upset you, it's just used for error messages.  The check will
    // succeed so long as the class is available on the classpath
    ClassUtils.assertClassForJarExistsInClasspath(
        clr, "com.google.appengine.api.datastore.DatastoreService", "appengine-api.jar");

    defaultDatastoreServiceConfigPrototypeForReads =
        createDatastoreServiceConfigPrototypeForReads(nucContext.getPersistenceConfiguration());
    defaultDatastoreServiceConfigPrototypeForWrites =
        createDatastoreServiceConfigPrototypeForWrites(nucContext.getPersistenceConfiguration());

    // Handler for persistence process
    persistenceHandler = new DatastorePersistenceHandler(this);
    dba = new DatastoreAdapter();
    initialiseIdentifierFactory(nucContext);
    if (nucContext.getApiAdapter().getName().equalsIgnoreCase("JDO")) {
      // TODO Drop this when remove DatastoreJDOMetaDataManager
      setCustomPluginManager();
    }
    addTypeManagerMappings();

    storageVersion = StorageVersion.fromStoreManager(this);
    initialiseAutoStart(clr);

    logConfiguration();
  }

  @Override
  public void close() {
    validatedClasses.clear();
    super.close();
  }

  public SerializationManager getSerializationManager() {
    if (serializationMgr == null) {
      serializationMgr = new SerializationManager();
    }
    return serializationMgr;
  }

  /**
   * Convenience method to log the configuration of this store manager.
   */
  protected void logConfiguration()
  {
    super.logConfiguration();

    if (NucleusLogger.DATASTORE.isDebugEnabled())
    {
        NucleusLogger.DATASTORE.debug("StorageVersion : " + storageVersion.toString());
        NucleusLogger.DATASTORE.debug("===========================================================");
    }
  }

  @Override
  public void transactionStarted(ExecutionContext ec) {
    NucleusLogger.GENERAL.info(">> DatastoreMgr.txnStarted");
    // Obtain a connection. This will create it now that the user has selected tx.begin()
//    getConnection(ec);
    super.transactionStarted(ec);
  }

  @Override
  public void transactionCommitted(ExecutionContext ec) {
      NucleusLogger.GENERAL.info(">> DatastoreMgr.txnCommitted");
    // TODO Make use of this to notify the ConnectionFactoryImpl that it should start a DatastoreTransaction
    super.transactionCommitted(ec);
  }

  @Override
  public void transactionRolledBack(ExecutionContext ec) {
      NucleusLogger.GENERAL.info(">> DatastoreMgr.txnRolledBack");
    // TODO Make use of this to notify the ConnectionFactoryImpl that it should start a DatastoreTransaction
    super.transactionRolledBack(ec);
  }

  private DatastoreServiceConfig createDatastoreServiceConfigPrototypeForReads(
      PersistenceConfiguration persistenceConfig) {
    return createDatastoreServiceConfigPrototype(
        persistenceConfig, READ_TIMEOUT_PROPERTY);
  }

  private DatastoreServiceConfig createDatastoreServiceConfigPrototypeForWrites(
      PersistenceConfiguration persistenceConfig) {
    return createDatastoreServiceConfigPrototype(persistenceConfig, WRITE_TIMEOUT_PROPERTY);
  }

  private DatastoreServiceConfig createDatastoreServiceConfigPrototype(
      PersistenceConfiguration persistenceConfiguration, String... timeoutProps) {
    DatastoreServiceConfig datastoreServiceConfig = DatastoreServiceConfig.Builder.withDefaults();

    for (String timeoutProp : timeoutProps) {
      int defaultDeadline = persistenceConfiguration.getIntProperty(timeoutProp);
      if (defaultDeadline > 0) {
        datastoreServiceConfig.deadline(defaultDeadline / 1000d);
      }
    }
    String defaultReadConsistencyStr = persistenceConfiguration.getStringProperty(
        DATASTORE_READ_CONSISTENCY_PROPERTY);
    if (defaultReadConsistencyStr != null) {
      try {
        datastoreServiceConfig.readPolicy(new ReadPolicy(Consistency.valueOf(defaultReadConsistencyStr)));
      } catch (IllegalArgumentException iae) {
        throw new NucleusFatalUserException(
            "Illegal value for " + DATASTORE_READ_CONSISTENCY_PROPERTY +
            ".  Valid values are " + Arrays.toString(Consistency.values()));
      }
    }
    return datastoreServiceConfig;
  }

  private void setCustomPluginManager() throws NoSuchFieldException, IllegalAccessException {
    // Replaces the configured plugin registry with our own implementation.
    // Reflection is required because there's no public mutator for this field.
    PluginManager pluginMgr = getNucleusContext().getPluginManager();
    Field registryField = PluginManager.class.getDeclaredField("registry");
    registryField.setAccessible(true);
    registryField.set(
        pluginMgr, new DatastorePluginRegistry((PluginRegistry) registryField.get(pluginMgr)));
  }

  private void addTypeManagerMappings() throws NoSuchFieldException, IllegalAccessException {
    Field javaTypes = TypeManager.class.getDeclaredField("javaTypes");
    javaTypes.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) javaTypes.get(nucleusContext.getTypeManager());
    Object arrayListValue = map.get(ArrayList.class.getName());
    // Arrays$ArrayList is an inner class that is used for the results of
    // Arrays.asList().  We want the same sco behavior for instances of
    // this class so they end up with the same sco implementation
    // that gets used for ArrayList.
    map.put("java.util.Arrays$ArrayList", arrayListValue);
  }

  // TODO This is wrong. It returns NucleusConnectionImpl where mc.getConnection is null hence does NOT give access
  // to the 
  @Override
  public NucleusConnection getNucleusConnection(ExecutionContext ec) {
    ConnectionFactory cf = connectionMgr.lookupConnectionFactory(txConnectionFactoryName);

    final ManagedConnection mc;
    final boolean enlisted;
    enlisted = ec.getTransaction().isActive();
    mc = cf.getConnection(enlisted ? ec : null, ec.getTransaction(), null); // Will throw exception if already locked

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

  // TODO Remove this when we support subclasses from a query
  @Override
  public Extent getExtent(ExecutionContext ec, Class c, boolean subclasses) {
    AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(c, ec.getClassLoaderResolver());
    validateMetaDataForClass(cmd, ec.getClassLoaderResolver());
    if (!cmd.isRequiresExtent()) {
      throw new NoExtentException(c.getName());
    }
    if (!getBooleanProperty(GET_EXTENT_CAN_RETURN_SUBCLASSES_PROPERTY, false)) {
      subclasses = false;
    }
    // In order to avoid breaking existing apps I'm hard-coding subclasses to
    // be false.  This breaks spec compliance since the no-arg overload of
    // PersistenceManager.getExtent() is supposed to return subclasses.
    return new DefaultCandidateExtent(ec, c, subclasses, cmd);
  }

  /**
   * Method to return the default identifier case.
   * @return Identifier case to use if not specified by the user
   */
  public String getDefaultIdentifierCase() {
    return "PreserveCase";
  }

  @Override
  public Collection<String> getSupportedOptions() {
    Set<String> opts = new HashSet<String>();
    opts.add("TransactionIsolationLevel.read-committed");
    opts.add("BackedSCO");
    opts.add("ApplicationIdentity");
//    opts.add("DatastoreIdentity"); // TODO Support DatastoreIdentity
    opts.add("OptimisticTransaction");
    opts.add("ORM");
    return opts;
  }

  @Override
  public DatastoreContainerObject newJoinDatastoreContainerObject(AbstractMemberMetaData fmd,
      ClassLoaderResolver clr) {
    return null;
  }

  @Override
  protected StoreData newStoreData(ClassMetaData cmd, ClassLoaderResolver clr) {
    InheritanceStrategy strat = cmd.getInheritanceMetaData().getStrategy();

    // The overarching rule for supported inheritance strategies is that we
    // don't split the state of an object across multiple entities.
    if (strat == InheritanceStrategy.SUBCLASS_TABLE) {
      // Table mapped into the table(s) of subclass(es)
      // Just add the SchemaData entry with no table - managed by subclass
      return buildStoreDataWithNoTable(cmd);
    } else if (strat == InheritanceStrategy.COMPLETE_TABLE) {
      if (cmd.isAbstract()) {
        // Abstract class with "complete-table" so gets no table
        return buildStoreDataWithNoTable(cmd);
      }
      return buildStoreData(cmd, clr);
    } else if (strat == InheritanceStrategy.NEW_TABLE &&
               (cmd.getSuperAbstractClassMetaData() == null ||
                cmd.getSuperAbstractClassMetaData().getInheritanceMetaData().getStrategy() == InheritanceStrategy.SUBCLASS_TABLE)) {
      // New Table means you store your fields and your fields only (no fields
      // from superclasses).  Thiis only ok if you don't have a persistent
      // superclass or your persistent superclass has delegated responsibility
      // for storing its fields to you.
      return buildStoreData(cmd, clr);
    } else if (isNewOrSuperclassTableInheritanceStrategy(cmd)) {
      // Table mapped into table of superclass
      // Find the superclass - should have been created first
      AbstractClassMetaData[] managingCmds = getClassesManagingTableForClass(cmd, clr);
      DatastoreTable superTable;
      if (managingCmds != null && managingCmds.length == 1) {
        MappedStoreData superData = (MappedStoreData) storeDataMgr.get(managingCmds[0].getFullClassName());
        if (superData != null) {
          // Specify the table if it already exists
          superTable = (DatastoreTable) superData.getDatastoreContainerObject();
          return buildStoreDataWithTable(cmd, superTable);
        }
      }
    }

    boolean jpa = getApiAdapter().getName().equalsIgnoreCase("JPA");
    String unsupportedMsg = GAE_LOCALISER.msg(jpa ? "AppEngine.BadInheritance.JPA" : "AppEngine.BadInheritance.JDO", 
        cmd.getInheritanceMetaData().getStrategy(), cmd.getFullClassName(), getApiAdapter().getName());
    throw new UnsupportedInheritanceStrategyException(unsupportedMsg);
  }

  private StoreData buildStoreDataWithNoTable(ClassMetaData cmd) {
    StoreData sdNew = new MappedStoreData(cmd, null, false);
    registerStoreData(sdNew);
    return sdNew;
  }

  private StoreData buildStoreData(ClassMetaData cmd, ClassLoaderResolver clr) {
    String tableName;
    if (cmd.getTable() != null) {
      // User specified a table name as part of the mapping so use that as the kind.
      tableName = cmd.getTable();
    } else {
      tableName = getIdentifierFactory().newDatastoreContainerIdentifier(cmd).getIdentifierName();
    }
    DatastoreTable table = new DatastoreTable(tableName, this, cmd, clr, dba);
    StoreData sd = new MappedStoreData(cmd, table, true);
    registerStoreData(sd);
    // needs to be called after we register the store data to avoid stack overflow
    table.buildMapping();
    return sd;
  }
  
  private StoreData buildStoreDataWithTable(ClassMetaData cmd, DatastoreTable table) {
    MappedStoreData sd = new MappedStoreData(cmd, table, false);
    registerStoreData(sd);
    sd.setDatastoreContainerObject(table);
    table.manageClass(cmd);
    return sd;
  }

  @Override
  public Object getResultValueAtPosition(Object key, JavaTypeMapping mapping, int position) {
    // this is the key, and we're only using this for keys, so just return it.
    return key;
  }

  @Override
  public boolean insertValuesOnInsert(DatastoreMapping datastoreMapping) {
    return true;
  }

  @Override
  public boolean allowsBatching() {
    return false;
  }

  public FieldManager getFieldManagerForResultProcessing(ObjectProvider op, Object resultSet,
                                                         StatementClassMapping resultMappings) {
    ExecutionContext ec = op.getExecutionContext();
    Class<?> cls = ec.getClassLoaderResolver().classForName(op.getClassMetaData().getFullClassName());
    Object internalKey = EntityUtils.idToInternalKey(ec, cls, resultSet, true);
    // Need to provide this to the field manager in the form of the pk
    // of the type: Key, Long, encoded String, or unencoded String
    return new KeyOnlyFieldManager(internalKey);
  }

  @Override
  public FieldManager getFieldManagerForResultProcessing(ExecutionContext ec, Object resultSet, 
          StatementClassMapping resultMappings, AbstractClassMetaData cmd) {
    Class<?> cls = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
    Object internalKey = EntityUtils.idToInternalKey(ec, cls, resultSet, true);
    // Need to provide this to the field manager in the form of the pk
    // of the type: Key, Long, encoded String, or unencoded String
    return new KeyOnlyFieldManager(internalKey);
  }

  public FieldManager getFieldManagerForStatementGeneration(ObjectProvider op, Object stmt,
                                                            StatementClassMapping stmtMappings,
                                                            boolean checkNonNullable) {
    return null;
  }

  public ResultObjectFactory newResultObjectFactory(AbstractClassMetaData acmd,
                                                    StatementClassMapping mappingDefinition,
                                                    boolean ignoreCache, FetchPlan fetchPlan, 
                                                    Class persistentClass) {
    return null;
  }

  protected FKListStore newFKListStore(AbstractMemberMetaData ammd, ClassLoaderResolver clr) {
    return new DatastoreFKListStore(ammd, this, clr);
  }

  @Override
  protected FKSetStore newFKSetStore(AbstractMemberMetaData ammd, ClassLoaderResolver clr) {
    return new DatastoreFKSetStore(ammd, this, clr);
  }

  public BatchPutManager getBatchPutManager() {
    return batchPutManager;
  }

  public BatchDeleteManager getBatchDeleteManager() {
    return batchDeleteManager;
  }

  public boolean storageVersionAtLeast(StorageVersion storageVersion) {
    return getStorageVersion().ordinal() >= storageVersion.ordinal();
  }

  @Override
  public DatastorePersistenceHandler getPersistenceHandler() {
    return (DatastorePersistenceHandler) super.getPersistenceHandler();
  }

  @Override
  public DatastoreTable getDatastoreClass(String className, ClassLoaderResolver clr) {
    try {
      // We see the occasional race condition when multiple threads concurrently
      // perform an operation using a persistence-capable class for which DataNucleus
      // has not yet generated the meta-data.  The result is usually
      // AbstractMemberMetaData objects with the same column listed twice in the meta-data.
      // Locking at this level is a bit more coarse-grained than I'd like but once the
      // meta-data has been built this will return super fast so it shouldn't be an issue.
      synchronized(this) {
        return (DatastoreTable) super.getDatastoreClass(className, clr);
      }
    } catch (NoTableManagedException e) {
      // Our parent class throws this when the class isn't PersistenceCapable also.
      Class cls = clr.classForName(className);
      ApiAdapter api = getApiAdapter();
      if (cls != null && !cls.isInterface() && !api.isPersistable(cls)) {
        throw new NoTableManagedException(
            "Class " + className + " does not seem to have been enhanced. You may want to rerun " +
            "the enhancer and check for errors in the output.");
        // Suggest you address why this method is being called before any check on whether it is persistable
        // then you can remove this error message
      }
      throw e;
    }
  }

  private static boolean memberHasExtension(AbstractClassMetaData acmd, int pos, String extensionName) {
    AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(pos);
    return ammd.hasExtension(extensionName);
  }

  static boolean hasEncodedPKField(AbstractClassMetaData acmd) {
    int pkPos = acmd.getPKMemberPositions()[0];
    return isEncodedPKField(acmd, pkPos);
  }

  static boolean isEncodedPKField(AbstractClassMetaData acmd, int pos) {
    return memberHasExtension(acmd, pos, ENCODED_PK);
  }

  static boolean isParentPKField(AbstractClassMetaData acmd, int pos) {
    return memberHasExtension(acmd, pos, PARENT_PK);
  }

  static boolean isPKNameField(AbstractClassMetaData acmd, int pos) {
    return memberHasExtension(acmd, pos, PK_NAME);
  }

  static boolean isPKIdField(AbstractClassMetaData acmd, int pos) {
    return memberHasExtension(acmd, pos, PK_ID);
  }
  
  public static boolean isNewOrSuperclassTableInheritanceStrategy(AbstractClassMetaData cmd) {
    while (cmd != null) {
      AbstractClassMetaData pcmd = cmd.getSuperAbstractClassMetaData();
      if (pcmd == null) {
	return cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.NEW_TABLE;
      } else if (cmd.getInheritanceMetaData().getStrategy() != InheritanceStrategy.SUPERCLASS_TABLE) {
	return false;
      }
      cmd = pcmd;
    }
    
    return false;
  }

  /**
   * Perform appengine-specific validation on the provided meta data.
   * @param acmd The meta data to validate.
   * @param clr The classloader resolver to use.
   */
  public void validateMetaDataForClass(AbstractClassMetaData acmd, ClassLoaderResolver clr) {
    // Only validate each meta data once
    if (validatedClasses.add(acmd.getFullClassName())) {
      new MetaDataValidator(acmd, getMetaDataManager(), clr).validate();
    }
  }

  public StorageVersion getStorageVersion() {
    return storageVersion;
  }
  
  public static final class UnsupportedInheritanceStrategyException extends NucleusFatalUserException {
    UnsupportedInheritanceStrategyException(String msg) {
      super(msg);
    }
  }

  /**
   * Helper method to determine if the connection factory associated with this
   * manager is transactional.
   */
  public boolean connectionFactoryIsTransactional() {
    DatastoreConnectionFactoryImpl connFactory = 
        (DatastoreConnectionFactoryImpl) connectionMgr.lookupConnectionFactory(txConnectionFactoryName);
    return connFactory.isTransactional();
  }

  /**
   * Returns a fresh instance so that callers can make changes without
   * impacting global state.
   */
  public DatastoreServiceConfig getDefaultDatastoreServiceConfigForReads() {
    return copyDatastoreServiceConfig(defaultDatastoreServiceConfigPrototypeForReads);
  }

  /**
   * Returns a fresh instance so that callers can make changes without
   * impacting global state.
   */
  public DatastoreServiceConfig getDefaultDatastoreServiceConfigForWrites() {
    return copyDatastoreServiceConfig(defaultDatastoreServiceConfigPrototypeForWrites);
  }

  // For testing
  String getTxConnectionFactoryName() {
    return txConnectionFactoryName;
  }

  // For testing
  String getNonTxConnectionFactoryName() {
    return nontxConnectionFactoryName;
  }

  // visible for testing
  static DatastoreServiceConfig copyDatastoreServiceConfig(DatastoreServiceConfig config) {
    DatastoreServiceConfig newConfig = DatastoreServiceConfig.Builder.
        withImplicitTransactionManagementPolicy(config.getImplicitTransactionManagementPolicy()).
        readPolicy(config.getReadPolicy());

    if (config.getDeadline() != null) {
      newConfig.deadline(config.getDeadline());
    }
    return newConfig;
  }
}
