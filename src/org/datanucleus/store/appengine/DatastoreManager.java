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
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.store.Extent;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.NucleusConnectionImpl;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.exceptions.NoExtentException;
import org.datanucleus.store.exceptions.NoTableManagedException;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.FetchStatement;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreData;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.StatementMappingForClass;
import org.datanucleus.store.mapped.mapping.DatastoreMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.scostore.FKArrayStoreSpecialization;
import org.datanucleus.store.mapped.scostore.FKListStoreSpecialization;
import org.datanucleus.store.mapped.scostore.FKMapStore;
import org.datanucleus.store.mapped.scostore.FKSetStore;
import org.datanucleus.store.mapped.scostore.JoinArrayStore;
import org.datanucleus.store.mapped.scostore.JoinListStore;
import org.datanucleus.store.mapped.scostore.JoinMapStore;
import org.datanucleus.store.mapped.scostore.JoinSetStore;
import org.datanucleus.store.query.ResultObjectFactory;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreManager extends MappedStoreManager {

  /**
   * Classes whose metadata we've validated.  This set gets hit on every
   * insert, update, and fetch.  I don't expect it to be a bottleneck but
   * if we're seeing contention we should look here.
   */
  private final Set<String> validatedClasses = Collections.synchronizedSet(new HashSet<String>());

  private static final String EXTENSION_PREFIX = "gae.";

  /**
   * The name of the annotation extension that marks a field as an parent.
   */
  public static final String PARENT_PK = EXTENSION_PREFIX + "parent-pk";

  /**
   * The name of the annotation extension that marks a field as an encoded pk
   */
  public static final String ENCODED_PK = EXTENSION_PREFIX + "encoded-pk";

  /**
   * The name of the annotation extension that marks a field as a primary key name
   */
  public static final String PK_NAME = EXTENSION_PREFIX + "pk-name";

  /**
   * The name of the annotation extension that marks a field as a primary key id
   */
  public static final String PK_ID = EXTENSION_PREFIX + "pk-id";

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
      PersistenceConfiguration conf, Map<String, String> map, String propName, String mapKeyName) {
    String val = conf.getStringProperty(propName);
    if (val != null) {
      map.put(mapKeyName, val);
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
  public boolean insertValuesOnInsert(DatastoreMapping datastoreMapping) {
    return true;
  }

  @Override
  public boolean allowsBatching() {
    return false;
  }

  @Override
  public FieldManager getFieldManagerForStatementGeneration(StateManager sm, Object stmt,
      StatementMappingForClass stmtMappings, boolean checkNonNullable) {
    // TODO(maxr)
    return null;
  }

  @Override
  public ResultObjectFactory newResultObjectFactory(DatastoreClass table,
      AbstractClassMetaData acmd, StatementMappingForClass mappingDefinition, boolean ignoreCache,
      boolean discriminator, FetchPlan fetchPlan, Class persistentClass) {
    // TODO(maxr)
    return null;
  }

  @Override
  protected FKListStoreSpecialization newFKListStoreSpecialization(ClassLoaderResolver clr) {
    return new DatastoreFKListStoreSpecialization(LOCALISER, clr, this);
  }

  @Override
  protected FKSetStore newFKSetStore(AbstractMemberMetaData ammd, ClassLoaderResolver clr) {
    return new DatastoreFKSetStore(ammd, this, clr);
  }

  @Override
  protected FKArrayStoreSpecialization newFKArrayStoreSpecialization(ClassLoaderResolver clr) {
    // Disabled for v1.0
    throw new UnsupportedOperationException("FK Arrays not supported.");
//    return new DatastoreFKArrayStoreSpecialization(LOCALISER, clr, this);
  }

  @Override
  protected FKMapStore newFKMapStore(AbstractMemberMetaData clr, ClassLoaderResolver amd) {
    // TODO(maxr)
    throw new UnsupportedOperationException("FK Maps not supported.");
  }

  @Override
  protected JoinArrayStore newJoinArrayStore(DatastoreContainerObject arrayTable,
      ClassLoaderResolver clr) {
    // TODO(maxr)
    throw new UnsupportedOperationException("Join Arrays not supported.");
  }

  @Override
  protected JoinMapStore newJoinMapStore(DatastoreContainerObject mapTable,
      ClassLoaderResolver clr) {
    // TODO(maxr)
    throw new UnsupportedOperationException("Join Maps not supported.");
  }

  @Override
  protected JoinListStore newJoinListStore(AbstractMemberMetaData amd, ClassLoaderResolver clr,
      DatastoreContainerObject table) {
    // TODO(maxr)
    throw new UnsupportedOperationException("Join Lists not supported.");
  }

  @Override
  protected JoinSetStore newJoinSetStore(AbstractMemberMetaData amd, ClassLoaderResolver clr,
      DatastoreContainerObject table) {
    // TODO(maxr)
    throw new UnsupportedOperationException("Join Sets not supported.");
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
      return KeyFactory.keyToString(key);
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

  @Override
  public DatastoreTable getDatastoreClass(String className, ClassLoaderResolver clr) {
    try {
      return (DatastoreTable) super.getDatastoreClass(className, clr);
    } catch (NoTableManagedException e) {
      // Our parent class throws this when the class isn't PersistenceCapable.
      // The error message is mis-leading so we'll swallow the exception and
      // throw something clearer.  We still need to throw
      // NoTableManagedException because in some scenarios this isn't really
      // an error and DataNucleus catches it.  If we throw something else we'll
      // get exceptions about java.lang.String not being PersistenceCapable.
      // Really.  I saw it with my own eyes.
      Class cls = clr.classForName(className);
      ApiAdapter api = getApiAdapter();
      if (cls != null && !cls.isInterface() && !api.isPersistable(cls)) {
        throw new NoTableManagedException(
            "Class " + className + " does not seem to have been enhanced.  You may want to rerun "
            + "the enhancer and check for errors in the output.");
      }
      // Some othe problem.  Parent class's inaccurate error message is no
      // worse than our best guess.
      throw e;
    }
  }

  private static boolean memberHasExtension(AbstractClassMetaData acmd, int pos, String extensionName) {
    AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtPosition(pos);
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

  @Override
  public void close() {
    validatedClasses.clear();
    super.close();
  }
}
