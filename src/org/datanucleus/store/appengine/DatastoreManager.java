// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ConnectionFactory;
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
import org.datanucleus.store.mapped.StatementExpressionIndex;
import org.datanucleus.store.mapped.mapping.DatastoreMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.query.ResultObjectFactory;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.util.Collection;
import java.util.Date;
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
    super("appengine", clr, omfContext);

    // Check if datastore api is in CLASSPATH.  Don't let the hard-coded
    // jar name upset you, it's just used for error messages.  The check will
    // succeed so long as the class is available on the classpath
    ClassUtils.assertClassForJarExistsInClasspath(
        clr, "com.google.apphosting.api.datastore.DatastoreService", "external-api.jar");

    PersistenceConfiguration conf = omfContext.getPersistenceConfiguration();
    conf.setProperty("datanucleus.attachSameDatastore", "true");
    // Handler for persistence process
    persistenceHandler = new DatastorePersistenceHandler(this);
    dba = new DatastoreAdapter();
    initialiseIdentifierFactory(omfContext);
    logConfiguration();
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
//      throw new NucleusUserException(LOCALISER_RDBMS.msg("039003", idFactoryName)).setFatal();
    }
    Map<String, String> props = Maps.newHashMap();
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
    Set<String> opts = Sets.newHashSet();
    opts.add("TransactionIsolationLevel.read-committed");
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
      StatementExpressionIndex[] stmtExprIndex) {
    return new KeyOnlyFieldManager((Key) obj);
  }

  @Override
  public Object getResultValueAtPosition(Object key, JavaTypeMapping mapping, int position) {
    // this is the key, and we're only using this for keys, so just return it.
    return key;
  }

  public FieldManager getFieldManagerForStatementGeneration(StateManager elementSM, Object stmt,
      StatementExpressionIndex[] stmtExprIndex, boolean checkNonNullable) {
    // TODO(maxr)
    return null;
  }

  public boolean insertValuesOnInsert(DatastoreMapping datastoreMapping) {
    return true;
  }

  public boolean allowsBatching() {
    return false;
  }

  public ResultObjectFactory newResultObjectFactory(DatastoreClass table, int[] fieldNumbers,
      AbstractClassMetaData acmd, StatementExpressionIndex[] statementExpressionIndex,
      int[] datastoreIdentityExpressionIndex, int[] versionIndex, boolean ignoreCache,
      boolean discriminator, boolean hasMetaDataInResults, FetchPlan fetchPlan,
      Class persistentClass) {
    // TODO(maxr)
    return null;
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
}
