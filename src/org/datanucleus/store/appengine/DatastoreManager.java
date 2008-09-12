// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.Extent;
import org.datanucleus.store.NucleusConnectionImpl;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.FetchStatement;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.exceptions.NoExtentException;
import org.datanucleus.ObjectManager;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ConnectionFactory;
import org.datanucleus.ManagedConnection;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.OMFContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;

import java.util.Date;
import java.sql.DatabaseMetaData;


/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreManager extends MappedStoreManager {

  public DatastoreManager(ClassLoaderResolver clr, OMFContext omfContext) {
    super("appengine", clr, omfContext);

    // Check if datastore api is in CLASSPATH.  Don't let the hard-coded
    // jar name upset you, it's just used for error messages.  The check will
    // succeed so long as the class is available on the classpath
    ClassUtils.assertClassForJarExistsInClasspath(
        clr, "com.google.apphosting.api.datastore.DatastoreService", "external-api.jar");

    // Handler for persistence process
    persistenceHandler = new DatastorePersistenceHandler(this);
    DatabaseMetaData dmd = new DatastoreMetaData();
    dba = new DatastoreAdapter(dmd);
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
    try {
      // Create the IdentifierFactory
      Class cls = Class.forName(idFactoryClassName);
      Class[] argTypes = new Class[]
          {org.datanucleus.store.mapped.DatastoreAdapter.class, String.class, String.class,
              String.class, String.class, String.class, String.class};
      Object[] args = new Object[]
          {
              dba,
              conf.getStringProperty("datanucleus.mapping.Catalog"),
              conf.getStringProperty("datanucleus.mapping.Schema"),
              conf.getStringProperty("datanucleus.identifier.case"),
              conf.getStringProperty("datanucleus.identifier.wordSeparator"),
              conf.getStringProperty("datanucleus.identifier.tablePrefix"),
              conf.getStringProperty("datanucleus.identifier.tableSuffix")
          };
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

  @Override
  public FetchStatement getFetchStatement(DatastoreContainerObject table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatastoreContainerObject newJoinDatastoreContainerObject(AbstractMemberMetaData fmd,
      ClassLoaderResolver clr) {
    throw new UnsupportedOperationException();
  }
}
