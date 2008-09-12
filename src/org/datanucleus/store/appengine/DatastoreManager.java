// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ConnectionFactory;
import org.datanucleus.ManagedConnection;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.Extent;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.NucleusConnectionImpl;
import org.datanucleus.store.exceptions.NoExtentException;

import java.util.Date;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreManager extends AbstractStoreManager {

  public DatastoreManager(ClassLoaderResolver clr, OMFContext omfContext) {
    super("appengine", clr, omfContext);

    // Check if JAXB API/RI JARs are in CLASSPATH
//      ClassUtils.assertClassForJarExistsInClasspath(clr, "javax.xml.bind.JAXBContext", "jaxb-api.jar");
//      ClassUtils.assertClassForJarExistsInClasspath(clr, "com.sun.xml.bind.api.JAXBRIContext", "jaxb-impl.jar");

    // Handler for persistence process
    persistenceHandler = new DatastorePersistenceHandler(this);

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
}
