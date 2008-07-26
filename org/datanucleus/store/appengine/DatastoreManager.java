// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.Extent;
import org.datanucleus.store.exceptions.NoExtentException;
import org.datanucleus.ObjectManager;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.ConnectionFactory;
import org.datanucleus.ManagedConnection;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.jdo.JDOConnectionImpl;
import org.datanucleus.metadata.AbstractClassMetaData;

import java.util.Date;
import java.io.PrintStream;
import java.io.Writer;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreManager extends AbstractStoreManager {

  public DatastoreManager(ClassLoaderResolver clr, OMFContext omfContext)
  {
      super("appengine", clr, omfContext);

      // Check if JAXB API/RI JARs are in CLASSPATH
//      ClassUtils.assertClassForJarExistsInClasspath(clr, "javax.xml.bind.JAXBContext", "jaxb-api.jar");
//      ClassUtils.assertClassForJarExistsInClasspath(clr, "com.sun.xml.bind.api.JAXBRIContext", "jaxb-impl.jar");

      // Handler for persistence process
      persistenceHandler = new DatastorePersistenceHandler(this);

      logConfiguration();
  }

  public NucleusConnection getNucleusConnection(ObjectManager om) {
    ConnectionFactory cf = getOMFContext().getConnectionFactoryRegistry().lookupConnectionFactory(txConnectionFactoryName);

    final ManagedConnection mc;
    final boolean enlisted;
    if (!om.getTransaction().isActive()) {
        // no active transaction so dont enlist
      enlisted = false;
    } else {
      enlisted = true;
    }
    mc = cf.getConnection(enlisted ? om : null, null); // Will throw exception if already locked

    // Lock the connection now that it is in use by the user
    mc.lock();

    return new JDOConnectionImpl(mc.getConnection(), new Runnable() {
      public void run() {
        // Unlock the connection now that the user has finished with it
        mc.unlock();
        if (!enlisted) {
            // Close the (unenlisted) connection (committing its statements)
            // TODO Anything to do here to commit with an XML connection?
        }
      }
    });
  }

  public Date getDatastoreDate() {
    throw new UnsupportedOperationException();
  }

  public void printInformation(String category, PrintStream ps) throws Exception {
  }

  public void addClasses(String[] classes, ClassLoaderResolver clr, Writer writer,
      boolean completeDdl) {
  }

  public void removeAllClasses(ClassLoaderResolver clr) {
  }

  public Extent getExtent(ObjectManager om, Class c, boolean subclasses) {
    AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(c, om.getClassLoaderResolver());
    if (!cmd.isRequiresExtent()) {
        throw new NoExtentException(c.getName());
    }
    return new DatastoreExtent(om, c, subclasses, cmd);
  }
}
