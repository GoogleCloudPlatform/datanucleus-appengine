// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestCase;

import javax.jdo.PersistenceManagerFactory;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import java.util.Properties;

/**
 * Base testcase for tests that need a {@link PersistenceManagerFactory}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JDOTestCase extends TestCase {

  protected PersistenceManagerFactory pmf;
  protected PersistenceManager pm;

  protected LocalDatastoreTestHelper ldth = new LocalDatastoreTestHelper();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth.setUp();
    Properties properties = new Properties();
    properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
                    "org.datanucleus.jdo.JDOPersistenceManagerFactory");
    properties.setProperty("javax.jdo.option.ConnectionURL","appengine");
    properties.setProperty("datanucleus.NontransactionalRead", Boolean.TRUE.toString());
    properties.setProperty("datanucleus.NontransactionalWrite", Boolean.TRUE.toString());
    properties.setProperty("datanucleus.identifier.case", "PreserveCase");
    pmf = JDOHelper.getPersistenceManagerFactory(properties);
    pm = pmf.getPersistenceManager();
  }

  @Override
  protected void tearDown() throws Exception {
    ldth.tearDown();
    pm.close();
    pmf.close();
    super.tearDown();
  }
}
