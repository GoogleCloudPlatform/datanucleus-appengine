// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestCase;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

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
    pmf = JDOHelper.getPersistenceManagerFactory("jdo_tests.properties");
    pm = pmf.getPersistenceManager();
  }

  @Override
  protected void tearDown() throws Exception {
    ldth.tearDown();
    ldth = null;
    pm.close();
    pm = null;
    pmf.close();
    pmf = null;
    super.tearDown();
  }
}
