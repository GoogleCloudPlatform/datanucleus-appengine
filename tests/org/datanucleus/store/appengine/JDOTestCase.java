// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestCase;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.persistence.Persistence;

/**
 * Base testcase for tests that need a {@link PersistenceManagerFactory}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JDOTestCase extends TestCase {

  protected PersistenceManagerFactory pmf;
  protected PersistenceManager pm;

  protected LocalDatastoreTestHelper ldth;
  private boolean failed = false;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth = new LocalDatastoreTestHelper();
    ldth.setUp();
    pmf = JDOHelper.getPersistenceManagerFactory(getPersistenceManagerFactoryName().name());
    pm = pmf.getPersistenceManager();
  }

  @Override
  protected void runTest() throws Throwable {
    try {
      super.runTest();
    } catch (Throwable t) {
      failed = true;
      throw t;
    }
  }

  public enum PersistenceManagerFactoryName { transactional, nontransactional }

  /**
   * By default we use a datasource that requires txns.
   * Override this if your test needs to use a different instance.
   */
  protected PersistenceManagerFactoryName getPersistenceManagerFactoryName() {
    return PersistenceManagerFactoryName.transactional;
  }

  @Override
  protected void tearDown() throws Exception {
    boolean throwIfActiveTxn = !failed;
    failed = false;
    ldth.tearDown(throwIfActiveTxn);
    ldth = null;
    if (pm.currentTransaction().isActive()) {
      pm.currentTransaction().rollback();
    }
    pm.close();
    pm = null;
    pmf.close();
    pmf = null;
    super.tearDown();
  }


  protected void beginTxn() {
    pm.currentTransaction().begin();
  }

  protected void commitTxn() {
    pm.currentTransaction().commit();
  }

  protected void rollbackTxn() {
    pm.currentTransaction().rollback();
  }

  protected void makePersistentInTxn(Object obj) {
    boolean success = false;
    beginTxn();
    try {
      pm.makePersistent(obj);
      commitTxn();
      success = true;
    } finally {
      if (!success) {
        rollbackTxn();
      }
    }
  }

  protected void switchDatasource(PersistenceManagerFactoryName name) {
    pm.close();
    pmf.close();
    pmf = JDOHelper.getPersistenceManagerFactory(name.name());
    pm = pmf.getPersistenceManager();
  }
}
