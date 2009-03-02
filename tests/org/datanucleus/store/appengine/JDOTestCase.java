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

  protected DatastoreTestHelper ldth;
  private boolean failed = false;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth = new DatastoreTestHelper();
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
    if (!pm.isClosed()) {
      if (pm.currentTransaction().isActive()) {
        pm.currentTransaction().rollback();
      }
      pm.close();
    }
    pm = null;
    if (!pmf.isClosed()) {
      pmf.close();
    }
    pmf = null;
    ldth.tearDown(throwIfActiveTxn);
    ldth = null;
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

  protected int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(
        new com.google.appengine.api.datastore.Query(clazz.getSimpleName())).countEntities();
  }

}
