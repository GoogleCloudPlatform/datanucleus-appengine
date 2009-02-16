// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestCase;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPATestCase extends TestCase {

  protected EntityManagerFactory emf;
  protected EntityManager em;

  protected DatastoreTestHelper ldth;

  private boolean failed = false;
  @Override
  protected void runTest() throws Throwable {
    try {
      super.runTest();
    } catch (Throwable t) {
      failed = true;
      throw t;
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth = new DatastoreTestHelper();
    ldth.setUp();
    emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name());
    em = emf.createEntityManager();
  }

  protected enum EntityManagerFactoryName {
    // nonTransactionalRead and nonTransactionalWrite are true
    transactional_ds_non_transactional_ops_allowed,
    // nonTransactionalRead and nonTransactionalWrite are false
    transactional_ds_non_transactional_ops_not_allowed,
    // nonTransactionalRead and nonTransactionalWrite are true
    nontransactional_ds_non_transactional_ops_allowed,
    // nonTransactionalRead and nonTransactionalWrite are false
    nontransactional_ds_non_transactional_ops_not_allowed
  }

  /**
   * By default we use a datasource that requires txns.
   * Override this if your test needs to use a different instance.
   */
  protected EntityManagerFactoryName getEntityManagerFactoryName() {
    return EntityManagerFactoryName.transactional_ds_non_transactional_ops_not_allowed;
  }

  @Override
  protected void tearDown() throws Exception {
    boolean throwIfActiveTxn = !failed;
    failed = false;
    if (em.isOpen()) {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }
    em = null;
    emf.close();
    emf = null;
    ldth.tearDown(throwIfActiveTxn);
    ldth = null;
    super.tearDown();
  }

  protected void beginTxn() {
    em.getTransaction().begin();
  }

  protected void commitTxn() {
    em.getTransaction().commit();
  }

  protected void rollbackTxn() {
    em.getTransaction().rollback();
  }

  protected void switchDatasource(EntityManagerFactoryName name) {
    em.close();
    emf.close();
    emf = Persistence.createEntityManagerFactory(name.name());
    em = emf.createEntityManager();
  }
}
