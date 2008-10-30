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

  protected LocalDatastoreTestHelper ldth;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth = new LocalDatastoreTestHelper();
    ldth.setUp();
    emf = Persistence.createEntityManagerFactory("nontransactional");
    em = emf.createEntityManager();
  }

  @Override
  protected void tearDown() throws Exception {
    ldth.tearDown();
    ldth = null;
    if (em.getTransaction().isActive()) {
      em.getTransaction().rollback();
    }
    em.close();
    em = null;
    emf.close();
    emf = null;
    super.tearDown();
  }
}
