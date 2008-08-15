// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestCase;

import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityManager;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPATestCase extends TestCase {

  protected EntityManagerFactory emf;
  protected EntityManager em;

  protected LocalDatastoreTestHelper ldth = new LocalDatastoreTestHelper();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth.setUp();
    emf = Persistence.createEntityManagerFactory("nontransactional");
    em = emf.createEntityManager();
  }

  protected void tearDown() throws Exception {
    em.close();
    emf.close();
    ldth.tearDown();
    super.tearDown();
  }
}
