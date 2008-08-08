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

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    emf = Persistence.createEntityManagerFactory("test");
    em = emf.createEntityManager();
  }

  protected void tearDown() throws Exception {
    em.close();
    emf.close();
    super.tearDown();
  }
}
