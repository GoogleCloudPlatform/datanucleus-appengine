// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

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
    emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name());
    em = emf.createEntityManager();
  }

  protected enum EntityManagerFactoryName { transactional, nontransactional }

  /**
   * By default we use a datasource that requires txns.
   * Override this if your test needs to use a different instance.
   */
  protected EntityManagerFactoryName getEntityManagerFactoryName() {
    return EntityManagerFactoryName.transactional;
  }

  @Override
  protected void tearDown() throws Exception {
    assertTrue(DatastoreFieldManager.PARENT_KEY_MAP.get().isEmpty());
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

  protected void beginTxn() {
    em.getTransaction().begin();
  }

  protected void commitTxn() {
    em.getTransaction().commit();
  }

  protected void assertKeyParentEquals(String parentKey, Entity childEntity, Key childKey) {
    assertEquals(KeyFactory.decodeKey(parentKey), childEntity.getKey().getParent());
    assertEquals(KeyFactory.decodeKey(parentKey), childKey.getParent());
  }

  protected void assertKeyParentEquals(String parentKey, Entity childEntity, String childKey) {
    assertEquals(KeyFactory.decodeKey(parentKey), childEntity.getKey().getParent());
    assertEquals(KeyFactory.decodeKey(parentKey), KeyFactory.decodeKey(childKey).getParent());
  }

  protected void assertKeyParentNull(Entity childEntity, String childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(KeyFactory.decodeKey(childKey).getParent());
  }

  protected void assertKeyParentNull(Entity childEntity, Key childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(childKey.getParent());
  }

}
