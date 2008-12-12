// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.HasVersionJPA;
import org.datanucleus.test.KitchenSink;

import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPADeleteTest extends JPATestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "VERSION";

  public void testSimpleDelete() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.encodeKey(key);
    KitchenSink ks;
    beginTxn();
    ks = em.find(KitchenSink.class, keyStr);
    assertNotNull(ks);
    em.remove(ks);
    commitTxn();
    beginTxn();
    try {
      assertNull(em.find(KitchenSink.class, keyStr));
    } finally {
      commitTxn();
    }
  }

  public void testSimpleDeleteWithNamedKey() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity("named key", null));
    assertEquals("named key", key.getName());
    String keyStr = KeyFactory.encodeKey(key);
    KitchenSink ks;
    beginTxn();
    ks = em.find(KitchenSink.class, keyStr);
    assertNotNull(ks);
    em.remove(ks);
    commitTxn();
    beginTxn();
    try {
      assertNull(em.find(KitchenSink.class, keyStr));
    } finally {
      commitTxn();
    }
  }

  public void testOptimisticLocking_Update() {
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.encodeKey(key);
    beginTxn();
    HasVersionJPA hv = em.find(HasVersionJPA.class, keyStr);

    hv.setValue("value");
    commitTxn();
    assertEquals(2L, hv.getVersion());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    beginTxn();
    hv = em.find(HasVersionJPA.class, keyStr);
    // we update the entity directly in the datastore right before commit
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    em.remove(hv);
    ldth.ds.put(entity);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (RollbackException re) {
      // good
      assertTrue(re.getCause() instanceof OptimisticLockException);
    }
    beginTxn();
    // make sure the version didn't change on the model object
    assertEquals(2L, hv.getVersion());
    commitTxn();
  }

  public void testOptimisticLocking_Delete() {
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.encodeKey(key);
    beginTxn();
    HasVersionJPA hv = em.find(HasVersionJPA.class, keyStr);

    // delete the entity in the datastore right before we commit
    ldth.ds.delete(key);
    em.remove(hv);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (RollbackException re) {
      // good
      assertTrue(re.getCause() instanceof OptimisticLockException);
    }
    beginTxn();
    // make sure the version didn't change on the model object
    assertEquals(1L, hv.getVersion());
    commitTxn();
  }

}
