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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.HasVersionJPA;
import org.datanucleus.test.KitchenSink;

import javax.jdo.JDOHelper;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPADeleteTest extends JPATestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "VERSION";

  public void testSimpleDelete() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.keyToString(key);
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
    String keyStr = KeyFactory.keyToString(key);
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
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
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
    // make sure the version didn't change on the model object
    assertEquals(2L, JDOHelper.getVersion(hv));
  }

  public void testOptimisticLocking_Delete() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
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
    // make sure the version didn't change on the model object
    assertEquals(1L, JDOHelper.getVersion(hv));
  }

  public void testDeletePersistentNew() {
    int count = countForClass(KitchenSink.class);
    beginTxn();
    KitchenSink ks = KitchenSink.newKitchenSink();
    em.persist(ks);
    em.remove(ks);
    commitTxn();
    assertEquals(count, countForClass(KitchenSink.class));
  }

  public void testDeletePersistentNew_NoTxn() {
    int count = countForClass(KitchenSink.class);
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    KitchenSink ks = KitchenSink.newKitchenSink();
    em.persist(ks);
    em.remove(ks);
    assertEquals(count, countForClass(KitchenSink.class));
    em.close();
  }

}
