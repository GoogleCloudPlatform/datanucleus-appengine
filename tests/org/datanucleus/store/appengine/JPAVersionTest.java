/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.HasIntVersionJPA;
import org.datanucleus.test.HasIntegerVersionJPA;
import org.datanucleus.test.HasLongVersionJPA;
import org.datanucleus.test.HasPrimitiveLongVersionJPA;
import org.datanucleus.test.HasPrimitiveShortVersionJPA;
import org.datanucleus.test.HasShortVersionJPA;
import org.datanucleus.test.HasVersionJPA;

import javax.jdo.JDOHelper;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPAVersionTest extends JPATestCase {
  private static final String DEFAULT_VERSION_PROPERTY_NAME = "VERSION";

  public void testVersionInsert() throws EntityNotFoundException {
    testVersionInsert(new HasLongVersionJPA());
    testVersionInsert(new HasPrimitiveLongVersionJPA());
    testVersionInsert(new HasIntVersionJPA());
    testVersionInsert(new HasIntegerVersionJPA());
    testVersionInsert(new HasShortVersionJPA());
    testVersionInsert(new HasPrimitiveShortVersionJPA());
  }

  private void testVersionInsert(HasVersionJPA hv) throws EntityNotFoundException {
    beginTxn();
    hv.setValue("yarg");
    em.persist(hv);
    commitTxn();
    assertEquals(1L, hv.getVersion().longValue());

    Entity entity = ds.get(
        KeyFactory.createKey(hv.getClass().getSimpleName(), hv.getId()));
    assertNotNull(entity);
    assertEquals(1L, entity.getProperty(DEFAULT_VERSION_PROPERTY_NAME));
  }

  public void testOptimisticLocking_DeleteAlreadyDeleted() {
    testOptimisticLocking_DeleteAlreadyDeleted(HasLongVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyDeleted(HasPrimitiveLongVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyDeleted(HasIntVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyDeleted(HasIntegerVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyDeleted(HasShortVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyDeleted(HasPrimitiveShortVersionJPA.class);
  }

  private void testOptimisticLocking_DeleteAlreadyDeleted(Class<? extends HasVersionJPA> clazz) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(clazz.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionJPA hv = em.find(clazz, keyStr);

    // delete the entity in the datastore right before we commit
    ds.delete(key);
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

  public void testOptimisticLocking_DeleteAlreadyUpdated() {
    testOptimisticLocking_DeleteAlreadyUpdated(HasLongVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyUpdated(HasPrimitiveLongVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyUpdated(HasIntVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyUpdated(HasIntegerVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyUpdated(HasShortVersionJPA.class);
    testOptimisticLocking_DeleteAlreadyUpdated(HasPrimitiveShortVersionJPA.class);
  }

  private void testOptimisticLocking_DeleteAlreadyUpdated(Class<? extends HasVersionJPA> clazz) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(clazz.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionJPA hv = em.find(clazz, keyStr);

    hv.setValue("value");
    commitTxn();
    assertEquals(2L, hv.getVersion().longValue());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    beginTxn();
    hv = em.find(clazz, keyStr);
    // we update the entity directly in the datastore right before commit
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    em.remove(hv);
    ds.put(entity);
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

  public void testOptimisticLocking_UpdateAlreadyDeleted() {
    testOptimisticLocking_UpdateAlreadyDeleted(HasLongVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyDeleted(HasPrimitiveLongVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyDeleted(HasIntVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyDeleted(HasIntegerVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyDeleted(HasShortVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyDeleted(HasPrimitiveShortVersionJPA.class);
  }

  private void testOptimisticLocking_UpdateAlreadyDeleted(Class<? extends HasVersionJPA> clazz) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(clazz.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionJPA hv = em.find(clazz, keyStr);

    // delete the entity in the datastore right before we commit
    ds.delete(key);
    hv.setValue("value");
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

  public void testOptimisticLocking_Merge() {
    testOptimisticLocking_Merge(HasLongVersionJPA.class);
    testOptimisticLocking_Merge(HasPrimitiveLongVersionJPA.class);
    testOptimisticLocking_Merge(HasIntVersionJPA.class);
    testOptimisticLocking_Merge(HasIntegerVersionJPA.class);
    testOptimisticLocking_Merge(HasShortVersionJPA.class);
    testOptimisticLocking_Merge(HasPrimitiveShortVersionJPA.class);
  }

  private void testOptimisticLocking_Merge(Class<? extends HasVersionJPA> clazz) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(clazz.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionJPA hv = em.find(clazz, keyStr);
    hv.setValue("value");
    commitTxn();
    assertEquals(2L, hv.getVersion().longValue());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    beginTxn();
    // we update the entity directly in the datastore right before commit
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    ds.put(entity);
    hv.setValue("another value");
    em.merge(hv);
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

  public void testOptimisticLocking_UpdateAlreadyUpdated() {
    testOptimisticLocking_UpdateAlreadyUpdated(HasLongVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyUpdated(HasPrimitiveLongVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyUpdated(HasIntVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyUpdated(HasIntegerVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyUpdated(HasShortVersionJPA.class);
    testOptimisticLocking_UpdateAlreadyUpdated(HasPrimitiveShortVersionJPA.class);
  }

  private void testOptimisticLocking_UpdateAlreadyUpdated(Class<? extends HasVersionJPA> clazz) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(clazz.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionJPA hv = em.find(clazz, keyStr);
    hv.setValue("value");
    commitTxn();
    assertEquals(2L, hv.getVersion().longValue());

    beginTxn();
    hv = em.find(clazz, keyStr);
    hv.setValue("a different value");
    commitTxn();
    assertEquals(3L, hv.getVersion().longValue());

    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 4L);

    beginTxn();
    hv = em.find(clazz, keyStr);
    hv.setValue("another value");
    // we update the entity directly in the datastore right before commit
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    ds.put(entity);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (RollbackException re) {
      // good
      assertTrue(re.getCause() instanceof OptimisticLockException);
    }
    // make sure the version didn't change on the model object
    assertEquals(3L, JDOHelper.getVersion(hv));
  }

  public void testVersionIncrement() {
    HasIntegerVersionJPA hv = new HasIntegerVersionJPA();
    beginTxn();
    hv.setValue("value");
    em.persist(hv);
    commitTxn();
    assertEquals(1L, hv.getVersion().longValue());
    beginTxn();
    hv = em.find(hv.getClass(), hv.getId());
    hv.setValue("another value");
    commitTxn();
    assertEquals(2, hv.getVersion().longValue());
    beginTxn();
    hv.setValue("yet another value");
    hv = em.merge(hv);
    commitTxn();
    assertEquals(3, hv.getVersion().longValue());
  }

  public void testVersionIncrement_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    HasIntegerVersionJPA hv = new HasIntegerVersionJPA();
    hv.setValue("value");
    em.persist(hv);
    em.close();
    assertEquals(1L, hv.getVersion().longValue());
    em = emf.createEntityManager();
    hv = em.find(hv.getClass(), hv.getId());
    hv.setValue("another value");
    em.close();
    assertEquals(2, hv.getVersion().longValue());
    em = emf.createEntityManager();
    hv.setValue("yet another value");
    hv = em.merge(hv);
    em.close();
    assertEquals(3, hv.getVersion().longValue());
  }
}
