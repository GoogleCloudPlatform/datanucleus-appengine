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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import static org.datanucleus.store.appengine.TestUtils.assertKeyParentNull;
import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasOneToOneChildAtMultipleLevelsJPA;
import org.datanucleus.test.HasOneToOneJPA;
import org.datanucleus.test.HasOneToOneLongPkJPA;
import org.datanucleus.test.HasOneToOneLongPkParentJPA;
import org.datanucleus.test.HasOneToOneLongPkParentKeyPkJPA;
import org.datanucleus.test.HasOneToOneParentJPA;
import org.datanucleus.test.HasOneToOneParentKeyPkJPA;
import org.datanucleus.test.HasOneToOneStringPkJPA;
import org.datanucleus.test.HasOneToOneStringPkParentJPA;
import org.datanucleus.test.HasOneToOneStringPkParentKeyPkJPA;
import org.datanucleus.test.HasOneToOneWithNonDeletingCascadeJPA;
import org.easymock.EasyMock;

import java.util.List;

import javax.jdo.JDOFatalUserException;
import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAOneToOneTest extends JPATestCase {

  public void testInsert_NewParentAndChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.stringToKey(hasParent.getId()), hasParentEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getId());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getId(), hasParentKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    assertCountsInDatastore(1, 1);
  }

  private void persistInTxn(Object obj) {
    beginTxn();
    em.persist(obj);
    commitTxn();
  }

  public void testInsert_NewParentExistingChild() throws EntityNotFoundException {
    // this can only work on a nontransactional datasource
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);

    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();

    persistInTxn(b);
    persistInTxn(hasKeyPk);
    persistInTxn(hasParent);
    persistInTxn(hasParentKeyPk);

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    pojo.setHasParentKeyPK(hasParentKeyPk);

    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      rollbackTxn();
    }

    assertNotNull(pojo.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertKeyParentNull(bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertKeyParentNull(hasKeyPkEntity, hasKeyPk.getId());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertKeyParentNull(hasParentEntity, hasParent.getId());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertKeyParentNull(hasParentKeyPkEntity, hasParentKeyPk.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    HasOneToOneJPA pojo = new HasOneToOneJPA();

    beginTxn();
    em.persist(pojo);
    commitTxn();
    assertNotNull(pojo.getId());
    assertNull(pojo.getBook());
    assertNull(pojo.getHasKeyPK());
    assertNull(pojo.getHasParent());
    assertNull(pojo.getHasParentKeyPK());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    beginTxn();
    Book b = newBook();
    pojo.setBook(b);

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    pojo.setHasKeyPK(hasKeyPk);

    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);

    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    em.merge(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());

    pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getId());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertKeyParentEquals(pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    beginTxn();
    b.setIsbn("yam");
    hasKeyPk.setStr("yar");
    hasParent.setStr("yap");
    hasParentKeyPk.setStr("yag");
    em.merge(pojo);
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals("yap", hasParentEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getId());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals("yag", hasParentKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    beginTxn();
    pojo = em.find(HasOneToOneJPA.class, pojo.getId());
    pojo.getBook().setIsbn("yam");
    pojo.getHasKeyPK().setStr("yar");
    pojo.getHasParent().setStr("yap");
    pojo.getHasParentKeyPK().setStr("yag");
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals("yap", hasParentEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getId());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals("yag", hasParentKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_NullOutChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    beginTxn();
    pojo.setBook(null);
    pojo.setHasKeyPK(null);
    pojo.setHasParent(null);
    pojo.setHasParentKeyPK(null);
    em.merge(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(b.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasParentKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));

    assertCountsInDatastore(1, 0);
  }

  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Book b = newBook();
    beginTxn();
    em.persist(b);
    commitTxn();
    HasOneToOneWithNonDeletingCascadeJPA pojo = new HasOneToOneWithNonDeletingCascadeJPA();
    pojo.setBook(b);

    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }

  public void testFind() throws EntityNotFoundException {
    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity =
        new Entity(HasOneToOneParentJPA.class.getSimpleName(), pojoEntity.getKey());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity =
        new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    beginTxn();
    HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getBook());
    assertEquals("auth", pojo.getBook().getAuthor());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
    assertNotNull(pojo.getHasParent());
    assertEquals("yap", pojo.getHasParent().getStr());
    assertNotNull(pojo.getHasParentKeyPK());
    assertEquals("yag", pojo.getHasParentKeyPK().getStr());
    commitTxn();
  }

  public void testQuery() {
    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity =
        new Entity(HasOneToOneParentJPA.class.getSimpleName(), pojoEntity.getKey());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity =
        new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " where id = :key");
    q.setParameter("key", pojoEntity.getKey());
    beginTxn();
    @SuppressWarnings("unchecked")
    List<HasOneToOneJPA> result = (List<HasOneToOneJPA>) q.getResultList();
    assertEquals(1, result.size());
    HasOneToOneJPA pojo = result.get(0);
    assertNotNull(pojo.getBook());
    assertEquals("auth", pojo.getBook().getAuthor());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
    assertNotNull(pojo.getHasParent());
    assertEquals("yap", pojo.getHasParent().getStr());
    assertNotNull(pojo.getHasParentKeyPK());
    assertEquals("yag", pojo.getHasParentKeyPK().getStr());
    commitTxn();
  }

  public void testChildFetchedLazily() throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    try {
      setUp();

      Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
      ldth.ds.put(pojoEntity);

      Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
      ldth.ds.put(bookEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity hasParentEntity =
          new Entity(HasOneToOneParentJPA.class.getSimpleName(), pojoEntity.getKey());
      hasParentEntity.setProperty("str", "yap");
      ldth.ds.put(hasParentEntity);

      Entity hasParentKeyPkEntity =
          new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
      hasParentKeyPkEntity.setProperty("str", "yag");
      ldth.ds.put(hasParentKeyPkEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(txn.getId()).andReturn("1").times(2);
      txn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(txn);
      EasyMock.expect(ds.beginTransaction()).andReturn(txn);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(ds.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      beginTxn();
      HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  public void testDeleteParentDeletesChild() {

    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity =
        new Entity(HasOneToOneParentJPA.class.getSimpleName(), pojoEntity.getKey());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentPkEntity =
        new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasParentPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentPkEntity);

    beginTxn();
    HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.keyToString(pojoEntity.getKey()));
    em.remove(pojo);
    commitTxn();
    assertCountsInDatastore(0, 0);
  }

  public void testChangeParent() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Book b1 = newBook();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b1);
    beginTxn();
    em.persist(pojo);
    commitTxn();

    HasOneToOneJPA pojo2 = new HasOneToOneJPA();
    beginTxn();
    pojo2.setBook(b1);
    em.persist(pojo2);
    try {
      commitTxn();
      em.close();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testNewParentNewChild_SetNamedKeyOnChild() throws EntityNotFoundException {
    HasOneToOneJPA pojo = new HasOneToOneJPA();
    Book b = newBook("named key");
    pojo.setBook(b);
    beginTxn();
    em.persist(pojo);
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertEquals("named key", bookEntity.getKey().getName());
  }

  public void testInsert_NewParentAndChild_LongKeyOnParent() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneLongPkParentJPA hasParent = new HasOneToOneLongPkParentJPA();
    HasOneToOneLongPkParentKeyPkJPA hasParentKeyPk = new HasOneToOneLongPkParentKeyPkJPA();

    HasOneToOneLongPkJPA pojo = new HasOneToOneLongPkJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.stringToKey(hasParent.getId()), hasParentEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentEntity, hasParent.getId());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getId(), hasParentKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getId());

    Entity pojoEntity = ldth.ds.get(TestUtils.createKey(pojo, pojo.getId()));
    assertNotNull(pojoEntity);

    assertEquals(HasOneToOneLongPkJPA.class.getName(), 1, countForClass(HasOneToOneLongPkJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), 1, countForClass(HasKeyPkJPA.class));
    assertEquals(HasOneToOneLongPkParentJPA.class.getName(),1, countForClass(HasOneToOneLongPkParentJPA.class));
    assertEquals(HasOneToOneLongPkParentKeyPkJPA.class.getName(),
                 1, countForClass(HasOneToOneLongPkParentKeyPkJPA.class));
  }

  public void testInsert_NewParentAndChild_StringKeyOnParent() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneStringPkParentJPA hasParent = new HasOneToOneStringPkParentJPA();
    HasOneToOneStringPkParentKeyPkJPA hasParentKeyPk = new HasOneToOneStringPkParentKeyPkJPA();

    HasOneToOneStringPkJPA pojo = new HasOneToOneStringPkJPA();
    pojo.setId("yar");
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.stringToKey(hasParent.getId()), hasParentEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentEntity, hasParent.getId());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getId(), hasParentKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getId());

    Entity pojoEntity = ldth.ds.get(TestUtils.createKey(pojo, pojo.getId()));
    assertNotNull(pojoEntity);

    assertEquals(HasOneToOneStringPkJPA.class.getName(), 1, countForClass(HasOneToOneStringPkJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), 1, countForClass(HasKeyPkJPA.class));
    assertEquals(HasOneToOneStringPkParentJPA.class.getName(),1, countForClass(HasOneToOneStringPkParentJPA.class));
    assertEquals(HasOneToOneStringPkParentKeyPkJPA.class.getName(),
                 1, countForClass(HasOneToOneStringPkParentKeyPkJPA.class));
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentEm() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    HasOneToOneJPA pojo = new HasOneToOneJPA();
    Book book = new Book();
    em.persist(book);
    em.close();
    em = emf.createEntityManager();
    pojo.setBook(book);
    em.persist(pojo);
    try {
      em.close();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }

    assertEquals(1, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Book.class));
    em = emf.createEntityManager();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertNull(pojo.getBook());
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSameEm() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    HasOneToOneJPA pojo = new HasOneToOneJPA();
    Book book = new Book();
    em.persist(book);
    em.close();
    em = emf.createEntityManager();
    pojo.setBook(book);
    em.persist(pojo);
    try {
      em.close();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }

    assertEquals(1, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Book.class));
    em = emf.createEntityManager();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertNull(pojo.getBook());
  }

  public void testChildAtMultipleLevels() {
    HasOneToOneChildAtMultipleLevelsJPA pojo = new HasOneToOneChildAtMultipleLevelsJPA();
    Book b1 = new Book();
    pojo.setBook(b1);
    HasOneToOneChildAtMultipleLevelsJPA child = new HasOneToOneChildAtMultipleLevelsJPA();
    Book b2 = new Book();
    child.setBook(b2);
    pojo.setChild(child);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    beginTxn();
    pojo = em.find(HasOneToOneChildAtMultipleLevelsJPA.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(child.getBook(), b2);
    commitTxn();
  }

  private Book newBook() {
    return newBook(null);
  }

  private Book newBook(String namedKey) {
    Book b = new Book(namedKey);
    b.setAuthor("max");
    b.setIsbn("22333");
    b.setTitle("yam");
    return b;
  }

  private int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(new Query(clazz.getSimpleName())).countEntities();
  }

  private void assertCountsInDatastore(int expectedParent, int expectedChildren) {
    assertEquals(
        HasOneToOneJPA.class.getName(), expectedParent, countForClass(HasOneToOneJPA.class));
    assertEquals(
        Book.class.getName(), expectedChildren, countForClass(Book.class));
    assertEquals(
        HasKeyPkJPA.class.getName(), expectedChildren, countForClass(HasKeyPkJPA.class));
    assertEquals(HasOneToOneParentJPA.class.getName(),
        expectedChildren, countForClass(HasOneToOneParentJPA.class));
    assertEquals(HasOneToOneParentKeyPkJPA.class.getName(),
        expectedChildren, countForClass(HasOneToOneParentKeyPkJPA.class));
  }

}
