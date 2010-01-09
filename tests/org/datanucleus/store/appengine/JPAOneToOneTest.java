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
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import static org.datanucleus.store.appengine.TestUtils.assertKeyParentNull;
import org.datanucleus.test.Book;
import org.datanucleus.test.HasEncodedStringPkJPA;
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

import java.lang.reflect.Method;
import java.util.List;

import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAOneToOneTest extends JPATestCase {

  public void testInsert_NewParentAndChild() throws Exception {
    testInsert_NewParentAndChild(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_NoTxn() throws Exception {
    testInsert_NewParentAndChild(NEW_EM_START_END);
  }
  private void testInsert_NewParentAndChild(StartEnd startEnd) throws Exception {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();
    HasEncodedStringPkJPA notDependent = new HasEncodedStringPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);
    pojo.setNotDependent(notDependent);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(notDependent.getId());
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

    Entity notDependentEntity = ldth.ds.get(KeyFactory.stringToKey(notDependent.getId()));
    assertNotNull(notDependentEntity);

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(bookEntity.getKey(), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(hasParentEntity.getKey(), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPkEntity.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));
    assertEquals(notDependentEntity.getKey(), pojoEntity.getProperty("notdependent_id"));

    assertCountsInDatastore(1, 1);
    assertEquals(1, countForClass(notDependent.getClass()));
  }

  private void persistInTxn(Object obj, StartEnd startEnd) {
    startEnd.start();
    em.persist(obj);
    startEnd.end();
  }

  public void testInsert_NewParentExistingChild() throws Exception {
    testInsert_NewParentExistingChild(TXN_START_END);
  }
  public void testInsert_NewParentExistingChild_NoTxn() throws Exception {
    testInsert_NewParentExistingChild(NEW_EM_START_END);
  }

  private void testInsert_NewParentExistingChild(StartEnd startEnd)
      throws Exception {
    // this can only work on a nontransactional datasource
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);

    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();

    persistInTxn(b, startEnd);
    persistInTxn(hasKeyPk, startEnd);
    persistInTxn(hasParent, startEnd);
    persistInTxn(hasParentKeyPk, startEnd);

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    pojo.setHasParentKeyPK(hasParentKeyPk);

    startEnd.start();
    em.persist(pojo);
    try {
      startEnd.end();
      fail("expected exception");
    } catch (PersistenceException e) {
      if (em.getTransaction().isActive()) {
        rollbackTxn();
      }
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

  public void testInsert_ExistingParentNewChild() throws Exception {
    testInsert_ExistingParentNewChild(TXN_START_END);
  }
  public void testInsert_ExistingParentNewChild_NoTxn() throws Exception {
    testInsert_ExistingParentNewChild(NEW_EM_START_END);
  }

  private void testInsert_ExistingParentNewChild(StartEnd startEnd) throws Exception {
    HasOneToOneJPA pojo = new HasOneToOneJPA();

    startEnd.start();
    em.persist(pojo);
    startEnd.end();
    assertNotNull(pojo.getId());
    assertNull(pojo.getBook());
    assertNull(pojo.getHasKeyPK());
    assertNull(pojo.getHasParent());
    assertNull(pojo.getHasParentKeyPK());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    startEnd.start();
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
    startEnd.end();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());

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

    pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(bookEntity.getKey(), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(hasParentEntity.getKey(), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPkEntity.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));
    assertNotNull(pojoEntity);

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChildWithMerge() throws Exception {
    testUpdate_UpdateChildWithMerge(TXN_START_END);
  }
  public void testUpdate_UpdateChildWithMerge_NoTxn() throws Exception {
    testUpdate_UpdateChildWithMerge(NEW_EM_START_END);
  }
  public void testUpdate_UpdateChildWithMerge(StartEnd startEnd) throws Exception {
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

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    b.setIsbn("yam");
    hasKeyPk.setStr("yar");
    hasParent.setStr("yap");
    hasParentKeyPk.setStr("yag");
    em.merge(pojo);
    startEnd.end();

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
    testUpdate_UpdateChild(TXN_START_END);
  }
  public void testUpdate_UpdateChild_NoTxn() throws EntityNotFoundException {
    testUpdate_UpdateChild(NEW_EM_START_END);
  }

  private void testUpdate_UpdateChild(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    pojo = em.find(HasOneToOneJPA.class, pojo.getId());
    pojo.getBook().setIsbn("yam");
    pojo.getHasKeyPK().setStr("yar");
    pojo.getHasParent().setStr("yap");
    pojo.getHasParentKeyPK().setStr("yag");
    startEnd.end();

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

  public void testUpdate_NullOutChild_Merge() throws EntityNotFoundException {
    testUpdate_NullOutChild_Merge(TXN_START_END);
  }
  public void testUpdate_NullOutChild_Merge_NoTxn() throws EntityNotFoundException {
    testUpdate_NullOutChild_Merge(TXN_START_END);
  }
  private void testUpdate_NullOutChild_Merge(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo.setBook(null);
    pojo.setHasKeyPK(null);
    pojo.setHasParent(null);
    pojo.setHasParentKeyPK(null);
    em.merge(pojo);
    startEnd.end();

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

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(6, parentEntity.getProperties().size());
    assertTrue(parentEntity.hasProperty("str"));
    assertTrue(parentEntity.hasProperty("book_id"));
    assertNull(parentEntity.getProperty("book_id"));
    assertTrue(parentEntity.hasProperty("haskeypk_id"));
    assertNull(parentEntity.getProperty("haskeypk_id"));
    assertTrue(parentEntity.hasProperty("hasparent_id"));
    assertNull(parentEntity.getProperty("hasparent_id"));
    assertTrue(parentEntity.hasProperty("hasparentkeypk_id"));
    assertNull(parentEntity.getProperty("hasparentkeypk_id"));
    assertTrue(parentEntity.hasProperty("notdependent_id"));
    assertNull(parentEntity.getProperty("notdependent_id"));
    assertCountsInDatastore(1, 0);
  }
  
  public void testUpdate_NullOutChild_Update() throws EntityNotFoundException {
    testUpdate_NullOutChild_Update(TXN_START_END);
  }
  public void testUpdate_NullOutChild_Update_NoTxn() throws EntityNotFoundException {
    testUpdate_NullOutChild_Update(TXN_START_END);
  }
  private void testUpdate_NullOutChild_Update(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    pojo.setBook(null);
    pojo.setHasKeyPK(null);
    pojo.setHasParent(null);
    pojo.setHasParentKeyPK(null);
    startEnd.end();

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

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(6, parentEntity.getProperties().size());
    assertTrue(parentEntity.hasProperty("str"));
    assertTrue(parentEntity.hasProperty("book_id"));
    assertNull(parentEntity.getProperty("book_id"));
    assertTrue(parentEntity.hasProperty("haskeypk_id"));
    assertNull(parentEntity.getProperty("haskeypk_id"));
    assertTrue(parentEntity.hasProperty("hasparent_id"));
    assertNull(parentEntity.getProperty("hasparent_id"));
    assertTrue(parentEntity.hasProperty("hasparentkeypk_id"));
    assertNull(parentEntity.getProperty("hasparentkeypk_id"));
    assertTrue(parentEntity.hasProperty("notdependent_id"));
    assertNull(parentEntity.getProperty("notdependent_id"));
    assertCountsInDatastore(1, 0);
  }

  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
    testUpdate_NullOutChild_NoDelete(TXN_START_END);
  }
  public void testUpdate_NullOutChild_NoDelete_NoTxn() throws EntityNotFoundException {
    testUpdate_NullOutChild_NoDelete(NEW_EM_START_END);
  }
  private void testUpdate_NullOutChild_NoDelete(StartEnd startEnd) throws EntityNotFoundException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Book b = newBook();
    startEnd.start();
    em.persist(b);
    startEnd.end();
    HasOneToOneWithNonDeletingCascadeJPA pojo = new HasOneToOneWithNonDeletingCascadeJPA();
    pojo.setBook(b);

    startEnd.start();
    em.persist(pojo);
    try {
      startEnd.end();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }

  public void testFind() throws EntityNotFoundException {
    testFind(TXN_START_END);
  }
  public void testFind_NoTxn() throws EntityNotFoundException {
    testFind(NEW_EM_START_END);
  }
  private void testFind(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
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
    startEnd.end();
  }

  public void testQuery() {
    testQuery(TXN_START_END);
  }
  public void testQuery_NoTxn() {
    testQuery(NEW_EM_START_END);
  }
  private void testQuery(StartEnd startEnd) {
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
    q.setParameter("key", KeyFactory.keyToString(pojoEntity.getKey()));
    startEnd.start();
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
    startEnd.end();
  }

  public void testChildFetchedLazily() throws Exception {
    // force a new emf to get created after we've installed our own
    // DatastoreService mock
    emf.close();
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
      // need to close the emf before we restore the original datastore service
      emf.close();
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  public void testDeleteParentDeletesChild() {
    testDeleteParentDeletesChild(TXN_START_END);
  }
  public void testDeleteParentDeletesChild_NoTxn() {
    testDeleteParentDeletesChild(NEW_EM_START_END);
  }
  private void testDeleteParentDeletesChild(StartEnd startEnd) {

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

    Entity notDependentEntity = new Entity(HasEncodedStringPkJPA.class.getSimpleName(), pojoEntity.getKey());
    ldth.ds.put(notDependentEntity);

    assertEquals(HasOneToOneJPA.class.getName(), 1, countForClass(HasOneToOneJPA.class));

    startEnd.start();
    HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.keyToString(pojoEntity.getKey()));
    em.remove(pojo);
    startEnd.end();
    try {
      pojoEntity = ldth.ds.get(pojoEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }
    assertCountsInDatastore(0, 0);
    assertEquals(1, countForClass(HasEncodedStringPkJPA.class));
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
    testNewParentNewChild_SetNamedKeyOnChild(TXN_START_END);
  }
  public void testNewParentNewChild_SetNamedKeyOnChild_NoTxn() throws EntityNotFoundException {
    testNewParentNewChild_SetNamedKeyOnChild(NEW_EM_START_END);
  }
  private void testNewParentNewChild_SetNamedKeyOnChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToOneJPA pojo = new HasOneToOneJPA();
    Book b = newBook("named key");
    pojo.setBook(b);
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertEquals("named key", bookEntity.getKey().getName());
  }

  public void testInsert_NewParentAndChild_LongKeyOnParent() throws EntityNotFoundException {
    testInsert_NewParentAndChild_LongKeyOnParent(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_LongKeyOnParent_NoTxn() throws EntityNotFoundException {
    testInsert_NewParentAndChild_LongKeyOnParent(NEW_EM_START_END);
  }
  private void testInsert_NewParentAndChild_LongKeyOnParent(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

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
    assertEquals(bookEntity.getKey(), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(hasParentEntity.getKey(), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPkEntity.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));

    assertEquals(HasOneToOneLongPkJPA.class.getName(), 1, countForClass(HasOneToOneLongPkJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), 1, countForClass(HasKeyPkJPA.class));
    assertEquals(HasOneToOneLongPkParentJPA.class.getName(),1, countForClass(HasOneToOneLongPkParentJPA.class));
    assertEquals(HasOneToOneLongPkParentKeyPkJPA.class.getName(),
                 1, countForClass(HasOneToOneLongPkParentKeyPkJPA.class));
  }

  public void testInsert_NewParentAndChild_StringKeyOnParent() throws EntityNotFoundException {
    testInsert_NewParentAndChild_StringKeyOnParent(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_StringKeyOnParent_NoTxn() throws EntityNotFoundException {
    testInsert_NewParentAndChild_StringKeyOnParent(NEW_EM_START_END);
  }
  public void testInsert_NewParentAndChild_StringKeyOnParent(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

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
    assertEquals(bookEntity.getKey(), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(hasParentEntity.getKey(), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPkEntity.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));

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
    } catch (PersistenceException e) {
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
    } catch (PersistenceException e) {
      // good
    }

    assertEquals(1, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Book.class));
    em = emf.createEntityManager();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertNull(pojo.getBook());
  }

  public void testChildAtMultipleLevels() throws EntityNotFoundException {
    testChildAtMultipleLevels(TXN_START_END);
  }
  public void testChildAtMultipleLevels_NoTxn() throws EntityNotFoundException {
    testChildAtMultipleLevels(NEW_EM_START_END);
  }
  private void testChildAtMultipleLevels(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToOneChildAtMultipleLevelsJPA pojo = new HasOneToOneChildAtMultipleLevelsJPA();
    Book b1 = new Book();
    pojo.setBook(b1);
    HasOneToOneChildAtMultipleLevelsJPA child = new HasOneToOneChildAtMultipleLevelsJPA();
    Book b2 = new Book();
    child.setBook(b2);
    pojo.setChild(child);
    startEnd.start();
    em.persist(pojo);
    startEnd.end();
    startEnd.start();
    pojo = em.find(HasOneToOneChildAtMultipleLevelsJPA.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(child.getBook(), b2);
    startEnd.end();

    Entity pojoEntity = ldth.ds.get(pojo.getId());
    Entity childEntity = ldth.ds.get(child.getId());
    Entity book1Entity = ldth.ds.get(KeyFactory.stringToKey(b1.getId()));
    Entity book2Entity = ldth.ds.get(KeyFactory.stringToKey(b2.getId()));
    assertEquals(book1Entity.getKey(), pojoEntity.getProperty("book_id"));
    assertEquals(book2Entity.getKey(), childEntity.getProperty("book_id"));
  }

  private static final class PutPolicy implements DatastoreServiceInterceptor.Policy {
    private final List<Object[]> putParamList = Utils.newArrayList();
    public void intercept(Object o, Method method, Object[] params) {
      if (method.getName().equals("put")) {
        putParamList.add(params);
      }
    }
  }

  PutPolicy setupPutPolicy(HasOneToOneJPA pojo, HasOneToOneParentJPA hasParent, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = new PutPolicy();
    DatastoreServiceInterceptor.install(policy);
    try {
      emf.close();
      switchDatasource(getEntityManagerFactoryName());
      Book book = new Book();
      pojo.setBook(book);
      pojo.setHasParent(hasParent);
      HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
      pojo.setHasKeyPK(hasKeyPk);

      startEnd.start();
      em.persist(pojo);
      startEnd.end();
      // 1 put for the parent, 3 puts for the children, 1 more put
      // to add the child keys back on the parent
      assertEquals(5, policy.putParamList.size());
      policy.putParamList.clear();
      return policy;
    } catch (Throwable t) {
      DatastoreServiceInterceptor.uninstall();
      throw t;
    }
  }

  public void testOnlyOnePutOnChildUpdate() throws Throwable {
    testOnlyOnePutOnChildUpdate(TXN_START_END);
  }
  public void testOnlyOnePutOnChildUpdate_NoTxn() throws Throwable {
    testOnlyOnePutOnChildUpdate(NEW_EM_START_END);
  }
  private void testOnlyOnePutOnChildUpdate(JPATestCase.StartEnd startEnd)
      throws Throwable {
    HasOneToOneJPA pojo = new HasOneToOneJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    PutPolicy policy = setupPutPolicy(pojo, hasParent, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.getBook().setIsbn("88");
      pojo.getHasParent().setStr("blarg");
      pojo.getHasKeyPK().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for each child update
    assertEquals(3, policy.putParamList.size());
  }

  public void testOnlyOneParentPutOnParentAndChildUpdate() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(TXN_START_END);
  }
  public void testOnlyOneParentPutOnParentAndChildUpdate_NoTxn() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(NEW_EM_START_END);
  }
  private void testOnlyOneParentPutOnParentAndChildUpdate(JPATestCase.StartEnd startEnd)
      throws Throwable {
    HasOneToOneJPA pojo = new HasOneToOneJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    PutPolicy policy = setupPutPolicy(pojo, hasParent, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.setStr("another val");
      pojo.getBook().setIsbn("88");
      pojo.getHasParent().setStr("blarg");
      pojo.getHasKeyPK().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for the parent update, 1 put for each child update
    assertEquals(4, policy.putParamList.size());
  }

  public void testOnlyOneParentPutOnChildDelete() throws Throwable {
    // 1 put for the parent, 3 puts for the children, 1 more put
    // to add the child keys back on the parent
    int expectedPutsInSetup = 5;
    // 1 put per child that we blank out plus one more put that cascades
    // from the HasOneToOneParentJPA up to the parent as part of the delete
    int expectedPutsOnChildDelete = 3;
    testOnlyOneParentPutOnChildDelete(TXN_START_END, expectedPutsOnChildDelete);
  }
  public void testOnlyOneParentPutOnChildDelete_NoTxn() throws Throwable {
    // 1 put per child that we blank out
    int expectedPutsOnChildDelete = 4;
    testOnlyOneParentPutOnChildDelete(NEW_EM_START_END, expectedPutsOnChildDelete);
  }
  private void testOnlyOneParentPutOnChildDelete(JPATestCase.StartEnd startEnd,
                                                 int expectedPutsOnChildDelete)
      throws Throwable {
    HasOneToOneJPA pojo = new HasOneToOneJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    PutPolicy policy = setupPutPolicy(pojo, hasParent, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.setStr("another val");
      pojo.setBook(null);
      pojo.setHasParent(null);
      pojo.setHasKeyPK(null);
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // datanuc issues 1 put for each relation we blank out
    assertEquals(expectedPutsOnChildDelete, policy.putParamList.size());
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
