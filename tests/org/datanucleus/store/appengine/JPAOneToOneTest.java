// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Query;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasOneToOneJPA;
import org.datanucleus.test.HasOneToOneParentJPA;
import org.datanucleus.test.HasOneToOneParentKeyPkJPA;
import org.datanucleus.test.HasOneToOneWithNonDeletingCascadeJPA;
import org.easymock.EasyMock;

import java.util.List;

import javax.persistence.EntityTransaction;

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

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.decodeKey(b.getId()), bookEntity.getKey());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.decodeKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.decodeKey(hasParent.getId()), hasParentEntity.getKey());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getId(), hasParentKeyPkEntity.getKey());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(bookEntity.getKey(), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(hasParentEntity.getKey(), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPkEntity.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_NewParentExistingChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    HasOneToOneParentJPA hasParent = new HasOneToOneParentJPA();
    HasOneToOneParentKeyPkJPA hasParentKeyPk = new HasOneToOneParentKeyPkJPA();

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(b);
    em.persist(hasKeyPk);
    em.persist(hasParent);
    em.persist(hasParentKeyPk);
    txn.commit();
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    pojo.setHasParentKeyPK(hasParentKeyPk);

    txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(pojo.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(b.getId()), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPk.getId(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(KeyFactory.decodeKey(hasParent.getId()), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPk.getId(), pojoEntity.getProperty("hasparentkeypk_id"));

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    HasOneToOneJPA pojo = new HasOneToOneJPA();

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();
    assertNotNull(pojo.getId());
    assertNull(pojo.getBook());
    assertNull(pojo.getHasKeyPK());
    assertNull(pojo.getHasParent());
    assertNull(pojo.getHasParentKeyPK());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertTrue(pojoEntity.getProperties().keySet().contains("book_id"));
    assertNull(pojoEntity.getProperty("book_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparent_id"));
    assertNull(pojoEntity.getProperty("hasparent_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparentkeypk_id"));
    assertNull(pojoEntity.getProperty("hasparentkeypk_id"));

    txn = em.getTransaction();
    txn.begin();
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
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(b.getId()), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPk.getId(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(KeyFactory.decodeKey(hasParent.getId()), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPk.getId(), pojoEntity.getProperty("hasparentkeypk_id"));

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

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    txn = em.getTransaction();
    txn.begin();
    b.setIsbn("yam");
    hasKeyPk.setStr("yar");
    hasParent.setStr("yap");
    hasParentKeyPk.setStr("yag");
    em.merge(pojo);
    txn.commit();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));

    Entity hasParentEntity = ldth.ds.get(KeyFactory.decodeKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals("yap", hasParentEntity.getProperty("str"));

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals("yag", hasParentKeyPkEntity.getProperty("str"));

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

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(hasParent.getId());
    assertNotNull(hasParentKeyPk.getId());
    assertNotNull(pojo.getId());

    txn = em.getTransaction();
    txn.begin();
    pojo = em.find(HasOneToOneJPA.class, pojo.getId());
    pojo.getBook().setIsbn("yam");
    pojo.getHasKeyPK().setStr("yar");
    pojo.getHasParent().setStr("yap");
    pojo.getHasParentKeyPK().setStr("yag");
    txn.commit();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));

    Entity hasParentEntity = ldth.ds.get(KeyFactory.decodeKey(hasParent.getId()));
    assertNotNull(hasParentEntity);
    assertEquals("yap", hasParentEntity.getProperty("str"));

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getId());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals("yag", hasParentKeyPkEntity.getProperty("str"));

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

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    txn = em.getTransaction();
    txn.begin();
    pojo.setBook(null);
    pojo.setHasKeyPK(null);
    pojo.setHasParent(null);
    pojo.setHasParentKeyPK(null);
    em.merge(pojo);
    txn.commit();

    try {
      ldth.ds.get(KeyFactory.decodeKey(b.getId()));
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
      ldth.ds.get(KeyFactory.decodeKey(hasParent.getId()));
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

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertTrue(pojoEntity.getProperties().keySet().contains("book_id"));
    assertNull(pojoEntity.getProperty("book_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparent_id"));
    assertNull(pojoEntity.getProperty("hasparent_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparentkeypk_id"));
    assertNull(pojoEntity.getProperty("hasparentkeypk_id"));

    assertCountsInDatastore(1, 0);
  }

  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
    Book b = newBook();
    HasOneToOneWithNonDeletingCascadeJPA pojo = new HasOneToOneWithNonDeletingCascadeJPA();
    pojo.setBook(b);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    txn = em.getTransaction();
    txn.begin();
    pojo.setBook(null);
    em.merge(pojo);
    txn.commit();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertTrue(pojoEntity.getProperties().keySet().contains("book_id"));
    assertNull(pojoEntity.getProperty("book_id"));
  }

  public void testFind() throws EntityNotFoundException {
    Entity bookEntity = Book.newBookEntity("auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity = new Entity(HasOneToOneParentJPA.class.getSimpleName());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity = new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    pojoEntity.setProperty("book_id", bookEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
    pojoEntity.setProperty("hasparentkeypk_id", hasParentKeyPkEntity.getKey());
    ldth.ds.put(pojoEntity);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getBook());
    assertEquals("auth", pojo.getBook().getAuthor());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
    assertNotNull(pojo.getHasParent());
    assertEquals("yap", pojo.getHasParent().getStr());
    assertNotNull(pojo.getHasParentKeyPK());
    assertEquals("yag", pojo.getHasParentKeyPK().getStr());
    txn.commit();
  }

  public void testQuery() {
    Entity bookEntity = Book.newBookEntity("auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity = new Entity(HasOneToOneParentJPA.class.getSimpleName());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity = new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    pojoEntity.setProperty("book_id", bookEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
    pojoEntity.setProperty("hasparentkeypk_id", hasParentKeyPkEntity.getKey());
    ldth.ds.put(pojoEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + HasOneToOneJPA.class.getName() + " where id = :key");
    q.setParameter("key", pojoEntity.getKey());
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
  }

  public void testChildFetchedLazily() throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    try {
      setUp();

      Entity bookEntity = Book.newBookEntity("auth", "22222", "the title");
      ldth.ds.put(bookEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity hasParentEntity = new Entity(HasOneToOneParentJPA.class.getSimpleName());
      hasParentEntity.setProperty("str", "yap");
      ldth.ds.put(hasParentEntity);

      Entity hasParentPkEntity = new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName());
      hasParentPkEntity.setProperty("str", "yag");
      ldth.ds.put(hasParentPkEntity);

      Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
      pojoEntity.setProperty("book_id", bookEntity.getKey());
      pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
      pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
      pojoEntity.setProperty("hasparentkeypk_id", hasParentPkEntity.getKey());
      ldth.ds.put(pojoEntity);

      // the only get we're going to perform is for the pojo
      EasyMock.expect(ds.get(pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      EntityTransaction txn = em.getTransaction();
      txn.begin();
      HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      txn.commit();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  public void testDeleteParentDeletesChild() {
    Entity bookEntity = Book.newBookEntity("auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity = new Entity(HasOneToOneParentJPA.class.getSimpleName());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentPkEntity = new Entity(HasOneToOneParentKeyPkJPA.class.getSimpleName());
    hasParentPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    pojoEntity.setProperty("book_id", bookEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
    pojoEntity.setProperty("hasparentkeypk_id", hasParentPkEntity.getKey());
    ldth.ds.put(pojoEntity);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    em.remove(pojo);
    txn.commit();

    assertCountsInDatastore(0, 0);
  }

  private Book newBook() {
    Book b = new Book();
    b.setAuthor("max");
    b.setIsbn("22333");
    b.setTitle("yam");
    return b;
  }

  private int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(new Query(clazz.getSimpleName())).countEntities();
  }

  private void assertCountsInDatastore(int expectedParent, int expectedChildren) {
    assertEquals(expectedParent, countForClass(HasOneToOneJPA.class));
    assertEquals(expectedChildren, countForClass(Book.class));
    assertEquals(expectedChildren, countForClass(HasKeyPkJPA.class));
    assertEquals(expectedChildren, countForClass(HasOneToOneParentJPA.class));
    assertEquals(expectedChildren, countForClass(HasOneToOneParentKeyPkJPA.class));
  }

}
