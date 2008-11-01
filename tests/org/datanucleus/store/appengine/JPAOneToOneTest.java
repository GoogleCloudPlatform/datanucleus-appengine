// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasOneToOneJPA;
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
    

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
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

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(bookEntity.getKey(), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
  }

  public void testInsert_NewParentExistingChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(b);
    em.persist(hasKeyPk);
    txn.commit();
    assertNotNull(b.getId());

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);

    txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(pojo.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(b.getId()), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPk.getId(), pojoEntity.getProperty("haskeypk_id"));
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

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertTrue(pojoEntity.getProperties().keySet().contains("book_id"));
    assertNull(pojoEntity.getProperty("book_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));

    txn = em.getTransaction();
    txn.begin();
    Book b = newBook();
    pojo.setBook(b);
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    pojo.setHasKeyPK(hasKeyPk);
    em.merge(pojo);
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(b.getId()), pojoEntity.getProperty("book_id"));
    assertEquals(hasKeyPk.getId(), pojoEntity.getProperty("haskeypk_id"));
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(pojo.getId());

    txn = em.getTransaction();
    txn.begin();
    b.setIsbn("yam");
    hasKeyPk.setStr("yar");
    em.merge(pojo);
    txn.commit();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(pojo.getId());

    txn = em.getTransaction();
    txn.begin();
    pojo = em.find(HasOneToOneJPA.class, pojo.getId());
    pojo.getBook().setIsbn("yam");
    pojo.getHasKeyPK().setStr("yar");
    txn.commit();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
  }

  public void testUpdate_NullOutChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    HasOneToOneJPA pojo = new HasOneToOneJPA();
    pojo.setBook(b);
    pojo.setHasKeyPK(hasKeyPk);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(pojo);
    txn.commit();

    txn = em.getTransaction();
    txn.begin();
    pojo.setBook(null);
    pojo.setHasKeyPK(null);
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

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertTrue(pojoEntity.getProperties().keySet().contains("book_id"));
    assertNull(pojoEntity.getProperty("book_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));
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
    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    pojoEntity.setProperty("book_id", bookEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    ldth.ds.put(pojoEntity);

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    HasOneToOneJPA pojo = em.find(HasOneToOneJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getBook());
    assertEquals("auth", pojo.getBook().getAuthor());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
    txn.commit();
  }

  public void testQuery() {
    Entity bookEntity = Book.newBookEntity("auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
    pojoEntity.setProperty("book_id", bookEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
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

      Entity pojoEntity = new Entity(HasOneToOneJPA.class.getSimpleName());
      pojoEntity.setProperty("book_id", bookEntity.getKey());
      pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
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

  private Book newBook() {
    Book b = new Book();
    b.setAuthor("max");
    b.setIsbn("22333");
    b.setTitle("yam");
    return b;
  }
}
