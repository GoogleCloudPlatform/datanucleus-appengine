// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Book;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAFetchTest extends JPATestCase {

  @Override
  protected EntityManagerFactoryName getEntityManagerFactoryName() {
    return EntityManagerFactoryName.nontransactional_no_txn_allowed;
  }

  public void testSimpleFetch() {
    Key key = ldth.ds.put(Book.newBookEntity("max", "47", "yam"));

    String keyStr = KeyFactory.keyToString(key);
    Book book = em.find(Book.class, keyStr);
    assertNotNull(book);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
  }

  public void testSimpleFetchWithNonTransactionalDatasource() {
    EntityManagerFactory emf = Persistence.createEntityManagerFactory(
        EntityManagerFactoryName.nontransactional_no_txn_not_allowed.name());
    EntityManager em = emf.createEntityManager();
    try {
      Key key = ldth.ds.put(Book.newBookEntity("max", "47", "yam"));

      String keyStr = KeyFactory.keyToString(key);
      Book book = em.find(Book.class, keyStr);
      assertNotNull(book);
      assertEquals(keyStr, book.getId());
      assertEquals("max", book.getAuthor());
      assertEquals("47", book.getIsbn());
      assertEquals("yam", book.getTitle());
    } finally {
      em.close();
      emf.close();
    }
  }

  public void testSimpleFetchWithNamedKey() {
    Key key = ldth.ds.put(Book.newBookEntity("named key", "max", "47", "yam"));

    String keyStr = KeyFactory.keyToString(key);
    Book book = em.find(Book.class, keyStr);
    assertNotNull(book);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
    assertEquals("named key", KeyFactory.stringToKey(book.getId()).getName());
  }

  public void testFetchNonExistent() {
    Key key = ldth.ds.put(Book.newBookEntity("max", "47", "yam"));
    ldth.ds.delete(key);
    String keyStr = KeyFactory.keyToString(key);
    assertNull(em.find(Book.class, keyStr));
  }
}
