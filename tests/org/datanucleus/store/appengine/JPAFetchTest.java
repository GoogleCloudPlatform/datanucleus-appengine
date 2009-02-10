// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Book;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAFetchTest extends JPATestCase {

  @Override
  protected EntityManagerFactoryName getEntityManagerFactoryName() {
    return EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed;
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
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Key key = ldth.ds.put(Book.newBookEntity("max", "47", "yam"));

    String keyStr = KeyFactory.keyToString(key);
    Book book = em.find(Book.class, keyStr);
    assertNotNull(book);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
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
