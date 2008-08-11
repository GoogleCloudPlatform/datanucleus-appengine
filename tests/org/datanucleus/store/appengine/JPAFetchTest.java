// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import org.datanucleus.test.Book;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAFetchTest extends JPATestCase {

  public void testSimpleFetch() {
    Key key = ldth.ds.put(Book.newBookEntity("max", "47", "yam"));

    String keyStr = KeyFactory.encodeKey(key);
    Book book = em.find(Book.class, keyStr);
    assertNotNull(book);
    assertEquals(keyStr, book.getId());
    assertEquals("max", book.getAuthor());
    assertEquals("47", book.getIsbn());
    assertEquals("yam", book.getTitle());
  }

  public void testFetchNonExistent() {
    Key key = ldth.ds.put(Book.newBookEntity("max", "47", "yam"));
    ldth.ds.delete(key);
    String keyStr = KeyFactory.encodeKey(key);
    assertNull(em.find(Book.class, keyStr));
  }
}
