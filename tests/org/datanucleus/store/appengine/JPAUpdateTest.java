// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import javax.persistence.EntityTransaction;

import org.datanucleus.test.Book;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JPAUpdateTest extends JPATestCase {

  public void testSimpleUpdate() throws EntityNotFoundException {
    Key key = ldth.ds.put(Book.newBookEntity("jimmy", "12345", "the title"));

    String keyStr = KeyFactory.encodeKey(key);
    Book book = em.find(Book.class, keyStr);
    EntityTransaction tx = em.getTransaction();

    assertEquals(keyStr, book.getId());
    assertEquals("jimmy", book.getAuthor());
    assertEquals("12345", book.getIsbn());
    assertEquals("the title", book.getTitle());

    tx.begin();
    book.setIsbn("56789");
    tx.commit();

    Entity bookCheck = ldth.ds.get(key);
    assertEquals("jimmy", bookCheck.getProperty("author"));
    assertEquals("56789", bookCheck.getProperty("isbn"));
    assertEquals("the title", bookCheck.getProperty("title"));
  }
}
