// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Book;

import javax.persistence.EntityTransaction;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAInsertionTest extends JPATestCase {

  public void testSimpleInsert() throws EntityNotFoundException {
    Book b1 = new Book();
    b1.setAuthor("jimmy");
    b1.setIsbn("isbn");
    b1.setTitle("the title");
    assertNull(b1.getId());
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(b1);
    txn.commit();
    assertNotNull(b1.getId());
    Entity entity = ldth.ds.get(KeyFactory.decodeKey(b1.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("author"));
    assertEquals("isbn", entity.getProperty("isbn"));
    assertEquals("the title", entity.getProperty("title"));
    assertEquals(Book.class.getName(), entity.getKind());
  }

  public void testSimpleInsertWithNamedKey() throws EntityNotFoundException {
    Book b1 = new Book("named key");
    b1.setAuthor("jimmy");
    b1.setIsbn("isbn");
    b1.setTitle("the title");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(b1);
    txn.commit();
    assertEquals("named key", KeyFactory.decodeKey(b1.getId()).getName());
    Entity entity = ldth.ds.get(KeyFactory.decodeKey(b1.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("author"));
    assertEquals("isbn", entity.getProperty("isbn"));
    assertEquals("the title", entity.getProperty("title"));
    assertEquals(Book.class.getName(), entity.getKind());
    assertEquals("named key", entity.getKey().getName());
  }

}
