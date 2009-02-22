// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;

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
    beginTxn();
    em.persist(b1);
    commitTxn();
    assertNotNull(b1.getId());
    Entity entity = ldth.ds.get(KeyFactory.stringToKey(b1.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("author"));
    assertEquals("isbn", entity.getProperty("isbn"));
    assertEquals("the title", entity.getProperty("title"));
    assertEquals(Book.class.getSimpleName(), entity.getKind());
  }

  public void testSimpleInsertWithNamedKey() throws EntityNotFoundException {
    Book b1 = new Book("named key");
    b1.setAuthor("jimmy");
    b1.setIsbn("isbn");
    b1.setTitle("the title");
    beginTxn();
    em.persist(b1);
    commitTxn();
    assertEquals("named key", KeyFactory.stringToKey(b1.getId()).getName());
    Entity entity = ldth.ds.get(KeyFactory.stringToKey(b1.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("author"));
    assertEquals("isbn", entity.getProperty("isbn"));
    assertEquals("the title", entity.getProperty("title"));
    assertEquals(Book.class.getSimpleName(), entity.getKind());
    assertEquals("named key", entity.getKey().getName());
  }

  public void testInsertWithKeyPk() {
    HasKeyPkJPA hk = new HasKeyPkJPA();

    beginTxn();
    em.persist(hk);
    commitTxn();

    assertNotNull(hk.getId());
    assertNull(hk.getAncestorId());
  }



  public void testInsertWithNamedKeyPk() {
    HasKeyPkJPA hk = new HasKeyPkJPA();
    hk.setId(KeyFactory.createKey(HasKeyPkJPA.class.getSimpleName(), "name"));
    beginTxn();
    em.persist(hk);
    commitTxn();

    assertNotNull(hk.getId());
    assertEquals("name", hk.getId().getName());
  }
}
