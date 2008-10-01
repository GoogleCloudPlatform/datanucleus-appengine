// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasStringAncestorKeyPkJPA;

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
    assertEquals(Book.class.getSimpleName(), entity.getKind());
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
    assertEquals(Book.class.getSimpleName(), entity.getKind());
    assertEquals("named key", entity.getKey().getName());
  }

  public void testInsertWithKeyPk() {
    HasKeyPkJPA hk = new HasKeyPkJPA();

    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(hk);
    txn.commit();

    assertNotNull(hk.getId());
    assertNull(hk.getAncestorId());
  }


  // These tests can't pass because there's no way to get JPA to persist
  // a non-pk field of type Key.
//  public void testInsertWithKeyPkAndAncestor() throws EntityNotFoundException {
//    Entity e = new Entity("yam");
//    ldth.ds.put(e);
//    HasKeyPkJPA hk1 = new HasKeyPkJPA();
//    hk1.setAncestorId(e.getKey());
//    EntityTransaction txn = em.getTransaction();
//    txn.begin();
//    em.persist(hk1);
//    txn.commit();
//
//    Entity reloaded = ldth.ds.get(hk1.getId());
//    assertEquals(hk1.getAncestorId(), reloaded.getKey().getParent());
//  }
//
//  public void testInsertWithStringPkAndKeyAncestor() throws EntityNotFoundException {
//    Entity e = new Entity("yam");
//    ldth.ds.put(e);
//    HasKeyAncestorKeyStringPkJPA hk1 = new HasKeyAncestorKeyStringPkJPA();
//    hk1.setAncestorKey(e.getKey());
//    EntityTransaction txn = em.getTransaction();
//    txn.begin();
//    em.persist(hk1);
//    txn.commit();
//
//    Entity reloaded = ldth.ds.get(KeyFactory.decodeKey(hk1.getKey()));
//    assertEquals(hk1.getAncestorKey(), reloaded.getKey().getParent());
//  }

  public void testInsertWithKeyPkAndStringAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJPA hk1 = new HasStringAncestorKeyPkJPA();
    hk1.setAncestorKey(KeyFactory.encodeKey(e.getKey()));
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(hk1);
    txn.commit();

    Entity reloaded = ldth.ds.get(hk1.getKey());
    assertEquals(hk1.getAncestorKey(), KeyFactory.encodeKey(reloaded.getKey().getParent()));
  }


  public void testInsertWithNamedKeyPk() {
    HasKeyPkJPA hk = new HasKeyPkJPA();
    hk.setId(KeyFactory.createKey("something", "name"));
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(hk);
    txn.commit();

    assertNotNull(hk.getId());
    assertEquals("name", hk.getId().getName());
  }
}
