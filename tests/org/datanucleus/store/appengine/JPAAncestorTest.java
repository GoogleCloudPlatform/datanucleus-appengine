// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasStringAncestorKeyPkJPA;
import org.datanucleus.test.HasStringAncestorStringPkJPA;

import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAAncestorTest extends JPATestCase {

  public void testInsert_IdGen() {
    Entity bookEntity = Book.newBookEntity("max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Key bookKey = bookEntity.getKey();
    HasStringAncestorStringPkJPA ha = new HasStringAncestorStringPkJPA(KeyFactory.keyToString(bookKey));
    beginTxn();
    em.persist(ha);
    commitTxn();
    Key keyWithParent = KeyFactory.stringToKey(ha.getId());
    assertEquals(bookKey, keyWithParent.getParent());
    // now we'll issue an ancestor query directly against the datastore and see
    // if our object comes back.
    Query q = new Query(ha.getClass().getSimpleName());
    q.setAncestor(bookKey);
    Entity result = ldth.ds.prepare(q).asSingleEntity();
    assertEquals(bookKey, result.getKey().getParent());
  }

  public void testInsert_NamedKey() {
    Entity bookEntity = Book.newBookEntity("parent named key", "max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Key bookKey = bookEntity.getKey();
    Key key = new Entity(HasStringAncestorStringPkJPA.class.getSimpleName(), "named key", bookKey).getKey();
    HasStringAncestorStringPkJPA ha = new HasStringAncestorStringPkJPA(null, KeyFactory.keyToString(key));
    beginTxn();
    em.persist(ha);
    commitTxn();
    Key keyWithParent = KeyFactory.stringToKey(ha.getId());
    assertEquals(bookKey, keyWithParent.getParent());
    // now we'll issue an ancestor query directly against the datastore and see
    // if our object comes back.
    Query q = new Query(ha.getClass().getSimpleName());
    q.setAncestor(bookKey);
    Entity result = ldth.ds.prepare(q).asSingleEntity();
    assertEquals(bookKey, result.getKey().getParent());
    assertEquals("named key", result.getKey().getName());
    assertEquals("parent named key", result.getKey().getParent().getName());
  }

  public void testInsert_SetAncestorAndPk() {
    Entity bookEntity = Book.newBookEntity("parent named key", "max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Key bookKey = bookEntity.getKey();
    HasStringAncestorStringPkJPA ha = new HasStringAncestorStringPkJPA(KeyFactory.keyToString(bookKey),
        TestUtils.createKeyString(HasStringAncestorStringPkJPA.class, "named key"));
    beginTxn();
    em.persist(ha);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }

  public void testFetch() {
    Entity bookEntity = Book.newBookEntity("max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Entity hasAncestorEntity = new Entity(HasStringAncestorStringPkJPA.class.getSimpleName(), bookEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasStringAncestorStringPkJPA ha = em.find(HasStringAncestorStringPkJPA.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(bookEntity.getKey()), ha.getAncestorId());
    commitTxn();
  }

  public void testFetchWithNamedKey() {
    Entity bookEntity = Book.newBookEntity("parent named key", "max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Entity hasAncestorEntity =
        new Entity(HasStringAncestorStringPkJPA.class.getSimpleName(), "named key", bookEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasStringAncestorStringPkJPA ha = em.find(HasStringAncestorStringPkJPA.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(bookEntity.getKey()), ha.getAncestorId());
    assertEquals("named key", KeyFactory.stringToKey(ha.getId()).getName());
    assertEquals("parent named key", KeyFactory.stringToKey(ha.getId()).getParent().getName());
    commitTxn();
  }

  public void testInsertWithNullAncestor() {
    HasStringAncestorStringPkJPA ha = new HasStringAncestorStringPkJPA(null);
    beginTxn();
    em.persist(ha);
    commitTxn();
    Key keyWithParent = KeyFactory.stringToKey(ha.getId());
    assertNull(keyWithParent.getParent());
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
//    Entity reloaded = ldth.ds.get(hk1.getKey());
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
//    Entity reloaded = ldth.ds.get(KeyFactory.stringToKey(hk1.getKey()));
//    assertEquals(hk1.getAncestorKey(), reloaded.getKey().getParent());
//  }

  public void testInsertWithKeyPkAndStringAncestor_IdGen() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJPA hk1 = new HasStringAncestorKeyPkJPA();
    hk1.setAncestorKey(KeyFactory.keyToString(e.getKey()));
    beginTxn();
    em.persist(hk1);
    commitTxn();

    Entity reloaded = ldth.ds.get(hk1.getKey());
    assertEquals(hk1.getAncestorKey(), KeyFactory.keyToString(reloaded.getKey().getParent()));
  }

  public void testInsertWithKeyPkAndStringAncestor_NamedKey() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJPA hk1 = new HasStringAncestorKeyPkJPA();
    Key key = new Entity(HasStringAncestorKeyPkJPA.class.getSimpleName(), "named key", e.getKey()).getKey();
    hk1.setKey(key);
    beginTxn();
    em.persist(hk1);
    commitTxn();
    assertEquals(e.getKey(), KeyFactory.stringToKey(hk1.getAncestorKey()));

    Entity reloaded = ldth.ds.get(hk1.getKey());
    assertEquals(hk1.getAncestorKey(), KeyFactory.keyToString(reloaded.getKey().getParent()));
  }

  public void testInsertWithKeyPkAndStringAncestor_SetKeyAndAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJPA hk1 = new HasStringAncestorKeyPkJPA();
    Key key = KeyFactory.createKey(HasStringAncestorKeyPkJPA.class.getSimpleName(), "named key");
    hk1.setKey(key);
    hk1.setAncestorKey(KeyFactory.keyToString(e.getKey()));
    beginTxn();
    em.persist(hk1);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException ex) {
      // good
      rollbackTxn();
    }
  }
}
