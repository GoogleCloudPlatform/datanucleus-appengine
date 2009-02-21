// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasAncestorJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAAncestorTest extends JPATestCase {

  public void testInsert() {
    Entity bookEntity = Book.newBookEntity("max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Key bookKey = bookEntity.getKey();
    HasAncestorJPA ha = new HasAncestorJPA(KeyFactory.keyToString(bookKey));
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

  public void testInsertWithNamedKey() {
    Entity bookEntity = Book.newBookEntity("parent named key", "max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Key bookKey = bookEntity.getKey();
    HasAncestorJPA ha = new HasAncestorJPA(KeyFactory.keyToString(bookKey),
        TestUtils.createKeyString(HasAncestorJPA.class, "named key"));
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

  public void testFetch() {
    Entity bookEntity = Book.newBookEntity("max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJPA.class.getSimpleName(), bookEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasAncestorJPA ha = em.find(HasAncestorJPA.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(bookEntity.getKey()), ha.getAncestorId());
    commitTxn();
  }

  public void testFetchWithNamedKey() {
    Entity bookEntity = Book.newBookEntity("parent named key", "max", "123456", "manifesto");
    ldth.ds.put(bookEntity);
    Entity hasAncestorEntity =
        new Entity(HasAncestorJPA.class.getSimpleName(), "named key", bookEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasAncestorJPA ha = em.find(HasAncestorJPA.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(bookEntity.getKey()), ha.getAncestorId());
    assertEquals("named key", KeyFactory.stringToKey(ha.getId()).getName());
    assertEquals("parent named key", KeyFactory.stringToKey(ha.getId()).getParent().getName());
    commitTxn();
  }

  public void testInsertWithNullAncestor() {
    HasAncestorJPA ha = new HasAncestorJPA(null);
    beginTxn();
    em.persist(ha);
    commitTxn();
    Key keyWithParent = KeyFactory.stringToKey(ha.getId());
    assertNull(keyWithParent.getParent());
  }
}
