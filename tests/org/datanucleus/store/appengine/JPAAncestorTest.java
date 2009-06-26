/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyAncestorStringPkJPA;
import org.datanucleus.test.HasKeyPkJPA;
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

  public void testInsertWithKeyPkAndAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyPkJPA hk1 = new HasKeyPkJPA();
    hk1.setAncestorId(e.getKey());
    beginTxn();
    em.persist(hk1);
    commitTxn();

    Entity reloaded = ldth.ds.get(hk1.getId());
    assertEquals(hk1.getAncestorId(), reloaded.getKey().getParent());
  }

  public void testInsertWithStringPkAndKeyAncestor_IdGen() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyAncestorStringPkJPA hk1 = new HasKeyAncestorStringPkJPA();
    hk1.setAncestorKey(e.getKey());
    beginTxn();
    em.persist(hk1);
    commitTxn();

    Entity reloaded = ldth.ds.get(KeyFactory.stringToKey(hk1.getKey()));
    assertEquals(hk1.getAncestorKey(), reloaded.getKey().getParent());
  }

  public void testInsertWithStringPkAndKeyAncestor_NamedKey() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyAncestorStringPkJPA hk1 = new HasKeyAncestorStringPkJPA();
    Key keyToSet =
        new Entity(HasKeyAncestorStringPkJPA.class.getSimpleName(), "yar", e.getKey()).getKey();
    hk1.setKey(KeyFactory.keyToString(keyToSet));
    beginTxn();
    em.persist(hk1);
    commitTxn();
    String key = hk1.getKey();
    assertEquals(e.getKey(), hk1.getAncestorKey());
    Entity reloaded = ldth.ds.get(KeyFactory.stringToKey(key));
    assertEquals(e.getKey(), reloaded.getKey().getParent());
  }

  public void testInsertWithStringPkAndKeyAncestor_SetAncestorAndPk() throws EntityNotFoundException {
    Entity parentEntity = new Entity("yam");
    ldth.ds.put(parentEntity);
    HasKeyAncestorStringPkJPA hk1 = new HasKeyAncestorStringPkJPA();
    Key keyToSet =
        new Entity(HasKeyAncestorStringPkJPA.class.getSimpleName(), "yar", parentEntity.getKey()).getKey();
    hk1.setKey(KeyFactory.keyToString(keyToSet));
    hk1.setAncestorKey(keyToSet);
    beginTxn();
    em.persist(hk1);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

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
