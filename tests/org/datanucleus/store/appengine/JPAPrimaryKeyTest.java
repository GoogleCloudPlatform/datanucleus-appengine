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

import org.datanucleus.test.HasEncodedStringPkJPA;
import org.datanucleus.test.HasEncodedStringPkSeparateIdFieldJPA;
import org.datanucleus.test.HasEncodedStringPkSeparateNameFieldJPA;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasLongPkJPA;
import org.datanucleus.test.HasUnencodedStringPkJPA;

import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAPrimaryKeyTest extends JPATestCase {
  public void testLongPk() throws EntityNotFoundException {
    HasLongPkJPA pojo = new HasLongPkJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(pojo.getId());
    Entity e = ldth.ds.get(KeyFactory.createKey(HasLongPkJPA.class.getSimpleName(), pojo.getId()));

    beginTxn();
    em.find(HasLongPkJPA.class, e.getKey().getId());
    em.find(HasLongPkJPA.class, e.getKey());
    em.find(HasLongPkJPA.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testLongPk_UserProvided() {
    HasLongPkJPA pojo = new HasLongPkJPA();
    pojo.setId(33L);
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUnencodedStringPk() throws EntityNotFoundException {
    HasUnencodedStringPkJPA pojo = new HasUnencodedStringPkJPA();
    pojo.setId("a name");
    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertEquals("a name", pojo.getId());
    Entity e = ldth.ds.get(KeyFactory.createKey(HasUnencodedStringPkJPA.class.getSimpleName(), pojo.getId()));

    beginTxn();
    em.find(HasUnencodedStringPkJPA.class, e.getKey().getName());
    em.find(HasUnencodedStringPkJPA.class, e.getKey());
    em.find(HasUnencodedStringPkJPA.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testUnencodedStringPk_NullValue() {
    HasUnencodedStringPkJPA pojo = new HasUnencodedStringPkJPA();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEncodedStringPk() throws EntityNotFoundException {
    HasEncodedStringPkJPA pojo = new HasEncodedStringPkJPA();
    Key key = KeyFactory.createKey(HasEncodedStringPkJPA.class.getSimpleName(), "a name");
    pojo.setId(KeyFactory.keyToString(key));
    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertEquals(KeyFactory.keyToString(key), pojo.getId());
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));

    beginTxn();
    em.find(HasEncodedStringPkJPA.class, e.getKey().getName());
    em.find(HasEncodedStringPkJPA.class, e.getKey());
    em.find(HasEncodedStringPkJPA.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testEncodedStringPk_NullValue() throws EntityNotFoundException {
    HasEncodedStringPkJPA pojo = new HasEncodedStringPkJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    assertNotNull(pojo.getId());
    Key key = KeyFactory.stringToKey(pojo.getId());
    Entity e = ldth.ds.get(key);

    beginTxn();
    em.find(HasEncodedStringPkJPA.class, e.getKey().getId());
    em.find(HasEncodedStringPkJPA.class, e.getKey());
    em.find(HasEncodedStringPkJPA.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testEncodedStringPk_NonKeyValue() throws EntityNotFoundException {
    HasEncodedStringPkJPA pojo = new HasEncodedStringPkJPA();
    pojo.setId("yar");
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEncodedStringPk_SeparateNameField() throws EntityNotFoundException {
    HasEncodedStringPkSeparateNameFieldJPA pojo = new HasEncodedStringPkSeparateNameFieldJPA();
    pojo.setName("a name");
    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertEquals(TestUtils.createKey(pojo, "a name"), KeyFactory.stringToKey(pojo.getId()));
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));

    beginTxn();
    pojo = em.find(HasEncodedStringPkSeparateNameFieldJPA.class, e.getKey().getName());
    assertEquals("a name", pojo.getName());
    em.find(HasEncodedStringPkSeparateNameFieldJPA.class, e.getKey());
    assertEquals("a name", pojo.getName());
    em.find(HasEncodedStringPkSeparateNameFieldJPA.class, KeyFactory.keyToString(e.getKey()));
    assertEquals("a name", pojo.getName());
    commitTxn();

  }

  public void testEncodedStringPk_SeparateIdField() throws EntityNotFoundException {
    HasEncodedStringPkSeparateIdFieldJPA pojo = new HasEncodedStringPkSeparateIdFieldJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    beginTxn();
    assertNotNull(pojo.getId());
    assertNotNull(pojo.getKey());
    Entity e = ldth.ds.get(TestUtils.createKey(pojo, pojo.getId()));
    Long id = e.getKey().getId();
    pojo = em.find(HasEncodedStringPkSeparateIdFieldJPA.class, e.getKey().getId());
    assertEquals(id, pojo.getId());
    em.find(HasEncodedStringPkSeparateIdFieldJPA.class, e.getKey());
    assertEquals(id, pojo.getId());
    em.find(HasEncodedStringPkSeparateIdFieldJPA.class, KeyFactory.keyToString(e.getKey()));
    assertEquals(id, pojo.getId());
    commitTxn();

  }

  public void testUserCannotSetIdWithKeyPk() {
    HasKeyPkJPA pojo = new HasKeyPkJPA();
    pojo.setId(TestUtils.createKey(pojo, 34));
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }

  public void testCannotChangeLongPk() {
    HasLongPkJPA pojo = new HasLongPkJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();
    beginTxn();
    pojo = em.find(HasLongPkJPA.class, pojo.getId());
    pojo.setId(88L);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }
  }
}