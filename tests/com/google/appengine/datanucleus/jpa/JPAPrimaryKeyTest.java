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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.TestUtils;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.HasEncodedStringPkJPA;
import com.google.appengine.datanucleus.test.HasEncodedStringPkSeparateIdFieldJPA;
import com.google.appengine.datanucleus.test.HasEncodedStringPkSeparateNameFieldJPA;
import com.google.appengine.datanucleus.test.HasKeyPkJPA;
import com.google.appengine.datanucleus.test.HasLongPkJPA;
import com.google.appengine.datanucleus.test.HasUnencodedStringPkJPA;


import java.util.List;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

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
    Entity e = ds.get(KeyFactory.createKey(HasLongPkJPA.class.getSimpleName(), pojo.getId()));

    beginTxn();
    em.find(HasLongPkJPA.class, e.getKey().getId());
    em.find(HasLongPkJPA.class, e.getKey());
    em.find(HasLongPkJPA.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testLongPk_UserProvided() throws EntityNotFoundException {
    HasLongPkJPA pojo = new HasLongPkJPA();
    pojo.setId(33L);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    // the fact that this doesn't throw an exception is the test
    ds.get(KeyFactory.createKey(HasLongPkJPA.class.getSimpleName(), 33));
  }

  public void testUnencodedStringPk() throws EntityNotFoundException {
    HasUnencodedStringPkJPA pojo = new HasUnencodedStringPkJPA();
    pojo.setId("a name");
    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertEquals("a name", pojo.getId());
    Entity e = ds.get(KeyFactory.createKey(HasUnencodedStringPkJPA.class.getSimpleName(), pojo.getId()));

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
    Entity e = ds.get(KeyFactory.stringToKey(pojo.getId()));

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
    Entity e = ds.get(key);

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
    Entity e = ds.get(KeyFactory.stringToKey(pojo.getId()));

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
    pojo.setId(34L);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    beginTxn();
    assertNotNull(pojo.getId());
    assertNotNull(pojo.getKey());
    Entity e = ds.get(TestUtils.createKey(pojo, pojo.getId()));
    long id = e.getKey().getId();
    assertEquals(34L, id);
    pojo = em.find(HasEncodedStringPkSeparateIdFieldJPA.class, e.getKey().getId());
    assertEquals(id, pojo.getId().longValue());
    em.find(HasEncodedStringPkSeparateIdFieldJPA.class, e.getKey());
    assertEquals(id, pojo.getId().longValue());
    em.find(HasEncodedStringPkSeparateIdFieldJPA.class, KeyFactory.keyToString(e.getKey()));
    assertEquals(id, pojo.getId().longValue());
    commitTxn();

  }

  public void testKeyPk_UserProvidedId() throws EntityNotFoundException {
    HasKeyPkJPA pojo = new HasKeyPkJPA();
    pojo.setId(TestUtils.createKey(pojo, 34));
    beginTxn();
    em.persist(pojo);
    commitTxn();
    // the fact that this doesn't throw an exception is the test
    ds.get(KeyFactory.createKey(HasKeyPkJPA.class.getSimpleName(), 34));
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
  
  public void testUnencodedStringPk_BatchGet() throws EntityNotFoundException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    HasUnencodedStringPkJPA pojo = new HasUnencodedStringPkJPA();
    beginTxn();
    pojo.setId("yar1");
    em.persist(pojo);
    commitTxn();

    HasUnencodedStringPkJPA pojo2 = new HasUnencodedStringPkJPA();
    beginTxn();
    pojo2.setId("yar2");
    em.persist(pojo2);
    commitTxn();

    assertNotNull(pojo.getId());
    assertNotNull(pojo2.getId());

    beginTxn();
    Query q = em.createQuery("select from " + HasUnencodedStringPkJPA.class.getName() + " b where id = :ids");
    q.setParameter("ids", Utils.newArrayList(pojo.getId(), pojo2.getId()));
    List<HasUnencodedStringPkJPA> pojos = (List<HasUnencodedStringPkJPA>) q.getResultList();
    assertEquals(2, pojos.size());
    // we should preserve order but right now we don't
    Set<String> pks = Utils.newHashSet(pojos.get(0).getId(), pojos.get(1).getId());
    assertEquals(pks, Utils.newHashSet("yar1", "yar2"));
    commitTxn();
  }

  public void testEncodedStringPk_BatchGet() throws EntityNotFoundException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    HasEncodedStringPkJPA pojo = new HasEncodedStringPkJPA();
    beginTxn();
    String key1 = new KeyFactory.Builder("parent", 44)
        .addChild(HasEncodedStringPkJPA.class.getSimpleName(), "yar1").getString();
    pojo.setId(key1);
    em.persist(pojo);
    commitTxn();

    HasEncodedStringPkJPA pojo2 = new HasEncodedStringPkJPA();
    beginTxn();
    String key2 = new KeyFactory.Builder("parent", 44)
        .addChild(HasEncodedStringPkJPA.class.getSimpleName(), "yar2").getString();        
    pojo2.setId(key2);
    em.persist(pojo2);
    commitTxn();

    assertNotNull(pojo.getId());
    assertNotNull(pojo2.getId());

    beginTxn();
    Query q = em.createQuery("select from " + HasEncodedStringPkJPA.class.getName() + " b where id = :ids");
    q.setParameter("ids", Utils.newArrayList(pojo.getId(), pojo2.getId()));
    List<HasEncodedStringPkJPA> pojos = (List<HasEncodedStringPkJPA>) q.getResultList();
    assertEquals(2, pojos.size());
    // we should preserve order but right now we don't
    Set<String> pks = Utils.newHashSet(pojos.get(0).getId(), pojos.get(1).getId());
    assertEquals(pks, Utils.newHashSet(key1, key2));
    commitTxn();
  }

  public void testUnencodedLongPk_BatchGet() throws EntityNotFoundException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    HasLongPkJPA pojo = new HasLongPkJPA();
    beginTxn();
    pojo.setId(1L);
    em.persist(pojo);
    commitTxn();

    HasLongPkJPA pojo2 = new HasLongPkJPA();
    beginTxn();
    pojo2.setId(2L);
    em.persist(pojo2);
    commitTxn();

    assertNotNull(pojo.getId());
    assertNotNull(pojo2.getId());

    beginTxn();
    Query q = em.createQuery("select from " + HasLongPkJPA.class.getName() + " b where id = :ids");
    q.setParameter("ids", Utils.newArrayList(pojo.getId(), pojo2.getId()));
    List<HasLongPkJPA> pojos = (List<HasLongPkJPA>) q.getResultList();
    assertEquals(2, pojos.size());
    // we should preserve order but right now we don't
    Set<Long> pks = Utils.newHashSet(pojos.get(0).getId(), pojos.get(1).getId());
    assertEquals(pks, Utils.newHashSet(1L, 2L));
    commitTxn();
  }
  
}