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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.TestUtils;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.jdo.HasEncodedStringPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasEncodedStringPkSeparateIdFieldJDO;
import com.google.appengine.datanucleus.test.jdo.HasEncodedStringPkSeparateNameFieldJDO;
import com.google.appengine.datanucleus.test.jdo.HasKeyPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasLongPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasLongPrimitivePkJDO;
import com.google.appengine.datanucleus.test.jdo.HasStringUUIDHexPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasStringUUIDPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasUnencodedStringPkJDO;

import java.util.List;
import java.util.Set;

import javax.jdo.JDOFatalUserException;
import javax.jdo.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOPrimaryKeyTest extends JDOTestCase {

  public void testLongPrimitivePk() throws EntityNotFoundException {
    HasLongPrimitivePkJDO pojo = new HasLongPrimitivePkJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(pojo.getId());
    Entity e = ds.get(KeyFactory.createKey(HasLongPrimitivePkJDO.class.getSimpleName(), pojo.getId()));

    beginTxn();
    pm.getObjectById(HasLongPrimitivePkJDO.class, e.getKey().getId());
    pm.getObjectById(HasLongPrimitivePkJDO.class, e.getKey());
    pm.getObjectById(HasLongPrimitivePkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testStringUUIDPk() throws EntityNotFoundException {
    HasStringUUIDPkJDO pojo = new HasStringUUIDPkJDO("First");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(pojo.getId());
    Entity e = ds.get(KeyFactory.createKey(HasStringUUIDPkJDO.class.getSimpleName(), pojo.getId()));

    beginTxn();
    pm.getObjectById(HasStringUUIDPkJDO.class, e.getKey());
    pm.getObjectById(HasStringUUIDPkJDO.class, e.getKey().getName());
    pm.getObjectById(HasStringUUIDPkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testStringUUIDHexPk() throws EntityNotFoundException {
    HasStringUUIDHexPkJDO pojo = new HasStringUUIDHexPkJDO("First");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(pojo.getId());
    Entity e = ds.get(KeyFactory.createKey(HasStringUUIDHexPkJDO.class.getSimpleName(), pojo.getId()));

    beginTxn();
    pm.getObjectById(HasStringUUIDHexPkJDO.class, e.getKey());
    pm.getObjectById(HasStringUUIDHexPkJDO.class, e.getKey().getName());
    pm.getObjectById(HasStringUUIDHexPkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testLongPk() throws EntityNotFoundException {
    HasLongPkJDO pojo = new HasLongPkJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(pojo.getId());
    Entity e = ds.get(KeyFactory.createKey(HasLongPkJDO.class.getSimpleName(), pojo.getId()));

    beginTxn();
    pm.getObjectById(HasLongPkJDO.class, e.getKey().getId());
    pm.getObjectById(HasLongPkJDO.class, e.getKey());
    pm.getObjectById(HasLongPkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testLongPk_UserProvided() throws EntityNotFoundException {
    HasLongPkJDO pojo = new HasLongPkJDO();
    pojo.setId(33L);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    // the fact that this doesn't throw an exception is the test
    ds.get(KeyFactory.createKey(HasLongPkJDO.class.getSimpleName(), 33));
  }

  public void testUnencodedStringPk() throws EntityNotFoundException {
    HasUnencodedStringPkJDO pojo = new HasUnencodedStringPkJDO();
    pojo.setId("a name");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertEquals("a name", pojo.getId());
    Entity e = ds.get(KeyFactory.createKey(HasUnencodedStringPkJDO.class.getSimpleName(), pojo.getId()));

    beginTxn();
    pm.getObjectById(HasUnencodedStringPkJDO.class, e.getKey().getName());
    pm.getObjectById(HasUnencodedStringPkJDO.class, e.getKey());
    pm.getObjectById(HasUnencodedStringPkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testUnencodedStringPk_NullValue() {
    HasUnencodedStringPkJDO pojo = new HasUnencodedStringPkJDO();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEncodedStringPk() throws EntityNotFoundException {
    HasEncodedStringPkJDO pojo = new HasEncodedStringPkJDO();
    Key key = KeyFactory.createKey(HasEncodedStringPkJDO.class.getSimpleName(), "a name");
    pojo.setId(KeyFactory.keyToString(key));
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertEquals(KeyFactory.keyToString(key), pojo.getId());
    Entity e = ds.get(KeyFactory.stringToKey(pojo.getId()));

    beginTxn();
    pm.getObjectById(HasEncodedStringPkJDO.class, e.getKey().getName());
    pm.getObjectById(HasEncodedStringPkJDO.class, e.getKey());
    pm.getObjectById(HasEncodedStringPkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testEncodedStringPk_NullValue() throws EntityNotFoundException {
    HasEncodedStringPkJDO pojo = new HasEncodedStringPkJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    assertNotNull(pojo.getId());
    Key key = KeyFactory.stringToKey(pojo.getId());
    Entity e = ds.get(key);

    beginTxn();
    pm.getObjectById(HasEncodedStringPkJDO.class, e.getKey().getId());
    pm.getObjectById(HasEncodedStringPkJDO.class, e.getKey());
    pm.getObjectById(HasEncodedStringPkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testEncodedStringPk_NonKeyValue() throws EntityNotFoundException {
    HasEncodedStringPkJDO pojo = new HasEncodedStringPkJDO();
    pojo.setId("yar");
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEncodedStringPk_SeparateNameField() throws EntityNotFoundException {
    HasEncodedStringPkSeparateNameFieldJDO pojo = new HasEncodedStringPkSeparateNameFieldJDO();
    pojo.setName("a name");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertEquals(TestUtils.createKey(pojo, "a name"), KeyFactory.stringToKey(pojo.getId()));
    Entity e = ds.get(KeyFactory.stringToKey(pojo.getId()));

    beginTxn();
    pojo = pm.getObjectById(HasEncodedStringPkSeparateNameFieldJDO.class, e.getKey().getName());
    assertEquals("a name", pojo.getName());
    pm.getObjectById(HasEncodedStringPkSeparateNameFieldJDO.class, e.getKey());
    assertEquals("a name", pojo.getName());
    pm.getObjectById(HasEncodedStringPkSeparateNameFieldJDO.class, KeyFactory.keyToString(e.getKey()));
    assertEquals("a name", pojo.getName());
    commitTxn();

  }

  public void testEncodedStringPk_SeparateIdField() throws EntityNotFoundException {
    HasEncodedStringPkSeparateIdFieldJDO pojo = new HasEncodedStringPkSeparateIdFieldJDO();
    pojo.setId(34L);
    beginTxn();
    pm.makePersistent(pojo);
    assertNotNull(pojo.getId());
    commitTxn();
    beginTxn();
    assertNotNull(pojo.getId());
    assertNotNull(pojo.getKey());
    Entity e = ds.get(TestUtils.createKey(pojo, pojo.getId()));
    long id = e.getKey().getId();
    assertEquals(34L, id);
    pojo = pm.getObjectById(HasEncodedStringPkSeparateIdFieldJDO.class, e.getKey().getId());
    assertEquals(id, pojo.getId().longValue());
    pm.getObjectById(HasEncodedStringPkSeparateIdFieldJDO.class, e.getKey());
    assertEquals(id, pojo.getId().longValue());
    pm.getObjectById(HasEncodedStringPkSeparateIdFieldJDO.class, KeyFactory.keyToString(e.getKey()));
    assertEquals(id, pojo.getId().longValue());
    commitTxn();

  }

  public void testKeyPk_UserProvidedId() throws EntityNotFoundException {
    HasKeyPkJDO pojo = new HasKeyPkJDO();
    pojo.setKey(TestUtils.createKey(pojo, 34));
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    // the fact that this doesn't throw an exception is the test
    ds.get(KeyFactory.createKey(HasKeyPkJDO.class.getSimpleName(), 34));
  }

  public void testCannotChangeLongPk() {
    HasLongPkJDO pojo = new HasLongPkJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    pojo = pm.getObjectById(HasLongPkJDO.class, pojo.getId());
    pojo.setId(88L);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithLongPkMappedToPojoWithUnencodedStringPk() {
    Entity entity = new Entity(HasUnencodedStringPkJDO.class.getSimpleName());
    Key key = ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasUnencodedStringPkJDO.class, key);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithStringPkMappedToPojoWithLongPk() {
    Entity entity = new Entity(HasLongPkJDO.class.getSimpleName(), "yar");
    Key key = ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasLongPkJDO.class, key);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithParentMappedToPojoWithUnencodedStringPk() {
    Key parent = KeyFactory.createKey("parent kind", 88);
    Entity entity = new Entity(HasUnencodedStringPkJDO.class.getSimpleName(), "yar", parent);
    Key key = ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasUnencodedStringPkJDO.class, key);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithParentMappedToPojoWithLongPk() {
    Key parent = KeyFactory.createKey("parent kind", 88);
    Entity entity = new Entity(HasLongPkJDO.class.getSimpleName(), parent);
    Key key = ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasLongPkJDO.class, key);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUnencodedStringPk_BatchGet() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    HasUnencodedStringPkJDO pojo = new HasUnencodedStringPkJDO();
    beginTxn();
    pojo.setId("yar1");
    pm.makePersistent(pojo);
    commitTxn();

    HasUnencodedStringPkJDO pojo2 = new HasUnencodedStringPkJDO();
    beginTxn();
    pojo2.setId("yar2");
    pm.makePersistent(pojo2);
    commitTxn();

    assertNotNull(pojo.getId());
    assertNotNull(pojo2.getId());

    beginTxn();
    Query q = pm.newQuery("select from " + HasUnencodedStringPkJDO.class.getName() + " where id == :ids");
    List<HasUnencodedStringPkJDO> pojos =
        (List<HasUnencodedStringPkJDO>) q.execute(Utils.newArrayList(pojo.getId(), pojo2.getId()));
    assertEquals(2, pojos.size());
    // we should preserve order but right now we don't
    Set<String> pks = Utils.newHashSet(pojos.get(0).getId(), pojos.get(1).getId());
    assertEquals(pks, Utils.newHashSet("yar1", "yar2"));
    commitTxn();
  }

  public void testEncodedStringPk_BatchGet() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    HasEncodedStringPkJDO pojo = new HasEncodedStringPkJDO();
    beginTxn();
    String key1 = new KeyFactory.Builder("parent", 44)
        .addChild(HasEncodedStringPkJDO.class.getSimpleName(), "yar1").getString();
    pojo.setId(key1);
    pm.makePersistent(pojo);
    commitTxn();

    HasEncodedStringPkJDO pojo2 = new HasEncodedStringPkJDO();
    beginTxn();
    String key2 = new KeyFactory.Builder("parent", 44)
        .addChild(HasEncodedStringPkJDO.class.getSimpleName(), "yar2").getString();
    pojo2.setId(key2);
    pm.makePersistent(pojo2);
    commitTxn();

    assertNotNull(pojo.getId());
    assertNotNull(pojo2.getId());

    beginTxn();
    Query q = pm.newQuery("select from " + HasEncodedStringPkJDO.class.getName() + " where id == :ids");
    List<HasEncodedStringPkJDO> pojos =
        (List<HasEncodedStringPkJDO>) q.execute(Utils.newArrayList(pojo.getId(), pojo2.getId()));
    assertEquals(2, pojos.size());
    // we should preserve order but right now we don't
    Set<String> pks = Utils.newHashSet(pojos.get(0).getId(), pojos.get(1).getId());
    assertEquals(pks, Utils.newHashSet(key1, key2));
    commitTxn();
  }

  public void testUnencodedLongPk_BatchGet() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    HasLongPkJDO pojo = new HasLongPkJDO();
    beginTxn();
    pojo.setId(1L);
    pm.makePersistent(pojo);
    commitTxn();

    HasLongPkJDO pojo2 = new HasLongPkJDO();
    beginTxn();
    pojo2.setId(2L);
    pm.makePersistent(pojo2);
    commitTxn();

    assertNotNull(pojo.getId());
    assertNotNull(pojo2.getId());

    beginTxn();
    Query q = pm.newQuery("select from " + HasLongPkJDO.class.getName() + " where id == :ids");
    List<HasLongPkJDO> pojos =
        (List<HasLongPkJDO>) q.execute(Utils.newArrayList(pojo.getId(), pojo2.getId()));
    assertEquals(2, pojos.size());
    // we should preserve order but right now we don't
    Set<Long> pks = Utils.newHashSet(pojos.get(0).getId(), pojos.get(1).getId());
    assertEquals(pks, Utils.newHashSet(1L, 2L));
    commitTxn();
  }
}
