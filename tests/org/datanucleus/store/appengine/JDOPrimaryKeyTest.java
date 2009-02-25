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

import org.datanucleus.test.HasEncodedStringPkJDO;
import org.datanucleus.test.HasEncodedStringPkSeparateIdFieldJDO;
import org.datanucleus.test.HasEncodedStringPkSeparateNameFieldJDO;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasLongPkJDO;
import org.datanucleus.test.HasUnencodedStringPkJDO;

import javax.jdo.JDOUserException;
import javax.jdo.JDOFatalUserException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOPrimaryKeyTest extends JDOTestCase {
  public void testLongPk() throws EntityNotFoundException {
    HasLongPkJDO pojo = new HasLongPkJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(pojo.getId());
    Entity e = ldth.ds.get(KeyFactory.createKey(HasLongPkJDO.class.getSimpleName(), pojo.getId()));

    beginTxn();
    pm.getObjectById(HasLongPkJDO.class, e.getKey().getId());
    pm.getObjectById(HasLongPkJDO.class, e.getKey());
    pm.getObjectById(HasLongPkJDO.class, KeyFactory.keyToString(e.getKey()));
    commitTxn();
  }

  public void testLongPk_UserProvided() {
    HasLongPkJDO pojo = new HasLongPkJDO();
    pojo.setId(33L);
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUnencodedStringPk() throws EntityNotFoundException {
    HasUnencodedStringPkJDO pojo = new HasUnencodedStringPkJDO();
    pojo.setId("a name");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertEquals("a name", pojo.getId());
    Entity e = ldth.ds.get(KeyFactory.createKey(HasUnencodedStringPkJDO.class.getSimpleName(), pojo.getId()));

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
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));

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
    Entity e = ldth.ds.get(key);

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
    } catch (JDOUserException e) {
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
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));

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
    beginTxn();
    pm.makePersistent(pojo);
    assertNotNull(pojo.getId());
    commitTxn();
    beginTxn();
    assertNotNull(pojo.getId());
    assertNotNull(pojo.getKey());
    Entity e = ldth.ds.get(TestUtils.createKey(pojo, pojo.getId()));
    Long id = e.getKey().getId();
    pojo = pm.getObjectById(HasEncodedStringPkSeparateIdFieldJDO.class, e.getKey().getId());
    assertEquals(id, pojo.getId());
    pm.getObjectById(HasEncodedStringPkSeparateIdFieldJDO.class, e.getKey());
    assertEquals(id, pojo.getId());
    pm.getObjectById(HasEncodedStringPkSeparateIdFieldJDO.class, KeyFactory.keyToString(e.getKey()));
    assertEquals(id, pojo.getId());
    commitTxn();

  }

  public void testUserCannotSetIdWithKeyPk() {
    HasKeyPkJDO pojo = new HasKeyPkJDO();
    pojo.setKey(TestUtils.createKey(pojo, 34));
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      System.out.println(e);
      rollbackTxn();
    }
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
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithLongPkMappedToPojoWithUnencodedStringPk() {
    Entity entity = new Entity(HasUnencodedStringPkJDO.class.getSimpleName());
    Key key = ldth.ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasUnencodedStringPkJDO.class, key);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithStringPkMappedToPojoWithLongPk() {
    Entity entity = new Entity(HasLongPkJDO.class.getSimpleName(), "yar");
    Key key = ldth.ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasLongPkJDO.class, key);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithParentMappedToPojoWithUnencodedStringPk() {
    Key parent = KeyFactory.createKey("parent kind", 88);
    Entity entity = new Entity(HasUnencodedStringPkJDO.class.getSimpleName(), "yar", parent);
    Key key = ldth.ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasUnencodedStringPkJDO.class, key);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testEntityWithParentMappedToPojoWithLongPk() {
    Key parent = KeyFactory.createKey("parent kind", 88);
    Entity entity = new Entity(HasLongPkJDO.class.getSimpleName(), parent);
    Key key = ldth.ds.put(entity);
    beginTxn();
    try {
      pm.getObjectById(HasLongPkJDO.class, key);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }
}
