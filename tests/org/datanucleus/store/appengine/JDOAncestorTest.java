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

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyAncestorKeyPkJDO;
import org.datanucleus.test.HasKeyAncestorStringPkJDO;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasStringAncestorKeyPkJDO;
import org.datanucleus.test.HasStringAncestorStringPkJDO;

import javax.jdo.JDOFatalUserException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOAncestorTest extends JDOTestCase {

  public void testInsert_IdGen() {
    Entity flightEntity = Flight.newFlightEntity("max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    HasStringAncestorStringPkJDO ha = new HasStringAncestorStringPkJDO(KeyFactory.keyToString(flightKey));
    makePersistentInTxn(ha);
    Key keyWithParent = KeyFactory.stringToKey(ha.getId());
    assertEquals(flightKey, keyWithParent.getParent());
    // now we'll issue an ancestor query directly against the datastore and see
    // if our object comes back.
    Query q = new Query(ha.getClass().getSimpleName());
    q.setAncestor(flightKey);
    Entity result = ldth.ds.prepare(q).asSingleEntity();
    assertEquals(flightKey, result.getKey().getParent());
  }

  public void testInsert_NamedKey() {
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    Key key = new Entity(HasStringAncestorStringPkJDO.class.getSimpleName(), "named key", flightKey).getKey();
    HasStringAncestorStringPkJDO ha = new HasStringAncestorStringPkJDO(null, KeyFactory.keyToString(key));
    makePersistentInTxn(ha);
    Key keyWithParent = KeyFactory.stringToKey(ha.getId());
    assertEquals(flightKey, keyWithParent.getParent());
    // now we'll issue an ancestor query directly against the datastore and see
    // if our object comes back.
    Query q = new Query(ha.getClass().getSimpleName());
    q.setAncestor(flightKey);
    Entity result = ldth.ds.prepare(q).asSingleEntity();
    assertEquals(flightKey, result.getKey().getParent());
    assertEquals("named key", result.getKey().getName());
    assertEquals("parent named key", result.getKey().getParent().getName());
  }

  public void testInsert_SetAncestorAndPk() {
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    HasStringAncestorStringPkJDO ha = new HasStringAncestorStringPkJDO(
        KeyFactory.keyToString(flightKey),
        KeyFactory.keyToString(KeyFactory.createKey(HasStringAncestorStringPkJDO.class.getSimpleName(), "named key")));
    beginTxn();
    try {
      pm.makePersistent(ha);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testFetch() {
    Entity flightEntity = Flight.newFlightEntity("max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasStringAncestorStringPkJDO.class.getSimpleName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasStringAncestorStringPkJDO ha = pm.getObjectById(HasStringAncestorStringPkJDO.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(flightEntity.getKey()), ha.getAncestorId());
    commitTxn();
  }

  public void testFetchWithNamedKey() {
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity =
        new Entity(HasStringAncestorStringPkJDO.class.getSimpleName(), "named key", flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasStringAncestorStringPkJDO ha = pm.getObjectById(HasStringAncestorStringPkJDO.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(flightEntity.getKey()), ha.getAncestorId());
    assertEquals("named key", KeyFactory.stringToKey(ha.getId()).getName());
    assertEquals("parent named key", KeyFactory.stringToKey(ha.getId()).getParent().getName());
    commitTxn();
  }

  public void testInsertWithNullAncestor() {
    HasStringAncestorStringPkJDO ha = new HasStringAncestorStringPkJDO(null);
    makePersistentInTxn(ha);
    Key keyWithParent = KeyFactory.stringToKey(ha.getId());
    assertNull(keyWithParent.getParent());
  }

  public void testKeyPKKeyAncestor_NamedKey() throws EntityNotFoundException {
    HasKeyAncestorKeyPkJDO pojo = new HasKeyAncestorKeyPkJDO();
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    Key pojoKey = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), "child named key", flightKey).getKey();
    pojo.setKey(pojoKey);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    ldth.ds.get(pojoKey);
    beginTxn();
    pojo = pm.getObjectById(HasKeyAncestorKeyPkJDO.class, pojoKey);
    assertEquals(pojo.getAncestorKey(), pojoKey.getParent());
    commitTxn();
  }

  public void testKeyPKKeyAncestor_NamedKeyWrongKind() throws EntityNotFoundException {
    HasKeyAncestorKeyPkJDO pojo = new HasKeyAncestorKeyPkJDO();
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    Key pojoKey = new Entity("blarg", "child named key", flightKey).getKey();
    pojo.setKey(pojoKey);
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testKeyPKKeyAncestor_IdGen() throws EntityNotFoundException {
    HasKeyAncestorKeyPkJDO pojo = new HasKeyAncestorKeyPkJDO();
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    pojo.setAncestorKey(flightKey);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    ldth.ds.get(pojo.getKey());
    beginTxn();
    pojo = pm.getObjectById(HasKeyAncestorKeyPkJDO.class, pojo.getKey());
    assertEquals(pojo.getAncestorKey(), pojo.getKey().getParent());
    commitTxn();
  }

  public void testKeyPKKeyAncestor_SetAncestorAndKey() throws EntityNotFoundException {
    HasKeyAncestorKeyPkJDO pojo = new HasKeyAncestorKeyPkJDO();
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    pojo.setAncestorKey(flightKey);
    Key pojoKey = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), "child named key", flightKey).getKey();
    pojo.setKey(pojoKey);
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testInsertWithKeyPkAndAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyPkJDO hk1 = new HasKeyPkJDO();
    hk1.setAncestorKey(e.getKey());
    beginTxn();
    pm.makePersistent(hk1);
    Key key = hk1.getKey();
    Key ancestorKey = hk1.getAncestorKey();
    assertNotNull(key);
    commitTxn();
    Entity reloaded = ldth.ds.get(hk1.getKey());
    assertEquals(ancestorKey, reloaded.getKey().getParent());
  }

  public void testInsertWithKeyPkAndStringAncestor_IdGen() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJDO hk1 = new HasStringAncestorKeyPkJDO();
    hk1.setAncestorKey(KeyFactory.keyToString(e.getKey()));
    beginTxn();
    pm.makePersistent(hk1);
    Key key = hk1.getKey();
    String ancestorKey = hk1.getAncestorKey();
    commitTxn();
    Entity reloaded = ldth.ds.get(key);
    assertEquals(ancestorKey, KeyFactory.keyToString(reloaded.getKey().getParent()));
  }

  public void testInsertWithKeyPkAndStringAncestor_NamedKey() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJDO hk1 = new HasStringAncestorKeyPkJDO();
    Key key = new Entity(HasStringAncestorKeyPkJDO.class.getSimpleName(), "named key", e.getKey()).getKey();
    hk1.setKey(key);
    beginTxn();
    pm.makePersistent(hk1);
    assertEquals(e.getKey(), KeyFactory.stringToKey(hk1.getAncestorKey()));
    String ancestorKey = hk1.getAncestorKey();
    commitTxn();
    Entity reloaded = ldth.ds.get(key);
    assertEquals(ancestorKey, KeyFactory.keyToString(reloaded.getKey().getParent()));
  }

  public void testInsertWithKeyPkAndStringAncestor_SetKeyAndAncestor() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasStringAncestorKeyPkJDO hk1 = new HasStringAncestorKeyPkJDO();
    Key key = KeyFactory.createKey(HasStringAncestorKeyPkJDO.class.getSimpleName(), "named key");
    hk1.setKey(key);
    hk1.setAncestorKey(KeyFactory.keyToString(e.getKey()));
    beginTxn();
    try {
      pm.makePersistent(hk1);
      fail("expected exception");
    } catch (JDOFatalUserException ex) {
      // good
      rollbackTxn();
    }
  }

  public void testInsertWithStringPkAndKeyAncestor_IdGen() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyAncestorStringPkJDO hk1 = new HasKeyAncestorStringPkJDO();
    hk1.setAncestorKey(e.getKey());
    beginTxn();
    pm.makePersistent(hk1);
    String key = hk1.getKey();
    Key ancestorKey = hk1.getAncestorKey();
    commitTxn();
    Entity reloaded = ldth.ds.get(KeyFactory.stringToKey(key));
    assertEquals(ancestorKey, reloaded.getKey().getParent());
  }

  public void testInsertWithStringPkAndKeyAncestor_NamedKey() throws EntityNotFoundException {
    Entity e = new Entity("yam");
    ldth.ds.put(e);
    HasKeyAncestorStringPkJDO hk1 = new HasKeyAncestorStringPkJDO();
    Key keyToSet =
        new Entity(HasKeyAncestorStringPkJDO.class.getSimpleName(), "yar", e.getKey()).getKey();
    hk1.setKey(KeyFactory.keyToString(keyToSet));
    beginTxn();
    pm.makePersistent(hk1);
    String key = hk1.getKey();
    assertEquals(e.getKey(), hk1.getAncestorKey());
    commitTxn();
    Entity reloaded = ldth.ds.get(KeyFactory.stringToKey(key));
    assertEquals(e.getKey(), reloaded.getKey().getParent());
  }

  public void testInsertWithStringPkAndKeyAncestor_SetAncestorAndPk() throws EntityNotFoundException {
    Entity parentEntity = new Entity("yam");
    ldth.ds.put(parentEntity);
    HasKeyAncestorStringPkJDO hk1 = new HasKeyAncestorStringPkJDO();
    Key keyToSet =
        new Entity(HasKeyAncestorStringPkJDO.class.getSimpleName(), "yar", parentEntity.getKey()).getKey();
    hk1.setKey(KeyFactory.keyToString(keyToSet));
    hk1.setAncestorKey(keyToSet);
    beginTxn();
    try {
      pm.makePersistent(hk1);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

}
