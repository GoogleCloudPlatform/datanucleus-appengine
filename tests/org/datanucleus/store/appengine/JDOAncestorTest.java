// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasAncestorJDO;
import org.datanucleus.test.HasKeyAncestorKeyPkJDO;

import javax.jdo.JDOUserException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOAncestorTest extends JDOTestCase {

  public void testInsert() {
    Entity flightEntity = Flight.newFlightEntity("max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    HasAncestorJDO ha = new HasAncestorJDO(KeyFactory.keyToString(flightKey));
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

  public void testInsertWithNamedKey() {
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    HasAncestorJDO ha = new HasAncestorJDO(
        KeyFactory.keyToString(flightKey),
        KeyFactory.keyToString(KeyFactory.createKey(HasAncestorJDO.class.getSimpleName(), "named key")));
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

  public void testFetch() {
    Entity flightEntity = Flight.newFlightEntity("max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJDO.class.getSimpleName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasAncestorJDO ha = pm.getObjectById(HasAncestorJDO.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(flightEntity.getKey()), ha.getAncestorId());
    commitTxn();
  }

  public void testFetchWithNamedKey() {
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity =
        new Entity(HasAncestorJDO.class.getSimpleName(), "named key", flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    beginTxn();
    HasAncestorJDO ha = pm.getObjectById(HasAncestorJDO.class, KeyFactory.keyToString(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.keyToString(flightEntity.getKey()), ha.getAncestorId());
    assertEquals("named key", KeyFactory.stringToKey(ha.getId()).getName());
    assertEquals("parent named key", KeyFactory.stringToKey(ha.getId()).getParent().getName());
    commitTxn();
  }

  public void testInsertWithNullAncestor() {
    HasAncestorJDO ha = new HasAncestorJDO(null);
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
    } catch (JDOUserException e) {
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
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }
}
