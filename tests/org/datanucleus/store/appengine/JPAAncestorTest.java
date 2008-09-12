// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Query;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasAncestorJPA;

import javax.persistence.EntityTransaction;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAAncestorTest extends JPATestCase {

  public void testInsert() {
    Entity flightEntity = Flight.newFlightEntity("max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    HasAncestorJPA ha = new HasAncestorJPA(KeyFactory.encodeKey(flightKey));
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(ha);
    txn.commit();
    Key keyWithParent = KeyFactory.decodeKey(ha.getId());
    assertEquals(flightKey, keyWithParent.getParent());
    // now we'll issue an ancestor query directly against the datastore and see
    // if our object comes back.
    Query q = new Query(ha.getClass().getName());
    q.setAncestor(flightKey);
    Entity result = ldth.ds.prepare(q).asSingleEntity();
    assertEquals(flightKey, result.getKey().getParent());
  }

  public void testInsertWithNamedKey() {
    Entity flightEntity = Flight.newFlightEntity("parent named key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Key flightKey = flightEntity.getKey();
    HasAncestorJPA ha = new HasAncestorJPA(KeyFactory.encodeKey(flightKey), "named key");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(ha);
    txn.commit();
    Key keyWithParent = KeyFactory.decodeKey(ha.getId());
    assertEquals(flightKey, keyWithParent.getParent());
    // now we'll issue an ancestor query directly against the datastore and see
    // if our object comes back.
    Query q = new Query(ha.getClass().getName());
    q.setAncestor(flightKey);
    Entity result = ldth.ds.prepare(q).asSingleEntity();
    assertEquals(flightKey, result.getKey().getParent());
    assertEquals("named key", result.getKey().getName());
    assertEquals("parent named key", result.getKey().getParent().getName());
  }

  public void testFetch() {
    Entity flightEntity = Flight.newFlightEntity("max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity = new Entity(HasAncestorJPA.class.getName(), flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    HasAncestorJPA ha = em.find(HasAncestorJPA.class, KeyFactory.encodeKey(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.encodeKey(flightEntity.getKey()), ha.getAncestorId());
  }

  public void testFetchWithNamedKey() {
    Entity flightEntity = Flight.newFlightEntity("named parent key", "max", "bos", "mia", 3, 4);
    ldth.ds.put(flightEntity);
    Entity hasAncestorEntity =
        new Entity(HasAncestorJPA.class.getName(), "named key", flightEntity.getKey());
    ldth.ds.put(hasAncestorEntity);

    HasAncestorJPA ha = em.find(HasAncestorJPA.class, KeyFactory.encodeKey(hasAncestorEntity.getKey()));
    assertEquals(KeyFactory.encodeKey(flightEntity.getKey()), ha.getAncestorId());
    assertEquals("named key", KeyFactory.decodeKey(ha.getId()).getName());
    assertEquals("named parent key", KeyFactory.decodeKey(ha.getId()).getParent().getName());
  }
}
