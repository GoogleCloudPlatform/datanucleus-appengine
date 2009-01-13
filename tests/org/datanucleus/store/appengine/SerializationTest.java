// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.DatastorePb;
import com.google.apphosting.api.datastore.Blob;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.HasSerializableJDO;

/**
 * Serialization tests.
 *
 * @author Max Ross <maxr@google.com>
 */
public class SerializationTest extends JDOTestCase {

  public void testInsert() throws EntityNotFoundException {
    HasSerializableJDO.Yam yam = new HasSerializableJDO.Yam();
    yam.setStr1("a");
    yam.setStr2("b");

    DatastorePb.Query query = new DatastorePb.Query();
    query.setApp("orm");
    query.setKind("fruit");

    HasSerializableJDO hasSerializable = new HasSerializableJDO();
    hasSerializable.setYam(yam);
    hasSerializable.setQuery(query);
    beginTxn();
    pm.makePersistent(hasSerializable);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.decodeKey(hasSerializable.getId()));
    Blob yamBlob = (Blob) e.getProperty("yam");
    assertNotNull(yamBlob);
    HasSerializableJDO.Yam reloadedYam = (HasSerializableJDO.Yam)
        SerializationManager.DEFAULT_SERIALIZATION_STRATEGY.deserialize(yamBlob, HasSerializableJDO.Yam.class);
    assertEquals(yam.getStr1(), reloadedYam.getStr1());
    assertEquals(yam.getStr2(), reloadedYam.getStr2());
    Blob queryBlob = (Blob) e.getProperty("query");
    assertNotNull(queryBlob);
    DatastorePb.Query reloadedQuery = (DatastorePb.Query)
        new HasSerializableJDO.ProtocolBufferSerializationStrategy().deserialize(queryBlob, DatastorePb.Query.class);
    assertEquals(query.getApp(), reloadedQuery.getApp());
    assertEquals(query.getKind(), reloadedQuery.getKind());
  }

  public void testFetch() {

    Entity e = new Entity(HasSerializableJDO.class.getSimpleName());

    HasSerializableJDO.Yam yam = new HasSerializableJDO.Yam();
    yam.setStr1("a");
    yam.setStr2("b");
    e.setProperty("yam", SerializationManager.DEFAULT_SERIALIZATION_STRATEGY.serialize(yam));

    DatastorePb.Query query = new DatastorePb.Query();
    query.setApp("harold");
    query.setKind("yes");
    e.setProperty("query", new HasSerializableJDO.ProtocolBufferSerializationStrategy().serialize(query));
    ldth.ds.put(e);
    beginTxn();
    HasSerializableJDO hasSerializable = pm.getObjectById(
        HasSerializableJDO.class, KeyFactory.encodeKey(e.getKey()));
    assertNotNull(hasSerializable);
    assertNotNull(hasSerializable.getYam());
    assertEquals(yam.getStr1(), hasSerializable.getYam().getStr1());
    assertEquals(yam.getStr2(), hasSerializable.getYam().getStr2());
    assertNotNull(hasSerializable.getQuery());
    assertEquals(query.getApp(), hasSerializable.getQuery().getApp());
    assertEquals(query.getKind(), hasSerializable.getQuery().getKind());
    commitTxn();
  }
}
