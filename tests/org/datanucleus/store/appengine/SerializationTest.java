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

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.apphosting.api.DatastorePb;

import org.datanucleus.test.HasSerializableJDO;

import java.util.List;

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
    hasSerializable.setYamList(Utils.newArrayList(yam));
    hasSerializable.setQuery(query);
    hasSerializable.setInteger(3);
    beginTxn();
    pm.makePersistent(hasSerializable);
    commitTxn();

    SerializationStrategy ss = SerializationManager.DEFAULT_SERIALIZATION_STRATEGY;
    Entity e = ldth.ds.get(TestUtils.createKey(hasSerializable, hasSerializable.getId()));
    Blob yamBlob = (Blob) e.getProperty("yam");
    assertNotNull(yamBlob);
    HasSerializableJDO.Yam reloadedYam = (HasSerializableJDO.Yam)
        ss.deserialize(yamBlob, HasSerializableJDO.Yam.class);
    assertEquals(yam.getStr1(), reloadedYam.getStr1());
    assertEquals(yam.getStr2(), reloadedYam.getStr2());
    Blob yamListBlob = (Blob) e.getProperty("yamList");
    List<HasSerializableJDO.Yam> reloadedYamList = (List<HasSerializableJDO.Yam>)
        ss.deserialize(yamListBlob, List.class);
    assertEquals(1, reloadedYamList.size());
    reloadedYam = reloadedYamList.get(0);
    assertEquals(yam.getStr1(), reloadedYam.getStr1());
    assertEquals(yam.getStr2(), reloadedYam.getStr2());
    Blob queryBlob = (Blob) e.getProperty("query");
    assertNotNull(queryBlob);
    DatastorePb.Query reloadedQuery = (DatastorePb.Query)
        new HasSerializableJDO.ProtocolBufferSerializationStrategy().deserialize(queryBlob, DatastorePb.Query.class);
    assertEquals(query.getApp(), reloadedQuery.getApp());
    assertEquals(query.getKind(), reloadedQuery.getKind());
    Blob integerBlob = (Blob) e.getProperty("integer");
    assertEquals(Integer.valueOf(3), ss.deserialize(integerBlob, Integer.class));
  }

  public void testFetch() {

    Entity e = new Entity(HasSerializableJDO.class.getSimpleName());
    SerializationStrategy ss = SerializationManager.DEFAULT_SERIALIZATION_STRATEGY;

    HasSerializableJDO.Yam yam = new HasSerializableJDO.Yam();
    yam.setStr1("a");
    yam.setStr2("b");
    e.setProperty("yam", ss.serialize(yam));

    List<HasSerializableJDO.Yam> yamList = Utils.newArrayList(yam);
    e.setProperty("yamList", ss.serialize(yamList));
    DatastorePb.Query query = new DatastorePb.Query();
    query.setApp("harold");
    query.setKind("yes");
    e.setProperty("query", new HasSerializableJDO.ProtocolBufferSerializationStrategy().serialize(query));
    e.setProperty("integer", ss.serialize(3));
    ldth.ds.put(e);
    beginTxn();
    HasSerializableJDO hasSerializable = pm.getObjectById(
        HasSerializableJDO.class, KeyFactory.keyToString(e.getKey()));
    assertNotNull(hasSerializable);
    assertNotNull(hasSerializable.getYam());
    assertEquals(yam.getStr1(), hasSerializable.getYam().getStr1());
    assertEquals(yam.getStr2(), hasSerializable.getYam().getStr2());
    assertEquals(yamList, hasSerializable.getYamList());
    assertNotNull(hasSerializable.getQuery());
    assertEquals(query.getApp(), hasSerializable.getQuery().getApp());
    assertEquals(query.getKind(), hasSerializable.getQuery().getKind());
    assertEquals(Integer.valueOf(3), hasSerializable.getInteger());
    commitTxn();
  }
}
