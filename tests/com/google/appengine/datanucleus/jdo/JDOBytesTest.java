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

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.datanucleus.PrimitiveArrays;
import com.google.appengine.datanucleus.SerializationManager;
import com.google.appengine.datanucleus.SerializationStrategy;
import com.google.appengine.datanucleus.test.HasBytesJDO;


import java.util.Arrays;
import java.util.HashSet;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOBytesTest extends JDOTestCase {

  private static final SerializationStrategy SS =
      SerializationManager.DEFAULT_SERIALIZATION_STRATEGY;

  public void testInsert() throws EntityNotFoundException {
    HasBytesJDO pojo = new HasBytesJDO();

    pojo.setPrimBytes("prim bytes".getBytes());
    pojo.setBytes(PrimitiveArrays.asList("bytes".getBytes()).toArray(new Byte[5]));
    pojo.setByteList(PrimitiveArrays.asList("byte list".getBytes()));
    pojo.setByteSet(new HashSet<Byte>(PrimitiveArrays.asList("byte set".getBytes())));
    pojo.setByteCollection(PrimitiveArrays.asList("byte collection".getBytes()));
    pojo.setOnePrimByte(Integer.valueOf(1).byteValue());
    pojo.setOneByte(Integer.valueOf(2).byteValue());
    pojo.setShortBlob(new ShortBlob("short blob".getBytes()));
    pojo.setSerializedPrimBytes("serialized prim bytes".getBytes());
    pojo.setSerializedBytes(PrimitiveArrays.asList("serialized bytes".getBytes()).toArray(new Byte[5]));
    pojo.setSerializedByteList(PrimitiveArrays.asList("serialized byte list".getBytes()));
    pojo.setSerializedByteSet(new HashSet<Byte>(PrimitiveArrays.asList("serialized byte set".getBytes())));

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    Entity e = ds.get(KeyFactory.createKey(HasBytesJDO.class.getSimpleName(), pojo.getId()));
    assertEquals(new ShortBlob("prim bytes".getBytes()), e.getProperty("primBytes"));
    assertEquals(new ShortBlob("bytes".getBytes()), e.getProperty("bytes"));
    assertEquals(new ShortBlob("byte list".getBytes()), e.getProperty("byteList"));
    assertEquals(new HashSet<Byte>(PrimitiveArrays.asList("byte set".getBytes())),
                 new HashSet<Byte>(PrimitiveArrays.asList(
                     ((ShortBlob) e.getProperty("byteSet")).getBytes())));
    assertEquals(new ShortBlob("byte collection".getBytes()), e.getProperty("byteCollection"));
    assertEquals(1L, e.getProperty("onePrimByte"));
    assertEquals(2L, e.getProperty("oneByte"));
    assertEquals(new ShortBlob("short blob".getBytes()), e.getProperty("shortBlob"));
    assertEquals(new Blob("serialized prim bytes".getBytes()), e.getProperty("serializedPrimBytes"));
    assertEquals(new Blob("serialized bytes".getBytes()), e.getProperty("serializedBytes"));
    assertEquals(SS.serialize(PrimitiveArrays.asList("serialized byte list".getBytes())),
                 e.getProperty("serializedByteList"));
    assertEquals(SS.serialize(new HashSet<Byte>(PrimitiveArrays.asList(
        "serialized byte set".getBytes()))), e.getProperty("serializedByteSet"));
  }

  public void testFetch() {
    Entity e = new Entity(HasBytesJDO.class.getSimpleName());
    e.setProperty("primBytes", new ShortBlob("prim bytes".getBytes()));
    e.setProperty("bytes", new ShortBlob("bytes".getBytes()));
    e.setProperty("byteList", new ShortBlob("byte list".getBytes()));
    e.setProperty("byteSet", new ShortBlob("byte set".getBytes()));
    e.setProperty("byteCollection", new ShortBlob("byte collection".getBytes()));
    e.setProperty("onePrimByte", 1L);
    e.setProperty("oneByte", 2L);
    e.setProperty("shortBlob", new ShortBlob("short blob".getBytes()));
    e.setProperty("serializedPrimBytes", new Blob("serialized prim bytes".getBytes()));
    e.setProperty("serializedBytes", new Blob("serialized bytes".getBytes()));
    e.setProperty("serializedByteList", SS.serialize(PrimitiveArrays.asList("serialized byte list".getBytes())));
    e.setProperty("serializedByteSet", SS.serialize(new HashSet<Byte>(PrimitiveArrays.asList("serialized byte set".getBytes()))));

    ds.put(e);

    beginTxn();
    HasBytesJDO pojo = pm.getObjectById(HasBytesJDO.class, e.getKey());
    assertTrue(Arrays.equals("prim bytes".getBytes(), pojo.getPrimBytes()));
    assertEquals(PrimitiveArrays.asList("bytes".getBytes()), Arrays.asList(pojo.getBytes()));
    assertEquals(PrimitiveArrays.asList("byte list".getBytes()), pojo.getByteList());
    assertEquals(new HashSet<Byte>(PrimitiveArrays.asList("byte set".getBytes())), pojo.getByteSet());
    assertEquals(PrimitiveArrays.asList("byte collection".getBytes()), pojo.getByteCollection());
    assertEquals(Integer.valueOf(1).byteValue(), pojo.getOnePrimByte());
    assertEquals(Integer.valueOf(2).byteValue(), pojo.getOneByte().byteValue());
    assertEquals(new ShortBlob("short blob".getBytes()), pojo.getShortBlob());
    assertEquals(new Blob("serialized prim bytes".getBytes()), new Blob(pojo.getSerializedPrimBytes()));
    assertEquals(new Blob("serialized bytes".getBytes()),
                 new Blob(PrimitiveArrays.toByteArray(Arrays.asList(pojo.getSerializedBytes()))));
    assertEquals(PrimitiveArrays.asList("serialized byte list".getBytes()),
                 pojo.getSerializedByteList());
    assertEquals(new HashSet<Byte>(PrimitiveArrays.asList("serialized byte set".getBytes())),
                 pojo.getSerializedByteSet());
    commitTxn();
  }
}
