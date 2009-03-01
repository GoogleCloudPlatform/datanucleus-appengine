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
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.ShortBlob;

import org.datanucleus.test.HasBytesJPA;

import java.util.Arrays;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPABytesTest extends JPATestCase {

  public void testInsert() throws EntityNotFoundException {
    HasBytesJPA pojo = new HasBytesJPA();

    pojo.setPrimBytes("prim bytes".getBytes());
    pojo.setBytes(PrimitiveArrays.asList("bytes".getBytes()).toArray(new Byte[5]));
    pojo.setOnePrimByte(Integer.valueOf(1).byteValue());
    pojo.setOneByte(Integer.valueOf(2).byteValue());
    pojo.setShortBlob(new ShortBlob("short blob".getBytes()));

    beginTxn();
    em.persist(pojo);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(HasBytesJPA.class.getSimpleName(), pojo.getId()));
    assertEquals(new ShortBlob("prim bytes".getBytes()), e.getProperty("primBytes"));
    assertEquals(new ShortBlob("bytes".getBytes()), e.getProperty("bytes"));
    assertEquals(1L, e.getProperty("onePrimByte"));
    assertEquals(2L, e.getProperty("oneByte"));
    assertEquals(new ShortBlob("short blob".getBytes()), e.getProperty("shortBlob"));
  }

  public void testFetch() {
    Entity e = new Entity(HasBytesJPA.class.getSimpleName());
    e.setProperty("primBytes", new ShortBlob("prim bytes".getBytes()));
    e.setProperty("bytes", new ShortBlob("bytes".getBytes()));
    e.setProperty("onePrimByte", 1L);
    e.setProperty("oneByte", 2L);
    e.setProperty("shortBlob", new ShortBlob("short blob".getBytes()));

    ldth.ds.put(e);

    beginTxn();
    HasBytesJPA pojo = em.find(HasBytesJPA.class, e.getKey());
    assertTrue(Arrays.equals("prim bytes".getBytes(), pojo.getPrimBytes()));
    assertEquals(PrimitiveArrays.asList("bytes".getBytes()), Arrays.asList(pojo.getBytes()));
    assertEquals(Integer.valueOf(1).byteValue(), pojo.getOnePrimByte());
    assertEquals(Integer.valueOf(2).byteValue(), pojo.getOneByte().byteValue());
    assertEquals(new ShortBlob("short blob".getBytes()), pojo.getShortBlob());
    commitTxn();
  }
}
