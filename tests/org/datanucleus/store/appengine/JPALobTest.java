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
import com.google.appengine.api.datastore.Text;

import org.datanucleus.test.HasLob;

import java.util.Arrays;
import java.util.Date;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPALobTest extends JPATestCase {

  public void testInsert() throws EntityNotFoundException {
    HasLob pojo = new HasLob();

    pojo.setBigString("a really big string");
    pojo.setBigByteArray("a really big byte array".getBytes());
    Date now = new Date();
    pojo.setDateList(Utils.newArrayList(now));

    beginTxn();
    em.persist(pojo);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(HasLob.class.getSimpleName(), pojo.getId()));
    assertEquals(new Text("a really big string"), e.getProperty("bigString"));
    assertEquals(new Blob("a really big byte array".getBytes()), e.getProperty("bigByteArray"));
    assertEquals(new Blob(SerializationManager.DEFAULT_SERIALIZATION_STRATEGY.serialize(
        Utils.newArrayList(now)).getBytes()), e.getProperty("dateList"));
  }

  public void testFetch() {
    Entity e = new Entity(HasLob.class.getSimpleName());
    e.setProperty("bigString", new Text("a really big string"));
    e.setProperty("bigByteArray", new Blob("a really big byte array".getBytes()));
    Date now = new Date();
    e.setProperty("dateList", new Blob(SerializationManager.DEFAULT_SERIALIZATION_STRATEGY.serialize(
        Utils.newArrayList(now)).getBytes()));
    ldth.ds.put(e);

    beginTxn();
    HasLob pojo = em.find(HasLob.class, e.getKey());
    assertNotNull(pojo);
    assertEquals("a really big string", pojo.getBigString());
    assertTrue(Arrays.equals("a really big byte array".getBytes(), pojo.getBigByteArray()));
    assertEquals(Utils.newArrayList(now), pojo.getDateList());
  }
}