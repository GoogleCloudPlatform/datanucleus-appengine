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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.jdo.KitchenSink;


/**
 * TODO This uses KitchenSink but that is a JDO model class!!
 * @author Max Ross <maxr@google.com>
 */
public class JPADeleteTest extends JPATestCase {

  public void testSimpleDelete() {
    Key key = ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.keyToString(key);
    KitchenSink ks;
    beginTxn();
    ks = em.find(KitchenSink.class, keyStr);
    assertNotNull(ks);
    em.remove(ks);
    commitTxn();
    beginTxn();
    try {
      assertNull(em.find(KitchenSink.class, keyStr));
    } finally {
      commitTxn();
    }
  }

  public void testSimpleDeleteWithNamedKey() {
    Key key = ds.put(KitchenSink.newKitchenSinkEntity("named key", null));
    assertEquals("named key", key.getName());
    String keyStr = KeyFactory.keyToString(key);
    KitchenSink ks;
    beginTxn();
    ks = em.find(KitchenSink.class, keyStr);
    assertNotNull(ks);
    em.remove(ks);
    commitTxn();
    beginTxn();
    try {
      assertNull(em.find(KitchenSink.class, keyStr));
    } finally {
      commitTxn();
    }
  }

  public void testDeletePersistentNew() {
    int count = countForClass(KitchenSink.class);
    beginTxn();
    KitchenSink ks = KitchenSink.newKitchenSink();
    em.persist(ks);
    em.remove(ks);
    commitTxn();
    assertEquals(count, countForClass(KitchenSink.class));
  }

  public void testDeletePersistentNew_NoTxn() {
    int count = countForClass(KitchenSink.class);
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    KitchenSink ks = KitchenSink.newKitchenSink();
    em.persist(ks);
    em.remove(ks);
    assertEquals(count, countForClass(KitchenSink.class));
    em.close();
  }

}
