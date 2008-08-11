// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import org.datanucleus.test.KitchenSink;

import javax.persistence.EntityTransaction;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPADeleteTest extends JPATestCase {

  public void testSimpleDelete() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.encodeKey(key);
    KitchenSink ks = em.find(KitchenSink.class, keyStr);
    assertNotNull(ks);
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.remove(ks);
    txn.commit();
    assertNull(em.find(KitchenSink.class, keyStr));
  }
}
