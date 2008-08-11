// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import org.datanucleus.test.KitchenSink;

import javax.jdo.JDOObjectNotFoundException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDODeleteTest extends JDOTestCase {

  public void testSimpleDelete() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.encodeKey(key);
    KitchenSink ks = pm.getObjectById(KitchenSink.class, keyStr);
    assertNotNull(ks);
    pm.deletePersistent(ks);
    try {
      pm.getObjectById(KitchenSink.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    }
  }
}
