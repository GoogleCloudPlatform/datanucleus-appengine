// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Flight;
import org.datanucleus.test.KitchenSink;

import javax.jdo.JDOObjectNotFoundException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOFetchTest extends JDOTestCase {

  public void testSimpleFetch() {
    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));

    String keyStr = KeyFactory.encodeKey(key);
    Flight flight = pm.getObjectById(Flight.class, keyStr);
    assertNotNull(flight);
    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());
  }

  public void testSimpleFetch_NamedKey() {
    Key key = ldth.ds.put(Flight.newFlightEntity("named key", "1", "yam", "bam", 1, 2));

    String keyStr = KeyFactory.encodeKey(key);
    Flight flight = pm.getObjectById(Flight.class, keyStr);
    assertNotNull(flight);
    assertEquals(keyStr, flight.getId());
    assertEquals("named key", KeyFactory.decodeKey(flight.getId()).getName());
  }

  public void testFetchNonExistent() {
    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    ldth.ds.delete(key);
    String keyStr = KeyFactory.encodeKey(key);
    try {
      pm.getObjectById(Flight.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    }
  }

  public void testKitchenSinkFetch() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.encodeKey(key);
    KitchenSink ks =
        pmf.getPersistenceManager().getObjectById(KitchenSink.class, keyStr);
    assertNotNull(ks);
    assertEquals(keyStr, ks.key);
    assertEquals(KitchenSink.newKitchenSink(ks.key), ks);
  }
}
