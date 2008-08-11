// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import javax.jdo.PersistenceManager;

import org.datanucleus.test.Flight;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JDOUpdateTest extends JDOTestCase {

  public void testSimpleUpdate() throws EntityNotFoundException {
    PersistenceManager pm = pmf.getPersistenceManager();
    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));

    String keyStr = KeyFactory.encodeKey(key);
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());

    pm.currentTransaction().begin();
    flight.setName("2");
    pm.currentTransaction().commit();

    Entity flightCheck = ldth.ds.get(key);
    assertEquals("yam", flightCheck.getProperty("origin"));
    assertEquals("bam", flightCheck.getProperty("dest"));
    assertEquals("2", flightCheck.getProperty("name"));
    assertEquals(1L, flightCheck.getProperty("you"));
    assertEquals(2L, flightCheck.getProperty("me"));
  }
}
