// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;
import org.datanucleus.test.Flight;
import org.datanucleus.test.KitchenSink;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOInsertionTest extends JDOTestCase {

  public void testSimpleInsert() throws EntityNotFoundException {
    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");
    assertNull(f1.getId());
    pm.makePersistent(f1);
    assertNotNull(f1.getId());
    Entity entity = ldth.ds.get(KeyFactory.decodeKey(f1.getId()));
    assertNotNull(entity);
    assertEquals("BOS", entity.getProperty("origin"));
    assertEquals("MIA", entity.getProperty("dest"));
    assertEquals("Harold", entity.getProperty("name"));
    assertEquals(2L, entity.getProperty("me"));
    assertEquals(4L, entity.getProperty("you"));
    assertEquals(Flight.class.getName(), entity.getKind());
  }

  public void testKitchenSinkInsert() throws EntityNotFoundException {
    KitchenSink ks = KitchenSink.newKitchenSink();
    assertNull(ks.key);
    pm.makePersistent(ks);
    assertNotNull(ks.key);

    Entity entity = ldth.ds.get(KeyFactory.decodeKey(ks.key));
    assertNotNull(entity);
    assertEquals(KitchenSink.class.getName(), entity.getKind());

    Entity sameEntity = KitchenSink.newKitchenSinkEntity(KeyFactory.decodeKey(ks.key));
    assertEquals(sameEntity.getProperties(), entity.getProperties());
  }

//  public void testEmbeddable() throws EntityNotFoundException {
//    Person p = new Person();
//    p.setDob(new Date());
//    p.setName(new Name());
//    p.getName().setFirstName("jimmy");
//    p.getName().setSurName("jam");
//    pm.makePersistent(p);
//
//    assertNotNull(p.getId());
//
//    Entity entity = ldth.ds.get(KeyFactory.decodeKey(p.getId()));
//    assertNotNull(entity);
//  }
}
