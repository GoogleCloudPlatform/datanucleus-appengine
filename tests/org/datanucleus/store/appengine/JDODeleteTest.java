// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasVersionWithFieldJDO;
import org.datanucleus.test.KitchenSink;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOOptimisticVerificationException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDODeleteTest extends JDOTestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "OPT_VERSION";

  public void testSimpleDelete() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.encodeKey(key);
    KitchenSink ks = pm.getObjectById(KitchenSink.class, keyStr);
    assertNotNull(ks);
    pm.currentTransaction().begin();
    pm.deletePersistent(ks);
    pm.currentTransaction().commit();
    try {
      pm.getObjectById(KitchenSink.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    }
  }

  public void testSimpleDelete_NamedKey() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity("named key", null));

    String keyStr = KeyFactory.encodeKey(key);
    KitchenSink ks = pm.getObjectById(KitchenSink.class, keyStr);
    assertNotNull(ks);
    assertEquals("named key", KeyFactory.decodeKey(ks.key).getName());
    pm.currentTransaction().begin();
    pm.deletePersistent(ks);
    pm.currentTransaction().commit();
    try {
      pm.getObjectById(KitchenSink.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    }
  }

  public void testOptimisticLocking_Update_NoField() {
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ldth.ds.put(flightEntity);

    String keyStr = KeyFactory.encodeKey(key);
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    pm.currentTransaction().begin();
    flightEntity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 2L);
    // we update the flight directly in the datastore right before we delete
    ldth.ds.put(flightEntity);
    try {
      pm.deletePersistent(flight);
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    } finally {
      pm.currentTransaction().rollback();
    }
  }

  public void testOptimisticLocking_Delete_NoField() {
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ldth.ds.put(flightEntity);

    String keyStr = KeyFactory.encodeKey(key);
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    pm.currentTransaction().begin();
    flight.setName("2");
    flightEntity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 2L);
    // we remove the flight from the datastore right before delete
    ldth.ds.delete(key);
    try {
      pm.deletePersistent(flight);
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    } finally {
      pm.currentTransaction().rollback();
    }
  }

  public void testOptimisticLocking_Update_HasVersionField() {
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.encodeKey(key);
    HasVersionWithFieldJDO hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);

    pm.currentTransaction().begin();
    hvwf.setValue("value");
    pm.currentTransaction().commit();
    assertEquals(2L, hvwf.getVersion());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);
    pm.currentTransaction().begin();
    // we update the entity directly in the datastore right before delete
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    ldth.ds.put(entity);
    try {
      pm.deletePersistent(hvwf);
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    } finally {
      pm.currentTransaction().rollback();
    }
    // make sure the version didn't change on the model object
    assertEquals(2L, hvwf.getVersion());
  }

  public void testOptimisticLocking_Delete_HasVersionField() {
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.encodeKey(key);
    HasVersionWithFieldJDO hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);

    pm.currentTransaction().begin();
    // delete the entity in the datastore right before we delete
    ldth.ds.delete(key);
    try {
      pm.deletePersistent(hvwf);
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    } finally {
      pm.currentTransaction().rollback();
    }
    // make sure the version didn't change on the model object
    assertEquals(1L, hvwf.getVersion());
  }
}
