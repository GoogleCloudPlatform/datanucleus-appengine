// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasVersionWithFieldJDO;
import org.datanucleus.test.KitchenSink;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOOptimisticVerificationException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDODeleteTest extends JDOTestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "OPT_VERSION";

  public void testSimpleDelete() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity(null));

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    KitchenSink ks = pm.getObjectById(KitchenSink.class, keyStr);
    assertNotNull(ks);
    pm.deletePersistent(ks);
    commitTxn();
    beginTxn();
    try {
      pm.getObjectById(KitchenSink.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    } finally {
      rollbackTxn();
    }
  }

  public void testSimpleDelete_NamedKey() {
    Key key = ldth.ds.put(KitchenSink.newKitchenSinkEntity("named key", null));

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    KitchenSink ks = pm.getObjectById(KitchenSink.class, keyStr);
    assertNotNull(ks);
    assertEquals("named key", KeyFactory.stringToKey(ks.key).getName());
    pm.deletePersistent(ks);
    commitTxn();
    beginTxn();
    try {
      pm.getObjectById(KitchenSink.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    } finally {
      rollbackTxn();
    }
  }

  public void testOptimisticLocking_Update_NoField() {
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ldth.ds.put(flightEntity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    flightEntity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 2L);
    // we update the flight directly in the datastore right before we delete
    ldth.ds.put(flightEntity);
    try {
      pm.deletePersistent(flight);
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    } finally {
      rollbackTxn();
    }
  }

  public void testOptimisticLocking_Delete_NoField() {
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ldth.ds.put(flightEntity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

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
      rollbackTxn();
    }
  }

  public void testOptimisticLocking_Update_HasVersionField() {
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionWithFieldJDO hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);

    hvwf.setValue("value");
    commitTxn();
    beginTxn();
    hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);
    assertEquals(2L, hvwf.getVersion());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    // we update the entity directly in the datastore right before delete
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    ldth.ds.put(entity);
    try {
      pm.deletePersistent(hvwf);
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    } finally {
      rollbackTxn();
    }
    // make sure the version didn't change on the model object
    assertEquals(2L, JDOHelper.getVersion(hvwf));
  }

  public void testOptimisticLocking_Delete_HasVersionField() {
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionWithFieldJDO hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);

    // delete the entity in the datastore right before we delete
    ldth.ds.delete(key);
    try {
      pm.deletePersistent(hvwf);
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    } finally {
      rollbackTxn();
    }
    // make sure the version didn't change on the model object
    assertEquals(1L, JDOHelper.getVersion(hvwf));
  }

  public void testNonTransactionalDelete() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    Flight f = pm.getObjectById(Flight.class, KeyFactory.keyToString(key));
    pm.deletePersistent(f);
    pm.close();
    try {
      ldth.ds.get(key);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    pm = pmf.getPersistenceManager();
  }
}
