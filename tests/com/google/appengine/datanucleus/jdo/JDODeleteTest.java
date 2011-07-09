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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.HasKeyAncestorKeyPkJDO;
import com.google.appengine.datanucleus.test.HasVersionWithFieldJDO;
import com.google.appengine.datanucleus.test.KitchenSink;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOOptimisticVerificationException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDODeleteTest extends JDOTestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "VERSION";

  public void testSimpleDelete() {
    Key key = ds.put(KitchenSink.newKitchenSinkEntity(null));

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
    Key key = ds.put(KitchenSink.newKitchenSinkEntity("named key", null));

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
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ds.put(flightEntity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    flightEntity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 2L);
    // we update the flight directly in the datastore right before we delete
    ds.put(flightEntity);
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
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ds.put(flightEntity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    flight.setName("2");
    flightEntity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 2L);
    // we remove the flight from the datastore right before delete
    ds.delete(key);
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
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ds.put(entity);

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
    ds.put(entity);
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
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionWithFieldJDO hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);

    // delete the entity in the datastore right before we delete
    ds.delete(key);
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

    Key key = ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    Flight f = pm.getObjectById(Flight.class, KeyFactory.keyToString(key));
    pm.deletePersistent(f);
    pm.close();
    try {
      ds.get(key);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    pm = pmf.getPersistenceManager();
  }

  public void testDeleteHasAncestorPkField() {
    Entity e =
        new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), KeyFactory.createKey("Yam", 24));
    ds.put(e);
    beginTxn();
    HasKeyAncestorKeyPkJDO pojo = pm.getObjectById(HasKeyAncestorKeyPkJDO.class, e.getKey());
    pm.deletePersistent(pojo);
    commitTxn();
  }

  public void testDeletePersistentNew() {
    beginTxn();
    KitchenSink ks = KitchenSink.newKitchenSink();
    pm.makePersistent(ks);
    String keyStr = ks.key;
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

  public void testDeletePersistentNew_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    KitchenSink ks = KitchenSink.newKitchenSink();
    pm.makePersistent(ks);
    String keyStr = ks.key;
    pm.deletePersistent(ks);
    pm.close();
    pm = pmf.getPersistenceManager();
    try {
      pm.getObjectById(KitchenSink.class, keyStr);
      fail("expected onfe");
    } catch (JDOObjectNotFoundException onfe) {
      // good
    }
  }
}
