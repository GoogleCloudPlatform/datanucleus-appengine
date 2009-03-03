/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * **********************************************************************/

package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.apphosting.api.ApiProxy;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.test.Flight;

import java.util.ConcurrentModificationException;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOConcurrentModificationTest extends JDOTestCase {

  public void testInsertCollides() {
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);
    Flight flight = new Flight();
    flight.setName("harold");
    flight.setOrigin("bos");
    flight.setDest("mia");
    flight.setYou(23);
    flight.setMe(24);
    flight.setFlightNumber(88);
    beginTxn();
    try {
      pm.makePersistent(flight);
      fail("expected exception");
    } catch (JDODataStoreException e) {
      // good
      assertTrue(e.getCause() instanceof ConcurrentModificationException);
    }
    assertTrue(pm.currentTransaction().isActive());
    rollbackTxn();
    assertEquals(flight, "harold", "bos", "mia", 23, 24, 88);
  }

  public void testInsertCollidesOnCommit() {
    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count != 0) {
              throw new ConcurrentModificationException();
            }
            count++;
          }
        };
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);
    Flight flight = new Flight();
    flight.setName("harold");
    flight.setOrigin("bos");
    flight.setDest("mia");
    flight.setYou(23);
    flight.setMe(24);
    flight.setFlightNumber(88);
    beginTxn();
    pm.makePersistent(flight);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOException e) {
      // good
      assertTrue(e.getCause() instanceof NucleusDataStoreException);
      assertTrue(e.getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(pm.currentTransaction().isActive());
    assertEquals(flight, "harold", "bos", "mia", 23, 24, 88);
  }

  public void testUpdateCollides() {
    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    CollisionDatastoreDelegate dd =
        new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    beginTxn();
    Flight f = pm.getObjectById(Flight.class, e.getKey());
    f.setYou(12);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDODataStoreException ex) {
      // good
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(pm.currentTransaction().isActive());
  }

  public void testUpdateOfDetachedCollides() {
    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    beginTxn();
    Flight f = pm.detachCopy(pm.getObjectById(Flight.class, e.getKey()));
    commitTxn();

    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    // update detached object
    f.setYou(12);
    beginTxn();

    // reattach
    pm.makePersistent(f);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDODataStoreException ex) {
      // good
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(pm.currentTransaction().isActive());
    // now verify that the new value is still in the detached version.
    assertEquals(f, "harold", "bos", "mia", 12, 24, 88);
  }

  public void testUpdateOfDetachedCollidesThenSucceeds() {

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    beginTxn();
    Flight f = pm.detachCopy(pm.getObjectById(Flight.class, e.getKey()));
    commitTxn();

    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update detached object
    f.setYou(12);
    beginTxn();

    // reattach
    pm.makePersistent(f);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDODataStoreException ex) {
      // good
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(pm.currentTransaction().isActive());
    beginTxn();
    pm.makePersistent(f);
    commitTxn();
    beginTxn();
    f = pm.getObjectById(Flight.class, e.getKey());
    assertEquals(f, "harold", "bos", "mia", 12, 24, 88);
    commitTxn();
  }

  public void testUpdateOfAttachedCollidesThenSucceeds() {

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    beginTxn();
    Flight f = pm.getObjectById(Flight.class, e.getKey());
    // make a copy right away, otherwise our change will get reverted
    // when the txn rolls back
    Flight fCopy = pm.detachCopy(f);
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update attached object
    fCopy.setYou(12);
    pm.makePersistent(fCopy);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDODataStoreException ex) {
      // good
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(pm.currentTransaction().isActive());
    beginTxn();
    pm.makePersistent(fCopy);
    commitTxn();
    beginTxn();
    f = pm.getObjectById(Flight.class, e.getKey());
    assertEquals(f, "harold", "bos", "mia", 12, 24, 88);
    commitTxn();
  }

  public void testDeleteCollides() {
    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    beginTxn();
    Flight f = pm.getObjectById(Flight.class, e.getKey());

    try {
      pm.deletePersistent(f);
      fail("expected exception");
    } catch (JDODataStoreException ex) {
      // good
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
    assertTrue(pm.currentTransaction().isActive());
    rollbackTxn();
  }

  public void testInsertCollides_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);
    Flight flight = new Flight();
    flight.setName("harold");
    flight.setOrigin("bos");
    flight.setDest("mia");
    flight.setYou(23);
    flight.setMe(24);
    flight.setFlightNumber(88);
    try {
      pm.makePersistent(flight);
      fail("expected exception");
    } catch (JDODataStoreException e) {
      // good
      assertTrue(e.getCause() instanceof ConcurrentModificationException);
    }
    assertEquals(flight, "harold", "bos", "mia", 23, 24, 88);
  }

  public void testUpdateCollides_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    Flight f = pm.getObjectById(Flight.class, e.getKey());
    f.setYou(12);
    try {
      pm.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // good
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
  }

  public void testUpdateOfDetachedCollides_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    Flight f = pm.detachCopy(pm.getObjectById(Flight.class, e.getKey()));
    pm.close();
    pm = pmf.getPersistenceManager();

    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    // update detached object
    f.setYou(12);

    // reattach
    pm.makePersistent(f);
    try {
      pm.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // good
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    // now verify that the new value is still in the detached version.
    assertEquals(f, "harold", "bos", "mia", 12, 24, 88);
  }

  public void testUpdateOfDetachedCollidesThenSucceeds_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    Flight f = pm.detachCopy(pm.getObjectById(Flight.class, e.getKey()));
    pm.close();
    pm = pmf.getPersistenceManager();
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update detached object
    f.setYou(12);

    // reattach
    pm.makePersistent(f);
    try {
      pm.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // good
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    pm = pmf.getPersistenceManager();
    pm.makePersistent(f);
    pm.close();
    pm = pmf.getPersistenceManager();
    f = pm.getObjectById(Flight.class, e.getKey());
    assertEquals(f, "harold", "bos", "mia", 12, 24, 88);
    pm.close();
  }

  public void testUpdateOfAttachedCollidesThenSucceeds_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    Flight f = pm.getObjectById(Flight.class, e.getKey());
    // make a copy right away, otherwise our change will get reverted
    // when the txn rolls back
    Flight fCopy = pm.detachCopy(f);
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update attached object
    fCopy.setYou(12);
    pm.makePersistent(fCopy);
    try {
      pm.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // good
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    pm = pmf.getPersistenceManager();
    pm.makePersistent(fCopy);
    pm.close();
    pm = pmf.getPersistenceManager();
    f = pm.getObjectById(Flight.class, e.getKey());
    assertEquals(f, "harold", "bos", "mia", 12, 24, 88);
    pm.close();
  }

  public void testDeleteCollides_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    Entity e = Flight.newFlightEntity("harold", "bos", "mia", 23, 24, 88);
    ldth.ds.put(e);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    Flight f = pm.getObjectById(Flight.class, e.getKey());

    try {
      pm.deletePersistent(f);
      fail("expected exception");
    } catch (JDODataStoreException ex) {
      // good
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
    pm.close();
  }

  private void assertEquals(Flight f, String name, String orig, String dest, int you, int me, int flightNumber) {
    assertEquals(name, f.getName());
    assertEquals(orig, f.getOrigin());
    assertEquals(dest, f.getDest());
    assertEquals(you, f.getYou());
    assertEquals(me, f.getMe());
    assertEquals(flightNumber, f.getFlightNumber());
  }
}
