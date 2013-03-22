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

import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.jdo.BidirectionalChildListJDO;
import com.google.appengine.datanucleus.test.jdo.Flight;
import com.google.appengine.datanucleus.test.jdo.HasKeyAncestorKeyPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasOneToManyListJDO;
import com.google.appengine.datanucleus.test.jdo.HasOneToOneJDO;

import java.lang.reflect.Method;

import javax.jdo.JDOUserException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOBatchDeleteTest extends JDOBatchTestCase {

  private static Entity newFlightEntity() {
    return Flight.newFlightEntity("Harold", "BOS", "MIA", 4, 2);
  }

  private static Entity newFlightEntity(Key parent, int childIndex) {
    Entity e = Flight.newFlightEntity(parent, null, "Harold", "BOS", "MIA", 4, 2, 23);
    e.setProperty("flights_INTEGER_IDX", childIndex);
    return e;
  }

  private static Entity newBidirChildEntity(Key parent, int childIndex) {
    Entity e = new Entity(BidirectionalChildListJDO.class.getSimpleName(), parent);
    e.setProperty("bidirChildren_INTEGER_IDX", childIndex);
    return e;
  }

  BatchRecorder newBatchRecorder() {
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    return new BatchRecorder(config) {
      boolean isBatchMethod(Method method) {
        return method.getName().equals("delete") && 
          (method.getParameterTypes().length == 1 && Iterable.class.isAssignableFrom(method.getParameterTypes()[0])) ||
          (method.getParameterTypes().length == 2 && Iterable.class.isAssignableFrom(method.getParameterTypes()[1]));
      }
    };
  }

  public void testDeletePersistentAll_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Key k1 = ds.put(newFlightEntity());
    Key k2 = ds.put(newFlightEntity());
    Flight f1 = pm.getObjectById(Flight.class, k1);
    Flight f2 = pm.getObjectById(Flight.class, k2);
    pm.deletePersistentAll(f1, f2);
    assertEquals(0, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);

    Key k3 = ds.put(newFlightEntity());
    Key k4 = ds.put(newFlightEntity());
    Flight f3 = pm.getObjectById(Flight.class, k3);
    Flight f4 = pm.getObjectById(Flight.class, k4);
    pm.deletePersistentAll(Utils.newArrayList(f3, f4));
    assertEquals(0, countForClass(Flight.class));
    assertEquals(2, batchRecorder.batchOps);
  }

  public void testDeletePersistentAll_OneEntity_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Key k1 = ds.put(newFlightEntity());
    Flight f1 = pm.getObjectById(Flight.class, k1);
    pm.deletePersistentAll(f1);
    assertEquals(0, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);

    Key k2 = ds.put(newFlightEntity());
    Flight f2 = pm.getObjectById(Flight.class, k2);
    pm.deletePersistentAll(Utils.newArrayList(f2));
    assertEquals(0, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);
  }

  public void testDeletePersistentAll_Txn_MultipleEntityGroups() {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    Key k1 = ds.put(newFlightEntity());
    Key k2 = ds.put(newFlightEntity());
    beginTxn();
    Flight f1 = pm.getObjectById(Flight.class, k1);
    commitTxn();
    beginTxn();
    Flight f2 = pm.getObjectById(Flight.class, k2);
    commitTxn();
    beginTxn();
    try {
      pm.deletePersistentAll(f1, f2);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
    }
    rollbackTxn();
    assertEquals(2, countForClass(Flight.class));
    // We don't get to the batch delete because we blow up
    // before we get there.
    assertEquals(0, batchRecorder.batchOps);

    Key k3 = ds.put(newFlightEntity());
    Key k4 = ds.put(newFlightEntity());
    beginTxn();
    Flight f3 = pm.getObjectById(Flight.class, k3);
    commitTxn();
    beginTxn();
    Flight f4 = pm.getObjectById(Flight.class, k4);
    commitTxn();
    beginTxn();
    try {
      pm.deletePersistentAll(Utils.newArrayList(f3, f4));
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
    }
    rollbackTxn();
    assertEquals(4, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);
    // cleanup outside tx
    pm.deletePersistentAll(Utils.newArrayList(f1, f2, f3, f4));
  }

  public void testDeletePersistentAll_Txn_OneEntityGroup() {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    Key parentKey = KeyFactory.createKey("yar", 24);
    Key k1 = ds.put(new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey));
    Key k2 = ds.put(new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey));
    beginTxn();
    HasKeyAncestorKeyPkJDO child1 = pm.getObjectById(HasKeyAncestorKeyPkJDO.class, k1);
    HasKeyAncestorKeyPkJDO child2 = pm.getObjectById(HasKeyAncestorKeyPkJDO.class, k2);
    pm.deletePersistentAll(child1, child2);
    commitTxn();
    assertEquals(0, countForClass(HasKeyAncestorKeyPkJDO.class));
    assertEquals(1, batchRecorder.batchOps);
  }

  public void testDeletePersistentAll_OneEntity_Txn() {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    Key k1 = ds.put(newFlightEntity());
    beginTxn();
    Flight f1 = pm.getObjectById(Flight.class, k1);
    pm.deletePersistentAll(f1);
    commitTxn();
    assertEquals(0, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);

    Key k2 = ds.put(newFlightEntity());
    beginTxn();
    Flight f2 = pm.getObjectById(Flight.class, k2);
    pm.deletePersistentAll(Utils.newArrayList(f2));
    commitTxn();
    assertEquals(0, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);
  }

  public void testDeletePersistentAll_CascadeDelete_OneToOne_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Key k1 = ds.put(new Entity(HasOneToOneJDO.class.getSimpleName()));
    Key k2 = ds.put(new Entity(HasOneToOneJDO.class.getSimpleName()));
    ds.put(newFlightEntity(k1, 1));
    ds.put(newFlightEntity(k2, 1));
    HasOneToOneJDO parent1 = pm.getObjectById(HasOneToOneJDO.class, k1);
    HasOneToOneJDO parent2 = pm.getObjectById(HasOneToOneJDO.class, k2);
    pm.deletePersistentAll(parent1, parent2);
    assertEquals(0, countForClass(HasOneToOneJDO.class));
    assertEquals(0, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);
  }


  public void testDeletePersistentAll_CascadeDelete_OneToOne_MultipleEntityGroups_Txn() {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    Key k1 = ds.put(new Entity(HasOneToOneJDO.class.getSimpleName()));
    Key k2 = ds.put(new Entity(HasOneToOneJDO.class.getSimpleName()));
    ds.put(newFlightEntity(k1, 1));
    ds.put(newFlightEntity(k2, 1));
    beginTxn();
    HasOneToOneJDO parent1 = pm.getObjectById(HasOneToOneJDO.class, k1);
    commitTxn();
    beginTxn();
    HasOneToOneJDO parent2 = pm.getObjectById(HasOneToOneJDO.class, k2);
    commitTxn();
    beginTxn();
    try {
      pm.deletePersistentAll(parent1, parent2);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
    }
    rollbackTxn();
    assertEquals(2, countForClass(HasOneToOneJDO.class));
    assertEquals(2, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);
    // cleanup outside tx
    pm.deletePersistentAll(parent1, parent2);
  }

  public void testDeletePersistentAll_CascadeDelete_OneToOne_OneEntityGroup_Txn() {
    switchDatasource(PersistenceManagerFactoryName.transactional);

    Key k1 = ds.put(new Entity(HasOneToOneJDO.class.getSimpleName(), KeyFactory.createKey("Yar", 43)));
    Key k2 = ds.put(new Entity(HasOneToOneJDO.class.getSimpleName(), KeyFactory.createKey("Yar", 43)));
    ds.put(newFlightEntity(k1, 1));
    ds.put(newFlightEntity(k2, 1));
    beginTxn();
    HasOneToOneJDO parent1 = pm.getObjectById(HasOneToOneJDO.class, k1);
    HasOneToOneJDO parent2 = pm.getObjectById(HasOneToOneJDO.class, k2);
    pm.deletePersistentAll(parent1, parent2);
    commitTxn();
    assertEquals(0, countForClass(HasOneToOneJDO.class));
    assertEquals(0, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);
  }

  public void testDeletePersistentAll_CascadeDelete_OneToMany_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    Entity owner1Entity = new Entity(HasOneToManyListJDO.class.getSimpleName());
    Key k1 = ds.put(owner1Entity);
    Entity owner2Entity = new Entity(HasOneToManyListJDO.class.getSimpleName());
    Key k2 = ds.put(owner2Entity);

    Entity fl1 = newFlightEntity(k1, 1);
    Entity fl2 = newFlightEntity(k1, 2);
    Entity fl3 = newFlightEntity(k2, 1);
    Entity fl4 = newFlightEntity(k2, 2);
    ds.put(fl1);
    ds.put(fl2);
    ds.put(fl3);
    ds.put(fl4);

    Entity bi1 = newBidirChildEntity(k1, 1);
    Entity bi2 = newBidirChildEntity(k1, 2);
    Entity bi3 = newBidirChildEntity(k2, 1);
    Entity bi4 = newBidirChildEntity(k2, 2);
    ds.put(bi1);
    ds.put(bi2);
    ds.put(bi3);
    ds.put(bi4);

    owner1Entity.setProperty("flights", Utils.newArrayList(fl1.getKey(), fl2.getKey()));
    owner1Entity.setProperty("bidirChildren", Utils.newArrayList(bi1.getKey(), bi2.getKey()));
    ds.put(owner1Entity);

    owner2Entity.setProperty("flights", Utils.newArrayList(fl3.getKey(), fl4.getKey()));
    owner2Entity.setProperty("bidirChildren", Utils.newArrayList(bi3.getKey(), bi4.getKey()));
    ds.put(owner2Entity);

    HasOneToManyListJDO parent1 = pm.getObjectById(HasOneToManyListJDO.class, k1);
    assertEquals(2, parent1.getFlights().size());
    assertEquals(2, parent1.getBidirChildren().size());
    HasOneToManyListJDO parent2 = pm.getObjectById(HasOneToManyListJDO.class, k2);
    assertEquals(2, parent2.getFlights().size());
    assertEquals(2, parent2.getBidirChildren().size());
    pm.deletePersistentAll(parent1, parent2);
    assertEquals(0, countForClass(HasOneToManyListJDO.class));
    assertEquals(0, countForClass(Flight.class));
    assertEquals(0, countForClass(BidirectionalChildListJDO.class));
    assertEquals(1, batchRecorder.batchOps);
  }

  public void testDeletePersistentAll_CascadeDelete_OneToMany_MultipleEntityGroups_Txn() {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    Entity owner1Entity = new Entity(HasOneToManyListJDO.class.getSimpleName());
    Key k1 = ds.put(owner1Entity);
    Entity owner2Entity = new Entity(HasOneToManyListJDO.class.getSimpleName());
    Key k2 = ds.put(owner2Entity);

    Entity fl1 = newFlightEntity(k1, 1);
    Entity fl2 = newFlightEntity(k1, 2);
    Entity fl3 = newFlightEntity(k2, 1);
    Entity fl4 = newFlightEntity(k2, 2);
    ds.put(fl1);
    ds.put(fl2);
    ds.put(fl3);
    ds.put(fl4);

    Entity bi1 = newBidirChildEntity(k1, 1);
    Entity bi2 = newBidirChildEntity(k1, 2);
    Entity bi3 = newBidirChildEntity(k2, 1);
    Entity bi4 = newBidirChildEntity(k2, 2);
    ds.put(bi1);
    ds.put(bi2);
    ds.put(bi3);
    ds.put(bi4);

    owner1Entity.setProperty("flights", Utils.newArrayList(fl1.getKey(), fl2.getKey()));
    owner1Entity.setProperty("bidirChildren", Utils.newArrayList(bi1.getKey(), bi2.getKey()));
    ds.put(owner1Entity);

    owner2Entity.setProperty("flights", Utils.newArrayList(fl3.getKey(), fl4.getKey()));
    owner2Entity.setProperty("bidirChildren", Utils.newArrayList(bi3.getKey(), bi4.getKey()));
    ds.put(owner2Entity);

    beginTxn();
    HasOneToManyListJDO parent1 = pm.getObjectById(HasOneToManyListJDO.class, k1);
    assertEquals(2, parent1.getFlights().size());
    assertEquals(2, parent1.getBidirChildren().size());
    commitTxn();
    beginTxn();
    HasOneToManyListJDO parent2 = pm.getObjectById(HasOneToManyListJDO.class, k2);
    assertEquals(2, parent2.getFlights().size());
    assertEquals(2, parent2.getBidirChildren().size());
    commitTxn();
    beginTxn();
    try {
      pm.deletePersistentAll(parent1, parent2);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
    }
    rollbackTxn();
    assertEquals(2, countForClass(HasOneToManyListJDO.class));
    assertEquals(4, countForClass(Flight.class));
    assertEquals(4, countForClass(BidirectionalChildListJDO.class));
    assertEquals(1, batchRecorder.batchOps);
    // cleanup outside tx
    pm.deletePersistentAll(parent1, parent2);
  }

  public void testDeletePersistentAll_CascadeDelete_OneToMany_OneEntityGroup_Txn() {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    Entity owner1Entity = new Entity(HasOneToManyListJDO.class.getSimpleName(), KeyFactory.createKey("yar", 43));
    Entity owner2Entity = new Entity(HasOneToManyListJDO.class.getSimpleName(), KeyFactory.createKey("yar", 43));
    Key k1 = ds.put(owner1Entity);
    Key k2 = ds.put(owner2Entity);

    Entity fl1 = newFlightEntity(k1, 1);
    Entity fl2 = newFlightEntity(k1, 2);
    Entity fl3 = newFlightEntity(k2, 1);
    Entity fl4 = newFlightEntity(k2, 2);
    ds.put(fl1);
    ds.put(fl2);
    ds.put(fl3);
    ds.put(fl4);

    Entity bi1 = newBidirChildEntity(k1, 1);
    Entity bi2 = newBidirChildEntity(k1, 2);
    Entity bi3 = newBidirChildEntity(k2, 1);
    Entity bi4 = newBidirChildEntity(k2, 2);
    ds.put(bi1);
    ds.put(bi2);
    ds.put(bi3);
    ds.put(bi4);

    owner1Entity.setProperty("flights", Utils.newArrayList(fl1.getKey(), fl2.getKey()));
    owner1Entity.setProperty("bidirChildren", Utils.newArrayList(bi1.getKey(), bi2.getKey()));
    ds.put(owner1Entity);

    owner2Entity.setProperty("flights", Utils.newArrayList(fl3.getKey(), fl4.getKey()));
    owner2Entity.setProperty("bidirChildren", Utils.newArrayList(bi3.getKey(), bi4.getKey()));
    ds.put(owner2Entity);

    beginTxn();
    HasOneToManyListJDO parent1 = pm.getObjectById(HasOneToManyListJDO.class, k1);
    assertEquals(2, parent1.getFlights().size());
    assertEquals(2, parent1.getBidirChildren().size());
    HasOneToManyListJDO parent2 = pm.getObjectById(HasOneToManyListJDO.class, k2);
    assertEquals(2, parent2.getFlights().size());
    assertEquals(2, parent2.getBidirChildren().size());
    pm.deletePersistentAll(parent1, parent2);
    commitTxn();

    assertEquals(0, countForClass(HasOneToManyListJDO.class));
    assertEquals(0, countForClass(Flight.class));
    assertEquals(0, countForClass(BidirectionalChildListJDO.class));
    assertEquals(1, batchRecorder.batchOps);
  }
}
