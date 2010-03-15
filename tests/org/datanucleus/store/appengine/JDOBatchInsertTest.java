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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.test.BidirectionalChildListJDO;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyAncestorKeyPkJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.HasOneToOneJDO;

import java.lang.reflect.Method;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOBatchInsertTest extends JDOBatchTestCase {

  private static Flight newFlight() {
    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");

    return f1;
  }

  BatchRecorder newBatchRecorder() {
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    return new BatchRecorder(config) {
      boolean isBatchMethod(Method method) {
        return method.getName().equals("put") && List.class.isAssignableFrom(method.getReturnType());
      }
    };
  }

  public void testMakePersistentAll_NoTxn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    pm.makePersistentAll(f1, f2);
    assertEquals(2, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);

    Flight f3 = newFlight();
    Flight f4 = newFlight();
    pm.makePersistentAll(Utils.newArrayList(f3, f4));
    assertEquals(4, countForClass(Flight.class));
    assertEquals(2, batchRecorder.batchOps);
  }

  public void testMakePersistentAll_OneEntity_NoTxn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();
    pm.makePersistentAll(f1);
    assertEquals(1, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);

    Flight f2 = newFlight();
    pm.makePersistentAll(Utils.newArrayList(f2));
    assertEquals(2, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);
  }

  public void testMakePersistentAll_Txn_MultipleEntityGroups() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    beginTxn();
    try {
      pm.makePersistentAll(f1, f2);
      fail("expected iae");
    } catch (NucleusUserException nue) {
      // good
    }
    rollbackTxn();
    assertEquals(0, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);

    Flight f3 = newFlight();
    Flight f4 = newFlight();
    beginTxn();
    try {
      pm.makePersistentAll(Utils.newArrayList(f3, f4));
      fail("expected iae");
    } catch (NucleusUserException nue) {
      // good
    }
    assertEquals(0, countForClass(Flight.class));
    assertEquals(2, batchRecorder.batchOps);
  }

  public void testMakePersistentAll_Txn_OneEntityGroup() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Key parentKey = KeyFactory.createKey("yar", 24);
    HasKeyAncestorKeyPkJDO pojo1 = new HasKeyAncestorKeyPkJDO();
    pojo1.setAncestorKey(parentKey);
    HasKeyAncestorKeyPkJDO pojo2 = new HasKeyAncestorKeyPkJDO();
    pojo2.setAncestorKey(parentKey);
    beginTxn();
    pm.makePersistentAll(pojo1, pojo2);
    commitTxn();
    assertEquals(2, countForClass(HasKeyAncestorKeyPkJDO.class));
    assertEquals(1, batchRecorder.batchOps);
  }

  public void testMakePersistentAll_OneEntity_Txn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Flight f1 = newFlight();
    beginTxn();
    pm.makePersistentAll(f1);
    commitTxn();
    assertEquals(1, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);

    Flight f2 = newFlight();
    beginTxn();
    pm.makePersistentAll(Utils.newArrayList(f2));
    commitTxn();
    assertEquals(2, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);
  }

  public void testMakePersistentAll_CascadeInsert_OneToOne_NoTxn() throws EntityNotFoundException {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    HasOneToOneJDO parent1 = new HasOneToOneJDO();
    parent1.setFlight(f1);
    HasOneToOneJDO parent2 = new HasOneToOneJDO();
    parent2.setFlight(f2);
    pm.makePersistentAll(parent1, parent2);
    assertEquals(2, countForClass(HasOneToOneJDO.class));
    assertEquals(2, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);

    Entity parent1Entity = ldth.ds.get(KeyFactory.stringToKey(parent1.getId()));
    Entity flight1Entity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    assertEquals(6, parent1Entity.getProperties().size());
    assertTrue(parent1Entity.hasProperty("hasKeyPK_key_OID"));
    assertNull(parent1Entity.getProperty("hasKeyPK_key_OID"));
    assertTrue(parent1Entity.hasProperty("hasParent_key_OID"));
    assertNull(parent1Entity.getProperty("hasParent_key_OID"));
    assertTrue(parent1Entity.hasProperty("hasParentKeyPK_key_OID"));
    assertNull(parent1Entity.getProperty("hasParentKeyPK_key_OID"));
    assertTrue(parent1Entity.hasProperty("notDependent_id_OID"));
    assertNull(parent1Entity.getProperty("notDependent_id_OID"));
    assertEquals(flight1Entity.getKey(), parent1Entity.getProperty("flight_id_OID"));
    Entity parent2Entity = ldth.ds.get(KeyFactory.stringToKey(parent2.getId()));
    Entity flight2Entity = ldth.ds.get(KeyFactory.stringToKey(f2.getId()));
    assertEquals(6, parent2Entity.getProperties().size());
    assertTrue(parent2Entity.hasProperty("hasKeyPK_key_OID"));
    assertNull(parent2Entity.getProperty("hasKeyPK_key_OID"));
    assertTrue(parent2Entity.hasProperty("hasParent_key_OID"));
    assertNull(parent2Entity.getProperty("hasParent_key_OID"));
    assertTrue(parent2Entity.hasProperty("hasParentKeyPK_key_OID"));
    assertNull(parent2Entity.getProperty("hasParentKeyPK_key_OID"));
    assertTrue(parent2Entity.hasProperty("notDependent_id_OID"));
    assertNull(parent2Entity.getProperty("notDependent_id_OID"));
    assertEquals(flight2Entity.getKey(), parent2Entity.getProperty("flight_id_OID"));
  }

  public void testMakePersistentAll_CascadeInsert_OneToOne_MultipleEntityGroups_Txn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    HasOneToOneJDO parent1 = new HasOneToOneJDO();
    parent1.setFlight(f1);
    HasOneToOneJDO parent2 = new HasOneToOneJDO();
    parent2.setFlight(f2);
    beginTxn();
    try {
      pm.makePersistentAll(parent1, parent2);
      fail("expected exception");
    } catch (NucleusUserException nue) {
      // good
    }
  }

  public void testMakePersistentAll_CascadeInsert_OneToOne_OneEntityGroup_Txn()
      throws EntityNotFoundException {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    HasOneToOneJDO parent1 = new HasOneToOneJDO();
    parent1.setId(new KeyFactory.Builder("Yar", 43).addChild(HasOneToOneJDO.class.getSimpleName(), "k1").getString());
    parent1.setFlight(f1);
    HasOneToOneJDO parent2 = new HasOneToOneJDO();
    parent2.setId(new KeyFactory.Builder("Yar", 43).addChild(HasOneToOneJDO.class.getSimpleName(), "k2").getString());
    parent2.setFlight(f2);
    beginTxn();
    pm.makePersistentAll(parent1, parent2);
    commitTxn();
    assertEquals(2, countForClass(HasOneToOneJDO.class));
    assertEquals(2, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);

    Entity parent1Entity = ldth.ds.get(KeyFactory.stringToKey(parent1.getId()));
    Entity flight1Entity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    assertEquals(6, parent1Entity.getProperties().size());
    assertTrue(parent1Entity.hasProperty("hasKeyPK_key_OID"));
    assertNull(parent1Entity.getProperty("hasKeyPK_key_OID"));
    assertTrue(parent1Entity.hasProperty("hasParent_key_OID"));
    assertNull(parent1Entity.getProperty("hasParent_key_OID"));
    assertTrue(parent1Entity.hasProperty("hasParentKeyPK_key_OID"));
    assertNull(parent1Entity.getProperty("hasParentKeyPK_key_OID"));
    assertTrue(parent1Entity.hasProperty("notDependent_id_OID"));
    assertNull(parent1Entity.getProperty("notDependent_id_OID"));
    assertEquals(flight1Entity.getKey(), parent1Entity.getProperty("flight_id_OID"));
    Entity parent2Entity = ldth.ds.get(KeyFactory.stringToKey(parent2.getId()));
    Entity flight2Entity = ldth.ds.get(KeyFactory.stringToKey(f2.getId()));
    assertEquals(6, parent2Entity.getProperties().size());
    assertTrue(parent2Entity.hasProperty("hasKeyPK_key_OID"));
    assertNull(parent2Entity.getProperty("hasKeyPK_key_OID"));
    assertTrue(parent2Entity.hasProperty("hasParent_key_OID"));
    assertNull(parent2Entity.getProperty("hasParent_key_OID"));
    assertTrue(parent2Entity.hasProperty("hasParentKeyPK_key_OID"));
    assertNull(parent2Entity.getProperty("hasParentKeyPK_key_OID"));
    assertTrue(parent2Entity.hasProperty("notDependent_id_OID"));
    assertNull(parent2Entity.getProperty("notDependent_id_OID"));
    assertEquals(flight2Entity.getKey(), parent2Entity.getProperty("flight_id_OID"));
  }

  public void testMakePersistentAll_CascadeInsert_OneToMany_NoTxn() throws EntityNotFoundException {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    Flight f3 = newFlight();
    Flight f4 = newFlight();
    BidirectionalChildListJDO bidirChild1 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild3 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild4 = new BidirectionalChildListJDO();
    HasOneToManyListJDO parent1 = new HasOneToManyListJDO();
    parent1.getFlights().add(f1);
    parent1.getFlights().add(f2);
    parent1.getBidirChildren().add(bidirChild1);
    parent1.getBidirChildren().add(bidirChild2);
    HasOneToManyListJDO parent2 = new HasOneToManyListJDO();
    parent2.getFlights().add(f3);
    parent2.getFlights().add(f4);
    parent2.getBidirChildren().add(bidirChild3);
    parent2.getBidirChildren().add(bidirChild4);
    pm.makePersistentAll(parent1, parent2);
    assertEquals(2, countForClass(HasOneToManyListJDO.class));
    assertEquals(4, countForClass(Flight.class));
    assertEquals(4, countForClass(BidirectionalChildListJDO.class));
    assertEquals(1, batchRecorder.batchOps);

    Entity parent1Entity = ldth.ds.get(KeyFactory.stringToKey(parent1.getId()));
    Entity parent2Entity = ldth.ds.get(KeyFactory.stringToKey(parent2.getId()));
    Entity flight1Entity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flight2Entity = ldth.ds.get(KeyFactory.stringToKey(f2.getId()));
    Entity flight3Entity = ldth.ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity flight4Entity = ldth.ds.get(KeyFactory.stringToKey(f4.getId()));
    Entity bidir1Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild1.getId()));
    Entity bidir2Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild2.getId()));
    Entity bidir3Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild3.getId()));
    Entity bidir4Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild4.getId()));
    assertEquals(4, parent1Entity.getProperties().size());
    assertTrue(parent1Entity.hasProperty("hasKeyPks"));
    assertNull(parent1Entity.getProperty("hasKeyPks"));
    assertEquals(
        Utils.newArrayList(flight1Entity.getKey(), flight2Entity.getKey()),
        parent1Entity.getProperty("flights"));
    assertEquals(
        Utils.newArrayList(bidir1Entity.getKey(), bidir2Entity.getKey()),
        parent1Entity.getProperty("bidirChildren"));

    assertEquals(4, parent2Entity.getProperties().size());
    assertTrue(parent2Entity.hasProperty("hasKeyPks"));
    assertNull(parent2Entity.getProperty("hasKeyPks"));
    assertEquals(
        Utils.newArrayList(flight3Entity.getKey(), flight4Entity.getKey()),
        parent2Entity.getProperty("flights"));
    assertEquals(
        Utils.newArrayList(bidir3Entity.getKey(), bidir4Entity.getKey()),
        parent2Entity.getProperty("bidirChildren"));
  }

  public void testMakePersistentAll_CascadeInsert_OneToMany_MultipleEntityGroups_Txn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    Flight f3 = newFlight();
    Flight f4 = newFlight();
    BidirectionalChildListJDO bidirChild1 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild3 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild4 = new BidirectionalChildListJDO();
    HasOneToManyListJDO parent1 = new HasOneToManyListJDO();
    parent1.getFlights().add(f1);
    parent1.getFlights().add(f2);
    parent1.getBidirChildren().add(bidirChild1);
    parent1.getBidirChildren().add(bidirChild2);
    HasOneToManyListJDO parent2 = new HasOneToManyListJDO();
    parent2.getFlights().add(f3);
    parent2.getFlights().add(f4);
    parent2.getBidirChildren().add(bidirChild3);
    parent2.getBidirChildren().add(bidirChild4);
    beginTxn();
    try {
      pm.makePersistentAll(parent1, parent2);
      fail("expected exception");
    } catch (NucleusUserException nue) {
      // good
    }
  }

  public void testMakePersistentAll_CascadeInsert_OneToMany_OneEntityGroup_Txn()
      throws EntityNotFoundException {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    Flight f3 = newFlight();
    Flight f4 = newFlight();
    BidirectionalChildListJDO bidirChild1 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild3 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild4 = new BidirectionalChildListJDO();
    HasOneToManyListJDO parent1 = new HasOneToManyListJDO();
    parent1.setId(new KeyFactory.Builder("Yar", 43).addChild(HasOneToManyListJDO.class.getSimpleName(), "k1").getString());
    parent1.getFlights().add(f1);
    parent1.getFlights().add(f2);
    parent1.getBidirChildren().add(bidirChild1);
    parent1.getBidirChildren().add(bidirChild2);
    HasOneToManyListJDO parent2 = new HasOneToManyListJDO();
    parent2.setId(new KeyFactory.Builder("Yar", 43).addChild(HasOneToManyListJDO.class.getSimpleName(), "k2").getString());
    parent2.getFlights().add(f3);
    parent2.getFlights().add(f4);
    parent2.getBidirChildren().add(bidirChild3);
    parent2.getBidirChildren().add(bidirChild4);
    beginTxn();
    pm.makePersistentAll(parent1, parent2);
    commitTxn();
    assertEquals(2, countForClass(HasOneToManyListJDO.class));
    assertEquals(4, countForClass(Flight.class));
    assertEquals(4, countForClass(BidirectionalChildListJDO.class));
    assertEquals(1, batchRecorder.batchOps);

    Entity parent1Entity = ldth.ds.get(KeyFactory.stringToKey(parent1.getId()));
    Entity parent2Entity = ldth.ds.get(KeyFactory.stringToKey(parent2.getId()));
    Entity flight1Entity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flight2Entity = ldth.ds.get(KeyFactory.stringToKey(f2.getId()));
    Entity flight3Entity = ldth.ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity flight4Entity = ldth.ds.get(KeyFactory.stringToKey(f4.getId()));
    Entity bidir1Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild1.getId()));
    Entity bidir2Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild2.getId()));
    Entity bidir3Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild3.getId()));
    Entity bidir4Entity = ldth.ds.get(KeyFactory.stringToKey(bidirChild4.getId()));
    assertEquals(4, parent1Entity.getProperties().size());
    assertTrue(parent1Entity.hasProperty("hasKeyPks"));
    assertNull(parent1Entity.getProperty("hasKeyPks"));
    assertEquals(
        Utils.newArrayList(flight1Entity.getKey(), flight2Entity.getKey()),
        parent1Entity.getProperty("flights"));
    assertEquals(
        Utils.newArrayList(bidir1Entity.getKey(), bidir2Entity.getKey()),
        parent1Entity.getProperty("bidirChildren"));

    assertEquals(4, parent2Entity.getProperties().size());
    assertTrue(parent2Entity.hasProperty("hasKeyPks"));
    assertNull(parent2Entity.getProperty("hasKeyPks"));
    assertEquals(
        Utils.newArrayList(flight3Entity.getKey(), flight4Entity.getKey()),
        parent2Entity.getProperty("flights"));
    assertEquals(
        Utils.newArrayList(bidir3Entity.getKey(), bidir4Entity.getKey()),
        parent2Entity.getProperty("bidirChildren"));
  }

  public void testCombineInsertAndUpdate_NoTxn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();
    pm.makePersistent(f1);
    f1 = pm.detachCopy(f1);
    assertEquals(1, countForClass(Flight.class));
    assertEquals(0, batchRecorder.batchOps);
    pm.close();
    assertEquals(ObjectState.DETACHED_CLEAN, JDOHelper.getObjectState(f1));
    f1.setName("jimmy");
    pm = pmf.getPersistenceManager();
    Flight f2 = new Flight();
    Flight f3 = new Flight();
    pm.makePersistentAll(f1, f2, f3);
    pm.close();
    assertEquals(3, countForClass(Flight.class));
    assertEquals(1, batchRecorder.batchOps);
  }
}
