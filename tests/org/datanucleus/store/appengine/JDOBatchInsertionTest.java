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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.test.BidirectionalChildListJDO;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyAncestorKeyPkJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.HasOneToOneJDO;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOBatchInsertionTest extends JDOTestCase {

  private BatchPutRecorder batchPutRecorder;

  private static Flight newFlight() {
    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");

    return f1;
  }

  private static final class BatchPutRecorder implements InvocationHandler {

    private final DatastoreService delegate;
    private int batchPuts = 0;

    public BatchPutRecorder(DatastoreService delegate) {
      this.delegate = delegate;
    }

    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
      if (method.getName().equals("put") && List.class.isAssignableFrom(method.getReturnType())) {
        batchPuts++;
      }
      try {
        return method.invoke(delegate, objects);
      } catch (InvocationTargetException ite) {
        throw ite.getTargetException();
      }
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    batchPutRecorder = new BatchPutRecorder(DatastoreServiceFactoryInternal.getDatastoreService());
    DatastoreService recordBatchPuts = (DatastoreService)
        Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] {DatastoreService.class}, batchPutRecorder);
    DatastoreServiceFactoryInternal.setDatastoreService(recordBatchPuts);
  }

  public void testMakePersistentAll_NoTxn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();
    Flight f2 = newFlight();
    pm.makePersistentAll(f1, f2);
    assertEquals(2, countForClass(Flight.class));
    assertEquals(1, batchPutRecorder.batchPuts);

    Flight f3 = newFlight();
    Flight f4 = newFlight();
    pm.makePersistentAll(Utils.newArrayList(f3, f4));
    assertEquals(4, countForClass(Flight.class));
    assertEquals(2, batchPutRecorder.batchPuts);
  }

  public void testMakePersistentAll_OneEntity_NoTxn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();
    pm.makePersistentAll(f1);
    assertEquals(1, countForClass(Flight.class));
    assertEquals(0, batchPutRecorder.batchPuts);

    Flight f2 = newFlight();
    pm.makePersistentAll(Utils.newArrayList(f2));
    assertEquals(2, countForClass(Flight.class));
    assertEquals(0, batchPutRecorder.batchPuts);
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
    assertEquals(1, batchPutRecorder.batchPuts);

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
    assertEquals(2, batchPutRecorder.batchPuts);
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
    assertEquals(1, batchPutRecorder.batchPuts);
  }

  public void testMakePersistentAll_OneEntity_Txn() {
    switchDatasource(JDOTestCase.PersistenceManagerFactoryName.transactional);
    Flight f1 = newFlight();
    beginTxn();
    pm.makePersistentAll(f1);
    commitTxn();
    assertEquals(1, countForClass(Flight.class));
    assertEquals(0, batchPutRecorder.batchPuts);

    Flight f2 = newFlight();
    beginTxn();
    pm.makePersistentAll(Utils.newArrayList(f2));
    commitTxn();
    assertEquals(2, countForClass(Flight.class));
    assertEquals(0, batchPutRecorder.batchPuts);
  }

  public void testMakePersistentAll_CascadeInsert_OneToOne_NoTxn() {
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
    assertEquals(1, batchPutRecorder.batchPuts);
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

  public void testMakePersistentAll_CascadeInsert_OneToOne_OneEntityGroup_Txn() {
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
    assertEquals(1, batchPutRecorder.batchPuts);
  }

  public void testMakePersistentAll_CascadeInsert_OneToMany_NoTxn() {
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
    parent1.getBidirChildren().add(bidirChild3);
    parent1.getBidirChildren().add(bidirChild4);
    pm.makePersistentAll(parent1, parent2);
    assertEquals(2, countForClass(HasOneToManyListJDO.class));
    assertEquals(4, countForClass(Flight.class));
    assertEquals(4, countForClass(BidirectionalChildListJDO.class));
    assertEquals(1, batchPutRecorder.batchPuts);
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
    parent1.getBidirChildren().add(bidirChild3);
    parent1.getBidirChildren().add(bidirChild4);
    beginTxn();
    try {
      pm.makePersistentAll(parent1, parent2);
      fail("expected exception");
    } catch (NucleusUserException nue) {
      // good
    }
  }

  public void testMakePersistentAll_CascadeInsert_OneToMany_OneEntityGroup_Txn() {
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
    parent1.getBidirChildren().add(bidirChild3);
    parent1.getBidirChildren().add(bidirChild4);
    beginTxn();
    pm.makePersistentAll(parent1, parent2);
    commitTxn();
    assertEquals(2, countForClass(HasOneToManyListJDO.class));
    assertEquals(4, countForClass(Flight.class));
    assertEquals(4, countForClass(BidirectionalChildListJDO.class));
    assertEquals(1, batchPutRecorder.batchPuts);
  }
}
