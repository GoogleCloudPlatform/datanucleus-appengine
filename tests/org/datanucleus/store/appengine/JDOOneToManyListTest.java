// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.BidirectionalChildListJDO;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.HasOneToManyWithOrderByJDO;
import org.datanucleus.test.HasOneToManyWithNonDeletingCascadeJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManyListTest extends JDOOneToManyTest {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    testInsert_NewParentAndChild(parent, bidirChild);
  }

  public void testInsertExistingParentNewChild() throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    testInsert_ExistingParentNewChild(parent, bidirChild);
  }

  public void testSwapAtPosition() throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    testSwapAtPosition(parent, bidirChild, bidirChild2);
  }

  public void testRemoveAtPosition() throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild3 = new BidirectionalChildListJDO();
    testRemoveAtPosition(parent, bidirChild, bidirChild2, bidirChild3);
  }

  public void testAddAtPosition() throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    testAddAtPosition(parent, bidirChild, bidirChild2);
  }

  public void testUpdateUpdateChildWithMerge() throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_UpdateChildWithMerge(pojo, bidir);
  }

  public void testUpdateUpdateChild() throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_UpdateChild(pojo, bidir);
  }

  public void testUpdateNullOutChildren() throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_NullOutChildren(pojo, bidir);
  }

  public void testUpdateClearOutChildren() throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_ClearOutChildren(pojo, bidir);
  }

  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    Flight f = newFlight();
    beginTxn();
    pm.makePersistent(f);
    commitTxn();
    HasOneToManyWithNonDeletingCascadeJDO pojo = new HasOneToManyWithNonDeletingCascadeJDO();
    pojo.getFlights().add(f);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertEquals(HasOneToManyWithNonDeletingCascadeJDO.class.getName(), 1,
                 countForClass(HasOneToManyWithNonDeletingCascadeJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));

    beginTxn();
    pojo.setFlights(null);
    pm.makePersistent(pojo);
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);

    assertEquals(HasOneToManyWithNonDeletingCascadeJDO.class.getName(), 1,
        countForClass(HasOneToManyWithNonDeletingCascadeJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));
  }

  public void testUpdate_ClearOutChild_NoDelete() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    Flight f = newFlight();
    beginTxn();
    pm.makePersistent(f);
    commitTxn();
    HasOneToManyWithNonDeletingCascadeJDO pojo = new HasOneToManyWithNonDeletingCascadeJDO();
    pojo.getFlights().add(f);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertEquals(HasOneToManyWithNonDeletingCascadeJDO.class.getName(), 1,
        countForClass(HasOneToManyWithNonDeletingCascadeJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));

    beginTxn();
    pojo.getFlights().clear();
    pm.makePersistent(pojo);
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals(HasOneToManyWithNonDeletingCascadeJDO.class.getName(), 1,
        countForClass(HasOneToManyWithNonDeletingCascadeJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));
  }

  public void testFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(HasOneToManyWithOrderByJDO.class);
  }

  public void testFind() throws EntityNotFoundException {
    testFind(HasOneToManyListJDO.class, BidirectionalChildListJDO.class);
  }

  public void testQuery() throws EntityNotFoundException {
    testQuery(HasOneToManyListJDO.class, BidirectionalChildListJDO.class);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyListJDO.class, BidirectionalChildListJDO.class);
  }

  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(HasOneToManyListJDO.class, BidirectionalChildListJDO.class);
  }

  public void testIndexOf() throws Exception {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir1 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidir2 = new BidirectionalChildListJDO();

    Flight f1 = newFlight();
    Flight f2 = newFlight();

    HasKeyPkJDO hasKeyPk1 = new HasKeyPkJDO();
    HasKeyPkJDO hasKeyPk2 = new HasKeyPkJDO();

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addFlight(f1);
    pojo.addFlight(f2);
    pojo.addHasKeyPk(hasKeyPk1);
    pojo.addHasKeyPk(hasKeyPk2);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(HasOneToManyListJDO.class, pojo.getId());
    assertEquals(0, pojo.getBidirChildren().indexOf(bidir1));
    assertEquals(1, pojo.getBidirChildren().indexOf(bidir2));
    assertEquals(0, pojo.getFlights().indexOf(f1));
    assertEquals(1, pojo.getFlights().indexOf(f2));
    assertEquals(0, pojo.getHasKeyPks().indexOf(hasKeyPk1));
    assertEquals(1, pojo.getHasKeyPks().indexOf(hasKeyPk2));
    commitTxn();
  }

  public void testRemoveAll() throws EntityNotFoundException {
    testRemoveAll(new HasOneToManyListJDO(), new BidirectionalChildListJDO(),
                  new BidirectionalChildListJDO(), new BidirectionalChildListJDO());
  }

  boolean isIndexed() {
    return true;
  }
}