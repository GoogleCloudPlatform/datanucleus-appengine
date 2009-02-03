// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.BidirectionalChildSetJDO;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasOneToManySetJDO;
import org.datanucleus.test.HasOneToManyWithNonDeletingCascadeJDO;
import org.datanucleus.test.HasOneToManyWithOrderByJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManySetTest extends JDOOneToManyTest {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    HasOneToManySetJDO parent = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidirChild = new BidirectionalChildSetJDO();
    testInsert_NewParentAndChild(parent, bidirChild);
  }

  public void testInsertExistingParentNewChild() throws EntityNotFoundException {
    HasOneToManySetJDO parent = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidirChild = new BidirectionalChildSetJDO();
    testInsert_ExistingParentNewChild(parent, bidirChild);
  }

  public void testUpdateUpdateChildWithMerge() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_UpdateChildWithMerge(pojo, bidir);
  }

  public void testUpdateUpdateChild() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_UpdateChild(pojo, bidir);
  }

  public void testUpdateNullOutChildren() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_NullOutChildren(pojo, bidir);
  }

  public void testUpdateClearOutChildren() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_ClearOutChildren(pojo, bidir);
  }

  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
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

    Entity FlightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(FlightEntity);

    assertEquals(HasOneToManyWithNonDeletingCascadeJDO.class.getName(), 1,
        countForClass(HasOneToManyWithNonDeletingCascadeJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));
  }

  public void testUpdate_ClearOutChild_NoDelete() throws EntityNotFoundException {
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

    Entity FlightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(FlightEntity);
    assertEquals(HasOneToManyWithNonDeletingCascadeJDO.class.getName(), 1,
        countForClass(HasOneToManyWithNonDeletingCascadeJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));
  }

  public void testFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(HasOneToManyWithOrderByJDO.class);
  }

  public void testFind() throws EntityNotFoundException {
    testFind(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  public void testQuery() throws EntityNotFoundException {
    testQuery(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  boolean isIndexed() {
    return false;
  }
}