// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.BidirectionalChildJDO;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasOneToManyJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.HasOneToManyWithOrderByJDOInterface;
import org.easymock.EasyMock;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
abstract class JDOOneToManyTest extends JDOTestCase {

  void testInsert_NewParentAndChild(HasOneToManyJDO parent,
      BidirectionalChildJDO bidirChild) throws EntityNotFoundException {
    bidirChild.setChildVal("yam");

    Flight f = newFlight();

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");

    parent.addBidirChild(bidirChild);
    bidirChild.setParent(parent);
    parent.addFlight(f);
    parent.addHasKeyPk(hasKeyPk);
    parent.setVal("yar");

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    assertNotNull(bidirChild.getId());
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(parent.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(parent.getClass(), bidirChild.getClass(), 1, 1);
  }

  void testInsert_ExistingParentNewChild(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidirChild) throws EntityNotFoundException {
    pojo.setVal("yar");

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    assertNotNull(pojo.getId());
    assertTrue(pojo.getFlights().isEmpty());
    assertTrue(pojo.getHasKeyPks().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());
    commitTxn();
    beginTxn();
    Flight f = newFlight();
    pojo.addFlight(f);

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");
    pojo.addHasKeyPk(hasKeyPk);

    bidirChild.setChildVal("yam");
    pojo.addBidirChild(bidirChild);

    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    commitTxn();
    
    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(pojo.getClass(), bidirChild.getClass(), 1, 1);
  }

  void testSwapAtPosition(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidir1,
      BidirectionalChildJDO bidir2) throws EntityNotFoundException {
    pojo.setVal("yar");
    bidir2.setChildVal("yam");
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir1);
    bidir1.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 1);

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String flight1Id = pojo.getFlights().iterator().next().getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().iterator().next().getKey();
    pojo.addBidirChildAtPosition(bidir2, 0);

    Flight f2 = newFlight();
    f2.setName("another name");
    pojo.addFlightAtPosition(f2, 0);

    HasKeyPkJDO hasKeyPk2 = new HasKeyPkJDO();
    hasKeyPk2.setStr("another str");
    pojo.addHasKeyPkAtPosition(hasKeyPk2, 0);
    commitTxn();

    beginTxn();
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getFlights().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(f2.getId());
    assertNotNull(hasKeyPk2.getKey());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(flight1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f2.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f2.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk2.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(pojo.getClass(), bidir2.getClass(), 1, 1);
  }

  void testRemoveAtPosition(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidir1,
      BidirectionalChildJDO bidir2,
      BidirectionalChildJDO bidir3) throws EntityNotFoundException {
    pojo.setVal("yar");
    bidir2.setChildVal("another yam");
    bidir3.setChildVal("yet another yam");
    Flight f = newFlight();
    Flight f2 = newFlight();
    Flight f3 = newFlight();
    f2.setName("another name");
    f3.setName("yet another name");
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasKeyPkJDO hasKeyPk2 = new HasKeyPkJDO();
    HasKeyPkJDO hasKeyPk3 = new HasKeyPkJDO();
    hasKeyPk2.setStr("another str");
    hasKeyPk3.setStr("yet another str");

    pojo.addFlight(f);
    pojo.addFlight(f2);
    pojo.addFlight(f3);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addHasKeyPk(hasKeyPk2);
    pojo.addHasKeyPk(hasKeyPk3);
    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 3);

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String flight1Id = pojo.getFlights().iterator().next().getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().iterator().next().getKey();
    pojo.removeBidirChildAtPosition(0);
    pojo.removeFlightAtPosition(0);
    pojo.removeHasKeyPkAtPosition(0);
    commitTxn();

    beginTxn();
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getFlights().size());
    assertEquals(2, pojo.getHasKeyPks().size());
    assertEquals(2, pojo.getBidirChildren().size());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(flight1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("another yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidir3.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yet another yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir3.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(1L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f2.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f2.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    flightEntity = ldth.ds.get(KeyFactory.stringToKey(f3.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("yet another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f3.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(1L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk2.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    hasKeyPkEntity = ldth.ds.get(hasKeyPk3.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yet another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk3.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(1L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(pojo.getClass(), bidir2.getClass(), 1, 2);
  }

  void testAddAtPosition(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidir1,
      BidirectionalChildJDO bidir2) throws EntityNotFoundException {
    pojo.setVal("yar");
    bidir2.setChildVal("yam");
    Flight f = newFlight();
    Flight f2 = newFlight();
    f2.setName("another name");
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasKeyPkJDO hasKeyPk2 = new HasKeyPkJDO();
    hasKeyPk2.setStr("another str");

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir1);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 1);

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String flight1Id = pojo.getFlights().iterator().next().getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().iterator().next().getKey();
    pojo.addAtPosition(0, bidir2);
    pojo.addAtPosition(0, f2);
    pojo.addAtPosition(0, hasKeyPk2);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getFlights().size());
    assertEquals(2, pojo.getHasKeyPks().size());
    assertEquals(2, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(f2.getId());
    assertNotNull(hasKeyPk2.getKey());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    commitTxn();

    ldth.ds.get(KeyFactory.stringToKey(bidir1Id));
    ldth.ds.get(KeyFactory.stringToKey(flight1Id));
    ldth.ds.get(hasKeyPk1Key);
    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f2.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f2.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk2.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(pojo.getClass(), bidir2.getClass(), 1, 2);
  }

  void testUpdate_UpdateChildWithMerge(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidir) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    beginTxn();
    f.setOrigin("yam");
    hasKeyPk.setStr("yar");
    bidir.setChildVal("yap");
    pm.makePersistent(pojo);
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_UpdateChild(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidir) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    pojo.getFlights().iterator().next().setOrigin("yam");
    pojo.getHasKeyPks().iterator().next().setStr("yar");
    pojo.getBidirChildren().iterator().next().setChildVal("yap");
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_NullOutChildren(HasOneToManyJDO pojo,
    BidirectionalChildJDO bidir) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    beginTxn();
    String flightId = f.getId();
    Key hasKeyPkKey = hasKeyPk.getKey();
    String bidirChildId = bidir.getId();

    pojo.nullFlights();
    pojo.nullHasKeyPks();
    pojo.nullBidirChildren();
    pm.makePersistent(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(flightId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasKeyPkKey);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testUpdate_ClearOutChildren(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidir) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    String flightId = f.getId();
    Key hasKeyPkId = hasKeyPk.getKey();
    String bidirChildId = bidir.getId();
    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    beginTxn();
    pojo.clearFlights();
    pojo.clearHasKeyPks();
    pojo.clearBidirChildren();
    pm.makePersistent(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(flightId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasKeyPkId);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testFindWithOrderBy(Class<? extends HasOneToManyWithOrderByJDOInterface> pojoClass)
      throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity1 = newFlightEntity(pojoEntity.getKey(), "bos1", "mia2", "name 1");
    flightEntity1.setProperty("flightsByOrigAndDest_INTEGER_IDX", 0);
    flightEntity1.setProperty("flightsByIdAndOrig_INTEGER_IDX", 0);
    flightEntity1.setProperty("flightsByOrigAndId_INTEGER_IDX", 0);
    ldth.ds.put(flightEntity1);

    Entity flightEntity2 = newFlightEntity(pojoEntity.getKey(), "bos2", "mia2", "name 2");
    flightEntity2.setProperty("flightsByOrigAndDest_INTEGER_IDX", 1);
    flightEntity2.setProperty("flightsByIdAndOrig_INTEGER_IDX", 1);
    flightEntity2.setProperty("flightsByOrigAndId_INTEGER_IDX", 1);
    ldth.ds.put(flightEntity2);

    Entity flightEntity3 = newFlightEntity(pojoEntity.getKey(), "bos1", "mia1", "name 0");
    flightEntity3.setProperty("flightsByOrigAndDest_INTEGER_IDX", 2);
    flightEntity3.setProperty("flightsByIdAndOrig_INTEGER_IDX", 2);
    flightEntity3.setProperty("flightsByOrigAndId_INTEGER_IDX", 2);
    ldth.ds.put(flightEntity3);

    beginTxn();
    HasOneToManyWithOrderByJDOInterface pojo = pm.getObjectById(
        pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getFlightsByOrigAndDest());
    assertEquals(3, pojo.getFlightsByOrigAndDest().size());
    assertEquals("name 2", pojo.getFlightsByOrigAndDest().get(0).getName());
    assertEquals("name 0", pojo.getFlightsByOrigAndDest().get(1).getName());
    assertEquals("name 1", pojo.getFlightsByOrigAndDest().get(2).getName());

    assertNotNull(pojo.getFlightsByIdAndOrig());
    assertEquals(3, pojo.getFlightsByIdAndOrig().size());
    assertEquals("name 0", pojo.getFlightsByIdAndOrig().get(0).getName());
    assertEquals("name 2", pojo.getFlightsByIdAndOrig().get(1).getName());
    assertEquals("name 1", pojo.getFlightsByIdAndOrig().get(2).getName());

    assertNotNull(pojo.getFlightsByOrigAndId());
    assertEquals(3, pojo.getFlightsByOrigAndId().size());
    assertEquals("name 2", pojo.getFlightsByOrigAndId().get(0).getName());
    assertEquals("name 1", pojo.getFlightsByOrigAndId().get(1).getName());
    assertEquals("name 0", pojo.getFlightsByOrigAndId().get(2).getName());

    commitTxn();
  }

  void testFind(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos1", "mia2", "name 1");
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ldth.ds.put(bidirEntity);

    beginTxn();
    HasOneToManyJDO pojo =
        pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getFlights());
    assertEquals(1, pojo.getFlights().size());
    assertEquals("bos1", pojo.getFlights().iterator().next().getOrigin());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().iterator().next().getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    commitTxn();
  }

  void testQuery(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia2", "name");
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ldth.ds.put(bidirEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + pojoClass.getName() + " where id == key parameters String key");
    beginTxn();
    @SuppressWarnings("unchecked")
    List<HasOneToManyListJDO> result =
        (List<HasOneToManyListJDO>) q.execute(KeyFactory.keyToString(pojoEntity.getKey()));
    assertEquals(1, result.size());
    HasOneToManyJDO pojo = result.get(0);
    assertNotNull(pojo.getFlights());
    assertEquals(1, pojo.getFlights().size());
    assertEquals("bos", pojo.getFlights().iterator().next().getOrigin());
    assertEquals(1, pojo.getFlights().size());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().iterator().next().getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    commitTxn();
  }

  void testChildFetchedLazily(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass) throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    try {
      setUp();

      Entity pojoEntity = new Entity(pojoClass.getSimpleName());
      ldth.ds.put(pojoEntity);

      Entity FlightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia", "name");
      ldth.ds.put(FlightEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
      bidirEntity.setProperty("childVal", "yap");
      ldth.ds.put(bidirEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(ds.beginTransaction()).andReturn(txn);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(ds.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      beginTxn();
      HasOneToManyJDO pojo =
          pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  void testDeleteParentDeletesChild(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass) throws Exception {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia", "name");
    flightEntity.setProperty("flights_INTEGER_IDX", 1);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ldth.ds.put(bidirEntity);

    beginTxn();
    HasOneToManyJDO pojo = pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    pm.deletePersistent(pojo);
    commitTxn();
    assertCountsInDatastore(pojoClass, bidirClass, 0, 0);
  }

  int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(new Query(clazz.getSimpleName())).countEntities();
  }

  void assertCountsInDatastore(Class<? extends HasOneToManyJDO> parentClass,
      Class<? extends BidirectionalChildJDO> bidirClass,
      int expectedParent, int expectedChildren) {
    assertEquals(parentClass.getName(), expectedParent, countForClass(parentClass));
    assertEquals(bidirClass.getName(), expectedChildren, countForClass(bidirClass));
    assertEquals(
        Flight.class.getName(), expectedChildren, countForClass(Flight.class));
    assertEquals(
        HasKeyPkJDO.class.getName(), expectedChildren, countForClass(HasKeyPkJDO.class));
  }

  Flight newFlight() {
    Flight f = new Flight();
    f.setOrigin("bos");
    f.setDest("mia");
    f.setName("jimmy");
    f.setMe(22);
    f.setYou(26);
    f.setFlightNumber(99);
    return f;
  }

  Entity newFlightEntity(
      Key parentKey, String orig, String dest, String name) {
    Entity entity = new Entity(Flight.class.getSimpleName(), parentKey);
    entity.setProperty("origin", orig);
    entity.setProperty("dest", dest);
    entity.setProperty("name", name);
    entity.setProperty("you", 44);
    entity.setProperty("me", 45);
    entity.setProperty("flight_number", 99);
    entity.setProperty("flights_INTEGER_IDX", 1);
    return entity;
  }

  abstract boolean isIndexed();
}
