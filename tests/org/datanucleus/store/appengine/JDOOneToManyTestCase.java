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
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.BidirectionalChildJDO;
import org.datanucleus.test.BidirectionalChildLongPkJDO;
import org.datanucleus.test.BidirectionalChildUnencodedStringPkJDO;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasExplicitIndexColumnJDO;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasOneToManyJDO;
import org.datanucleus.test.HasOneToManyKeyPkJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.HasOneToManyLongPkJDO;
import org.datanucleus.test.HasOneToManyUnencodedStringPkJDO;
import org.datanucleus.test.HasOneToManyWithOrderByJDO;
import org.easymock.EasyMock;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;

/**
 * @author Max Ross <maxr@google.com>
 */
abstract class JDOOneToManyTestCase extends JDOTestCase {

  @Override
  protected void tearDown() throws Exception {
    if (pm.currentTransaction().isActive() && failed) {
      pm.currentTransaction().rollback();
    }
    pmf.close();
    super.tearDown();
  }

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
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(flightEntity.getKey()), parentEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("flights"));
    assertNull(pojoEntity.getProperty("flights"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(flightEntity.getKey()), parentEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

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
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
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
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(f2.getId())), pojoEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(hasKeyPk2.getKey()), pojoEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(flightEntity.getKey()), parentEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

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
    assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(pojo));
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
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir3.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(f2.getId()),
        KeyFactory.stringToKey(f3.getId())), pojoEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(
        hasKeyPk2.getKey(),
        hasKeyPk3.getKey()), pojoEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir3.getId())), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(f2.getId()),
        KeyFactory.stringToKey(f3.getId())), parentEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(
        hasKeyPk2.getKey(),
        hasKeyPk3.getKey()), parentEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir1.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(f2.getId()),
        KeyFactory.stringToKey(f.getId())), pojoEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(
        hasKeyPk2.getKey(),
        hasKeyPk.getKey()), pojoEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(bidir2.getId()),
        KeyFactory.stringToKey(bidir1.getId())), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(
        KeyFactory.stringToKey(f2.getId()),
        KeyFactory.stringToKey(f.getId())), parentEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(
        hasKeyPk2.getKey(),
        hasKeyPk.getKey()), parentEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("flights"));
    assertNull(pojoEntity.getProperty("flights"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

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
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("flights"));
    assertNull(pojoEntity.getProperty("flights"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testFindWithOrderBy(Class<? extends HasOneToManyWithOrderByJDO> pojoClass)
      throws EntityNotFoundException {
    getObjectManager().getOMFContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.allowMultipleRelationsOfSameType", true);
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

    Entity explicitIndexEntity1 =
        new Entity(HasExplicitIndexColumnJDO.class.getSimpleName(), pojoEntity.getKey());
    explicitIndexEntity1.setProperty("index", 3);
    ldth.ds.put(explicitIndexEntity1);

    Entity explicitIndexEntity2 =
        new Entity(HasExplicitIndexColumnJDO.class.getSimpleName(), pojoEntity.getKey());
    explicitIndexEntity2.setProperty("index", 2);
    ldth.ds.put(explicitIndexEntity2);

    Entity explicitIndexEntity3 =
        new Entity(HasExplicitIndexColumnJDO.class.getSimpleName(), pojoEntity.getKey());
    explicitIndexEntity3.setProperty("index", 1);
    ldth.ds.put(explicitIndexEntity3);

    beginTxn();
    HasOneToManyWithOrderByJDO pojo = pm.getObjectById(
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

    assertNotNull(pojo.getHasIndexColumn());
    assertEquals(3, pojo.getHasIndexColumn().size());
    assertEquals(explicitIndexEntity3.getKey(), pojo.getHasIndexColumn().get(0).getId());
    assertEquals(explicitIndexEntity2.getKey(), pojo.getHasIndexColumn().get(1).getId());
    assertEquals(explicitIndexEntity1.getKey(), pojo.getHasIndexColumn().get(2).getId());

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
      EasyMock.expect(txn.getId()).andReturn("1").times(2);
      txn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(txn);
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

  void testRemoveAll(HasOneToManyJDO pojo, BidirectionalChildJDO bidir1,
                     BidirectionalChildJDO bidir2, BidirectionalChildJDO bidir3)
      throws EntityNotFoundException {
    Flight f1 = new Flight();
    Flight f2 = new Flight();
    Flight f3 = new Flight();
    pojo.addFlight(f1);
    pojo.addFlight(f2);
    pojo.addFlight(f3);

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    String f2Id = f2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeFlights(Collections.singleton(f2));
    assertFalse(pojo.getFlights().contains(f2));
    assertTrue(pojo.getFlights().contains(f1));
    assertTrue(pojo.getFlights().contains(f3));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    assertFalse(pojo.getBidirChildren().contains(bidir2));
    assertTrue(pojo.getBidirChildren().contains(bidir1));
    assertTrue(pojo.getBidirChildren().contains(bidir3));
    commitTxn();
    beginTxn();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());

    assertEquals(2, pojo.getFlights().size());
    assertFalse(pojo.getFlights().contains(f2));
    assertTrue(pojo.getFlights().contains(f1));
    assertTrue(pojo.getFlights().contains(f3));

    assertEquals(2, pojo.getBidirChildren().size());
    assertFalse(pojo.getBidirChildren().contains(bidir2));
    assertTrue(pojo.getBidirChildren().contains(bidir1));
    assertTrue(pojo.getBidirChildren().contains(bidir3));
    commitTxn();

    Entity flightEntity1 = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flightEntity3 = ldth.ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity bidirEntity1 = ldth.ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ldth.ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, flightEntity1.getProperty("flights_INTEGER_IDX"));
      assertEquals(1L, flightEntity3.getProperty("flights_INTEGER_IDX"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  void testRemoveAll_LongPkOnParent(HasOneToManyLongPkJDO pojo, BidirectionalChildLongPkJDO bidir1,
                     BidirectionalChildLongPkJDO bidir2, BidirectionalChildLongPkJDO bidir3)
      throws EntityNotFoundException {
    Flight f1 = new Flight();
    Flight f2 = new Flight();
    Flight f3 = new Flight();
    pojo.addFlight(f1);
    pojo.addFlight(f2);
    pojo.addFlight(f3);

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    String f2Id = f2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeFlights(Collections.singleton(f2));
    assertFalse(pojo.getFlights().contains(f2));
    assertTrue(pojo.getFlights().contains(f1));
    assertTrue(pojo.getFlights().contains(f3));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    assertFalse(pojo.getBidirChildren().contains(bidir2));
    assertTrue(pojo.getBidirChildren().contains(bidir1));
    assertTrue(pojo.getBidirChildren().contains(bidir3));
    commitTxn();
    beginTxn();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());

    assertEquals(2, pojo.getFlights().size());
    assertFalse(pojo.getFlights().contains(f2));
    assertTrue(pojo.getFlights().contains(f1));
    assertTrue(pojo.getFlights().contains(f3));

    assertEquals(2, pojo.getBidirChildren().size());
    assertFalse(pojo.getBidirChildren().contains(bidir2));
    assertTrue(pojo.getBidirChildren().contains(bidir1));
    assertTrue(pojo.getBidirChildren().contains(bidir3));
    commitTxn();

    Entity flightEntity1 = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flightEntity3 = ldth.ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity bidirEntity1 = ldth.ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ldth.ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, flightEntity1.getProperty("flights_INTEGER_IDX_longpk"));
      assertEquals(1L, flightEntity3.getProperty("flights_INTEGER_IDX_longpk"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  void testRemoveAll_UnencodedStringPkOnParent(HasOneToManyUnencodedStringPkJDO pojo, BidirectionalChildUnencodedStringPkJDO bidir1,
                     BidirectionalChildUnencodedStringPkJDO bidir2, BidirectionalChildUnencodedStringPkJDO bidir3)
      throws EntityNotFoundException {
    Flight f1 = new Flight();
    Flight f2 = new Flight();
    Flight f3 = new Flight();
    pojo.addFlight(f1);
    pojo.addFlight(f2);
    pojo.addFlight(f3);

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    String f2Id = f2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeFlights(Collections.singleton(f2));
    assertFalse(pojo.getFlights().contains(f2));
    assertTrue(pojo.getFlights().contains(f1));
    assertTrue(pojo.getFlights().contains(f3));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    assertFalse(pojo.getBidirChildren().contains(bidir2));
    assertTrue(pojo.getBidirChildren().contains(bidir1));
    assertTrue(pojo.getBidirChildren().contains(bidir3));
    commitTxn();
    beginTxn();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());

    assertEquals(2, pojo.getFlights().size());
    assertFalse(pojo.getFlights().contains(f2));
    assertTrue(pojo.getFlights().contains(f1));
    assertTrue(pojo.getFlights().contains(f3));

    assertEquals(2, pojo.getBidirChildren().size());
    assertFalse(pojo.getBidirChildren().contains(bidir2));
    assertTrue(pojo.getBidirChildren().contains(bidir1));
    assertTrue(pojo.getBidirChildren().contains(bidir3));
    commitTxn();

    Entity flightEntity1 = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flightEntity3 = ldth.ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity bidirEntity1 = ldth.ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ldth.ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, flightEntity1.getProperty("flights_INTEGER_IDX_unencodedstringpk"));
      assertEquals(1L, flightEntity3.getProperty("flights_INTEGER_IDX_unencodedstringpk"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  void testChangeParent(HasOneToManyJDO pojo, HasOneToManyJDO pojo2) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = new Flight();
    pojo.addFlight(f1);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo2.addFlight(f1);
    try {
      pm.makePersistent(pojo2);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      rollbackTxn();
    }
  }

  void testNewParentNewChild_NamedKeyOnChild(HasOneToManyJDO pojo) throws EntityNotFoundException {
    Flight f1 = new Flight();
    pojo.addFlight(f1);
    f1.setId(KeyFactory.keyToString(
        KeyFactory.createKey(Flight.class.getSimpleName(), "named key")));
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    assertEquals("named key", flightEntity.getKey().getName());
  }

  void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(HasOneToManyJDO pojo) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = new Flight();
    pm.makePersistent(f1);
    f1 = pm.detachCopy(f1);
    pm.close();
    pm = pmf.getPersistenceManager();
    pojo.addFlight(f1);
    try {
      pm.makePersistent(pojo);
//      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    pm.close();

    assertEquals(1, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Flight.class));
    pm = pmf.getPersistenceManager();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
  }

  void testAddAlreadyPersistedChildToParent_NoTxnSamePm(HasOneToManyJDO pojo) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = new Flight();
    pm.makePersistent(f1);
    pojo.addFlight(f1);
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    pm.close();

    assertEquals(1, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Flight.class));
    pm = pmf.getPersistenceManager();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
  }

  void testFetchOfOneToManyParentWithKeyPk(HasOneToManyKeyPkJDO pojo) {
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
    commitTxn();
  }

  void testFetchOfOneToManyParentWithLongPk(HasOneToManyLongPkJDO pojo) {
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
    commitTxn();
  }

  void testFetchOfOneToManyParentWithUnencodedStringPk(HasOneToManyUnencodedStringPkJDO pojo) {
    pojo.setId("yar");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
    commitTxn();
  }

  void testAddChildToOneToManyParentWithLongPk(
      HasOneToManyLongPkJDO pojo, BidirectionalChildLongPkJDO bidirChild)
      throws EntityNotFoundException {
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    Flight f = new Flight();
    pojo.addFlight(f);
    pojo.addBidirChild(bidirChild);
    commitTxn();
    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ldth.ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals(pojoEntity.getKey(), flightEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddChildToOneToManyParentWithUnencodedStringPk(
      HasOneToManyUnencodedStringPkJDO pojo, BidirectionalChildUnencodedStringPkJDO bidirChild)
      throws EntityNotFoundException {
    pojo.setId("yar");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    Flight f = new Flight();
    pojo.addFlight(f);
    pojo.addBidirChild(bidirChild);
    commitTxn();
    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ldth.ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals(pojoEntity.getKey(), flightEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddQueriedParentToBidirChild(HasOneToManyJDO pojo, BidirectionalChildJDO bidir)
      throws EntityNotFoundException {
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    bidir.setParent(pojo);
    pm.makePersistent(bidir);
    pojo.getBidirChildren().add(bidir);
    commitTxn();
    assertEquals(1, countForClass(bidir.getClass()));
    Entity e = ldth.ds.prepare(new Query(bidir.getClass().getSimpleName())).asSingleEntity();
    assertNotNull(e.getParent());
    beginTxn();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    assertEquals(1, pojo.getBidirChildren().size());
    commitTxn();
    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddFetchedParentToBidirChild(HasOneToManyJDO pojo, BidirectionalChildJDO bidir)
      throws EntityNotFoundException {
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    bidir.setParent(pojo);
    pm.makePersistent(bidir);
    pojo.getBidirChildren().add(bidir);
    commitTxn();
    assertEquals(1, countForClass(bidir.getClass()));
    Entity e = ldth.ds.prepare(new Query(bidir.getClass().getSimpleName())).asSingleEntity();
    assertNotNull(e.getParent());
    beginTxn();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    assertEquals(1, pojo.getBidirChildren().size());
    commitTxn();
    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testReplaceBidirColl(HasOneToManyJDO parent,
                            BidirectionalChildJDO bidir1,
                            Collection<BidirectionalChildJDO> newColl) {
    bidir1.setParent(parent);
    parent.addBidirChild(bidir1);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    String childKey = bidir1.getId();
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), KeyFactory.stringToKey(parent.getId()));
    parent.setBidirChildren(newColl);
    commitTxn();

    beginTxn();
    parent = pm.getObjectById(parent.getClass(), KeyFactory.stringToKey(parent.getId()));
    assertEquals(2, parent.getBidirChildren().size());
    Iterator<BidirectionalChildJDO> childIter = parent.getBidirChildren().iterator();
    assertFalse(childKey.equals(childIter.next().getId()));
    assertFalse(childKey.equals(childIter.next().getId()));
    commitTxn();
    assertEquals(2, countForClass(newColl.iterator().next().getClass()));
  }


//  void testIndexOf(HasOneToManyJDO pojo, BidirectionalChildJDO bidir1, BidirectionalChildJDO bidir2,
//                   BidirectionalChildJDO bidir3) {
//
//    Flight f1 = new Flight();
//    Flight f2 = new Flight();
//    Flight f3 = new Flight();
//    pojo.addFlight(f1);
//    pojo.addFlight(f2);
//    pojo.addFlight(f3);
//
//    pojo.addBidirChild(bidir1);
//    pojo.addBidirChild(bidir2);
//    pojo.addBidirChild(bidir3);
//
//    beginTxn();
//    pm.makePersistent(pojo);
//    commitTxn();
//    beginTxn();
//    assertEquals(0, pojo.indexOf(f1));
//    assertEquals(1, pojo.indexOf(f2));
//    assertEquals(2, pojo.indexOf(f3));
//    assertEquals(0, pojo.indexOf(bidir1));
//    assertEquals(1, pojo.indexOf(bidir2));
//    assertEquals(2, pojo.indexOf(bidir3));
//    commitTxn();
//
//    beginTxn();
//    pojo = pm.getObjectById(pojo.getClass(), pojo.getKey());
//    assertEquals(0, pojo.indexOf(f1));
//    assertEquals(1, pojo.indexOf(f2));
//    assertEquals(2, pojo.indexOf(f3));
//    assertEquals(0, pojo.indexOf(bidir1));
//    assertEquals(1, pojo.indexOf(bidir2));
//    assertEquals(2, pojo.indexOf(bidir3));
//    commitTxn();
//  }

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
