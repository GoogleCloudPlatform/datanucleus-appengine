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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.DatastoreServiceInterceptor;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.BidirectionalChildJDO;
import com.google.appengine.datanucleus.test.BidirectionalChildLongPkJDO;
import com.google.appengine.datanucleus.test.BidirectionalChildUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.HasExplicitIndexColumnJDO;
import com.google.appengine.datanucleus.test.HasKeyPkJDO;
import com.google.appengine.datanucleus.test.HasOneToManyJDO;
import com.google.appengine.datanucleus.test.HasOneToManyKeyPkJDO;
import com.google.appengine.datanucleus.test.HasOneToManyListJDO;
import com.google.appengine.datanucleus.test.HasOneToManyListWithOrderByJDO;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkJDO;
import com.google.appengine.datanucleus.test.HasOneToManyUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.HasOneToManyWithOrderByJDO;

import static com.google.appengine.datanucleus.TestUtils.assertKeyParentEquals;

import org.datanucleus.util.NucleusLogger;
import org.easymock.EasyMock;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;

/**
 * @author Max Ross <maxr@google.com>
 */
abstract class JDOOneToManyTestCase extends JDOTestCase {

  @Override
  protected void tearDown() throws Exception {
    try {
      if (!pm.isClosed() && pm.currentTransaction().isActive()) {
        pm.currentTransaction().rollback();
      }
      pmf.close();
    } finally {
      super.tearDown();
    }
  }

  void testInsert_NewParentAndChild(HasOneToManyJDO parent,
      BidirectionalChildJDO bidirChild, StartEnd startEnd) throws EntityNotFoundException {
    bidirChild.setChildVal("yam");

    Flight f = newFlight();

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");

    parent.addBidirChild(bidirChild);
    bidirChild.setParent(parent);
    parent.addFlight(f);
    parent.addHasKeyPk(hasKeyPk);
    parent.setVal("yar");

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(parent.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(flightEntity.getKey()), parentEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(parent.getClass(), bidirChild.getClass(), 1, 1);
  }

  void testInsert_ExistingParentNewChild(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidirChild, StartEnd startEnd) throws EntityNotFoundException {
    pojo.setVal("yar");

    NucleusLogger.GENERAL.info(">> {}.START");
    startEnd.start();
    NucleusLogger.GENERAL.info(">> pm.makePersistent(HasOneToManyJDO)");
    pm.makePersistent(pojo);
    NucleusLogger.GENERAL.info(">> {}.END");
    startEnd.end();
    NucleusLogger.GENERAL.info(">> {}.START pojo.state="+ JDOHelper.getObjectState(pojo));
    startEnd.start();
    NucleusLogger.GENERAL.info(">> checking pojo in-memory");
    assertNotNull(pojo.getId());
    assertTrue(pojo.getFlights().isEmpty());
    assertTrue(pojo.getHasKeyPks().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    NucleusLogger.GENERAL.info(">> checking pojo using low-level API");
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals("yar", pojoEntity.getProperty("val"));
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("flights"));
    assertNull(pojoEntity.getProperty("flights"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    NucleusLogger.GENERAL.info(">> {}.END");
    startEnd.end();
    NucleusLogger.GENERAL.info(">> {}.START");
    startEnd.start();
    NucleusLogger.GENERAL.info(">> pm.makePersistent of pojo in state="+ JDOHelper.getObjectState(pojo));
    pojo = pm.makePersistent(pojo);
    assertEquals("yar", pojo.getVal());
    Flight f = newFlight();
    NucleusLogger.GENERAL.info(">> pojo.addFlight(new Flight()) with pojo in state " + JDOHelper.getObjectState(pojo));
    pojo.addFlight(f);

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");
    NucleusLogger.GENERAL.info(">> pojo.addHasKeyPK(new HasKeyPK())");
    pojo.addHasKeyPk(hasKeyPk);

    bidirChild.setChildVal("yam");
    NucleusLogger.GENERAL.info(">> pojo.addBidirChild(new BidirectionalChildSetJDO())");
    pojo.addBidirChild(bidirChild);

    NucleusLogger.GENERAL.info(">> {}.END");
    startEnd.end();

    NucleusLogger.GENERAL.info(">> {}.START");
    startEnd.start();
    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    NucleusLogger.GENERAL.info(">> {}.END");
    startEnd.end();
    
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
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
      BidirectionalChildJDO bidir2, StartEnd startEnd) throws EntityNotFoundException {
    pojo.setVal("yar");
    bidir2.setChildVal("yam");
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir1);
    bidir1.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 1);

    startEnd.start();
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
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getFlights().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(f2.getId());
    assertNotNull(hasKeyPk2.getKey());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(f2.getId())), pojoEntity.getProperty("flights"));
    assertEquals(Utils.newArrayList(hasKeyPk2.getKey()), pojoEntity.getProperty("hasKeyPks"));

    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(flight1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f2.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f2.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk2.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
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
      BidirectionalChildJDO bidir3, StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 3);

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String flight1Id = pojo.getFlights().iterator().next().getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().iterator().next().getKey();
    pojo.removeBidirChildAtPosition(0);
    pojo.removeFlightAtPosition(0);
    pojo.removeHasKeyPkAtPosition(0);
    startEnd.end();

    startEnd.start();
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getFlights().size());
    assertEquals(2, pojo.getHasKeyPks().size());
    assertEquals(2, pojo.getBidirChildren().size());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
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

    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(flight1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("another yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yet another yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir3.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(1L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f2.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f2.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    flightEntity = ds.get(KeyFactory.stringToKey(f3.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("yet another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f3.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(1L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk2.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    hasKeyPkEntity = ds.get(hasKeyPk3.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yet another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk3.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(1L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
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
      BidirectionalChildJDO bidir2, StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 1);

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().iterator().next().getId();
    String flight1Id = pojo.getFlights().iterator().next().getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().iterator().next().getKey();
    pojo.addAtPosition(0, bidir2);
    pojo.addAtPosition(0, f2);
    pojo.addAtPosition(0, hasKeyPk2);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getFlights().size());
    assertEquals(2, pojo.getHasKeyPks().size());
    assertEquals(2, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(f2.getId());
    assertNotNull(hasKeyPk2.getKey());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
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

    startEnd.end();

    ds.get(KeyFactory.stringToKey(bidir1Id));
    ds.get(KeyFactory.stringToKey(flight1Id));
    ds.get(hasKeyPk1Key);
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f2.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("another name", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f2.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f2.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk2.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
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
      BidirectionalChildJDO bidir, StartEnd startEnd) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    f.setOrigin("yam");
    hasKeyPk.setStr("yar");
    bidir.setChildVal("yap");
    pm.makePersistent(pojo);
    startEnd.end();

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_UpdateChild(HasOneToManyJDO pojo,
      BidirectionalChildJDO bidir, StartEnd startEnd) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    pojo.getFlights().iterator().next().setOrigin("yam");
    pojo.getHasKeyPks().iterator().next().setStr("yar");
    pojo.getBidirChildren().iterator().next().setChildVal("yap");
    startEnd.end();

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_NullOutChildren(HasOneToManyJDO pojo,
    BidirectionalChildJDO bidir, StartEnd startEnd) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    startEnd.start();
    String flightId = f.getId();
    Key hasKeyPkKey = hasKeyPk.getKey();
    String bidirChildId = bidir.getId();

    pojo.nullFlights();
    pojo.nullHasKeyPks();
    pojo.nullBidirChildren();
    pm.makePersistent(pojo);
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(flightId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(hasKeyPkKey);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
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
      BidirectionalChildJDO bidir, StartEnd startEnd) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pojo.addFlight(f);
    pojo.addHasKeyPk(hasKeyPk);
    pojo.addBidirChild(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    String flightId = f.getId();
    Key hasKeyPkId = hasKeyPk.getKey();
    String bidirChildId = bidir.getId();
    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    startEnd.start();
    pojo = pm.makePersistent(pojo);
    pojo.clearFlights();
    pojo.clearHasKeyPks();
    pojo.clearBidirChildren();
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(flightId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(hasKeyPkId);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("flights"));
    assertNull(pojoEntity.getProperty("flights"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testFindWithOrderBy(Class<? extends HasOneToManyWithOrderByJDO> pojoClass, StartEnd startEnd)
      throws EntityNotFoundException {
    getExecutionContext().getNucleusContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.allowMultipleRelationsOfSameType", true);
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity flightEntity1 = newFlightEntity(pojoEntity.getKey(), "bos1", "mia2", "name 1");
    flightEntity1.setProperty("flightsByOrigAndDest_INTEGER_IDX", 0);
    flightEntity1.setProperty("flightsByIdAndOrig_INTEGER_IDX", 0);
    flightEntity1.setProperty("flightsByOrigAndId_INTEGER_IDX", 0);
    ds.put(flightEntity1);

    Entity flightEntity2 = newFlightEntity(pojoEntity.getKey(), "bos2", "mia2", "name 2");
    flightEntity2.setProperty("flightsByOrigAndDest_INTEGER_IDX", 1);
    flightEntity2.setProperty("flightsByIdAndOrig_INTEGER_IDX", 1);
    flightEntity2.setProperty("flightsByOrigAndId_INTEGER_IDX", 1);
    ds.put(flightEntity2);

    Entity flightEntity3 = newFlightEntity(pojoEntity.getKey(), "bos1", "mia1", "name 0");
    flightEntity3.setProperty("flightsByOrigAndDest_INTEGER_IDX", 2);
    flightEntity3.setProperty("flightsByIdAndOrig_INTEGER_IDX", 2);
    flightEntity3.setProperty("flightsByOrigAndId_INTEGER_IDX", 2);
    ds.put(flightEntity3);

    Entity explicitIndexEntity1 =
        new Entity(HasExplicitIndexColumnJDO.class.getSimpleName(), pojoEntity.getKey());
    explicitIndexEntity1.setProperty("index", 3);
    ds.put(explicitIndexEntity1);

    Entity explicitIndexEntity2 =
        new Entity(HasExplicitIndexColumnJDO.class.getSimpleName(), pojoEntity.getKey());
    explicitIndexEntity2.setProperty("index", 2);
    ds.put(explicitIndexEntity2);

    Entity explicitIndexEntity3 =
        new Entity(HasExplicitIndexColumnJDO.class.getSimpleName(), pojoEntity.getKey());
    explicitIndexEntity3.setProperty("index", 1);
    ds.put(explicitIndexEntity3);

    startEnd.start();
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

    startEnd.end();
  }

  void testFind(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass, StartEnd startEnd) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos1", "mia2", "name 1");
    ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ds.put(bidirEntity);

    startEnd.start();
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
    startEnd.end();
  }

  void testQuery(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass, StartEnd startEnd) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia2", "name");
    ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ds.put(bidirEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + pojoClass.getName() + " where id == key parameters String key");
    startEnd.start();
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
    startEnd.end();
  }

  void testChildFetchedLazily(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass) throws Exception {
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    tearDown();
    DatastoreService mockDatastore = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService(config);
    DatastoreServiceFactoryInternal.setDatastoreService(mockDatastore);
    try {
      setUp();

      Entity pojoEntity = new Entity(pojoClass.getSimpleName());
      ds.put(pojoEntity);

      Entity FlightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia", "name");
      ds.put(FlightEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ds.put(hasKeyPkEntity);

      Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
      bidirEntity.setProperty("childVal", "yap");
      ds.put(bidirEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(txn.getId()).andReturn("1").times(2);
      txn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(txn);
      EasyMock.expect(mockDatastore.beginTransaction()).andReturn(txn);
      EasyMock.expect(mockDatastore.getCurrentTransaction(null)).andReturn(txn);
      EasyMock.expect(mockDatastore.getCurrentTransaction(null)).andReturn(null);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(mockDatastore.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(mockDatastore);

      beginTxn();
      HasOneToManyJDO pojo =
          pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(mockDatastore);
  }

  void testDeleteParentDeletesChild(Class<? extends HasOneToManyJDO> pojoClass,
      Class<? extends BidirectionalChildJDO> bidirClass, StartEnd startEnd) throws Exception {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia", "name");
    flightEntity.setProperty("flights_INTEGER_IDX", 1);
    ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ds.put(bidirEntity);

    startEnd.start();
    HasOneToManyJDO pojo = pm.getObjectById(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    pm.deletePersistent(pojo);
    startEnd.end();
    assertCountsInDatastore(pojoClass, bidirClass, 0, 0);
  }

  void testRemoveAll(HasOneToManyJDO pojo, BidirectionalChildJDO bidir1,
                     BidirectionalChildJDO bidir2, BidirectionalChildJDO bidir3, StartEnd startEnd)
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

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    String f2Id = f2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeFlights(Collections.singleton(f2));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    startEnd.end();
    startEnd.start();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());

    assertEquals(2, pojo.getFlights().size());
    Set<String> flightIds = Utils.newHashSet(f1.getId(), f2.getId(), f3.getId());
    for (Flight f : pojo.getFlights()) {
      flightIds.remove(f.getId());
    }
    assertEquals(1, flightIds.size());
    assertEquals(f2.getId(), flightIds.iterator().next());

    assertEquals(2, pojo.getBidirChildren().size());
    Set<String> bidirIds = Utils.newHashSet(bidir1.getId(), bidir2.getId(), bidir3.getId());
    for (BidirectionalChildJDO b : pojo.getBidirChildren()) {
      bidirIds.remove(b.getId());
    }
    assertEquals(1, bidirIds.size());
    assertEquals(bidir2.getId(), bidirIds.iterator().next());
    startEnd.end();

    Entity flightEntity1 = ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flightEntity3 = ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity bidirEntity1 = ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, flightEntity1.getProperty("flights_INTEGER_IDX"));
      assertEquals(1L, flightEntity3.getProperty("flights_INTEGER_IDX"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  void testRemoveAll_LongPkOnParent(HasOneToManyLongPkJDO pojo, BidirectionalChildLongPkJDO bidir1,
                     BidirectionalChildLongPkJDO bidir2, BidirectionalChildLongPkJDO bidir3, StartEnd startEnd)
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

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    String f2Id = f2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeFlights(Collections.singleton(f2));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    startEnd.end();
    startEnd.start();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(2, pojo.getFlights().size());
    Set<String> flightIds = Utils.newHashSet(f1.getId(), f2.getId(), f3.getId());
    for (Flight f : pojo.getFlights()) {
      flightIds.remove(f.getId());
    }
    assertEquals(1, flightIds.size());
    assertEquals(f2.getId(), flightIds.iterator().next());

    assertEquals(2, pojo.getBidirChildren().size());
    Set<String> bidirIds = Utils.newHashSet(bidir1.getId(), bidir2.getId(), bidir3.getId());
    for (BidirectionalChildLongPkJDO b : pojo.getBidirChildren()) {
      bidirIds.remove(b.getId());
    }
    assertEquals(1, bidirIds.size());
    assertEquals(bidir2.getId(), bidirIds.iterator().next());
    startEnd.end();

    Entity flightEntity1 = ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flightEntity3 = ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity bidirEntity1 = ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, flightEntity1.getProperty("flights_INTEGER_IDX_longpk"));
      assertEquals(1L, flightEntity3.getProperty("flights_INTEGER_IDX_longpk"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  void testRemoveAll_UnencodedStringPkOnParent(HasOneToManyUnencodedStringPkJDO pojo, BidirectionalChildUnencodedStringPkJDO bidir1,
                     BidirectionalChildUnencodedStringPkJDO bidir2, 
                     BidirectionalChildUnencodedStringPkJDO bidir3, StartEnd startEnd)
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

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    String f2Id = f2.getId();
    String bidir2Id = bidir2.getId();
    pojo.removeFlights(Collections.singleton(f2));
    pojo.removeBidirChildren(Collections.singleton(bidir2));
    startEnd.end();
    startEnd.start();

    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());

    assertEquals(2, pojo.getFlights().size());
    Set<String> flightIds = Utils.newHashSet(f1.getId(), f2.getId(), f3.getId());
    for (Flight f : pojo.getFlights()) {
      flightIds.remove(f.getId());
    }
    assertEquals(1, flightIds.size());
    assertEquals(f2.getId(), flightIds.iterator().next());

    assertEquals(2, pojo.getBidirChildren().size());
    Set<String> bidirIds = Utils.newHashSet(bidir1.getId(), bidir2.getId(), bidir3.getId());
    for (BidirectionalChildUnencodedStringPkJDO b : pojo.getBidirChildren()) {
      bidirIds.remove(b.getId());
    }
    assertEquals(1, bidirIds.size());
    assertEquals(bidir2.getId(), bidirIds.iterator().next());
    startEnd.end();

    Entity flightEntity1 = ds.get(KeyFactory.stringToKey(f1.getId()));
    Entity flightEntity3 = ds.get(KeyFactory.stringToKey(f3.getId()));
    Entity bidirEntity1 = ds.get(KeyFactory.stringToKey(bidir1.getId()));
    Entity bidirEntity3 = ds.get(KeyFactory.stringToKey(bidir3.getId()));
    if (isIndexed()) {
      assertEquals(0L, flightEntity1.getProperty("flights_INTEGER_IDX_unencodedstringpk"));
      assertEquals(1L, flightEntity3.getProperty("flights_INTEGER_IDX_unencodedstringpk"));
      assertEquals(0L, bidirEntity1.getProperty("bidirChildren_INTEGER_IDX"));
      assertEquals(1L, bidirEntity3.getProperty("bidirChildren_INTEGER_IDX"));
    }
    try {
      ds.get(KeyFactory.stringToKey(f2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bidir2Id));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  void testChangeParent(HasOneToManyJDO pojo, HasOneToManyJDO pojo2, StartEnd startEnd) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = new Flight();
    pojo.addFlight(f1);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo2.addFlight(f1);
    try {
      pm.makePersistent(pojo2);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      if (pm.currentTransaction().isActive()) {
        rollbackTxn();
      }
    }
  }

  void testNewParentNewChild_NamedKeyOnChild(HasOneToManyJDO pojo, StartEnd startEnd) throws EntityNotFoundException {
    Flight f1 = new Flight();
    pojo.addFlight(f1);
    f1.setId(KeyFactory.keyToString(
        KeyFactory.createKey(Flight.class.getSimpleName(), "named key")));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f1.getId()));
    assertEquals("named key", flightEntity.getKey().getName());
  }

  void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(HasOneToManyJDO pojo) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = new Flight();
    pm.makePersistent(f1);
    f1 = pm.detachCopy(f1);
    pm.close();
    pm = pmf.getPersistenceManager();
    f1 = pm.makePersistent(f1);
    pojo.addFlight(f1);
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    }
    pm.close();

    assertEquals(0, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Flight.class));
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

    assertEquals(0, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Flight.class));
  }

  void testFetchOfOneToManyParentWithKeyPk(HasOneToManyKeyPkJDO pojo, StartEnd startEnd) {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithLongPk(HasOneToManyLongPkJDO pojo, StartEnd startEnd) {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithUnencodedStringPk(HasOneToManyUnencodedStringPkJDO pojo, StartEnd startEnd) {
    pojo.setId("yar");
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getFlights().size());
    startEnd.end();
  }

  void testAddChildToOneToManyParentWithLongPk(
      HasOneToManyLongPkJDO pojo, BidirectionalChildLongPkJDO bidirChild, StartEnd startEnd)
      throws EntityNotFoundException {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    Flight f = new Flight();
    pojo.addFlight(f);
    pojo.addBidirChild(bidirChild);
    startEnd.end();
    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals(pojoEntity.getKey(), flightEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddChildToOneToManyParentWithUnencodedStringPk(
      HasOneToManyUnencodedStringPkJDO pojo, BidirectionalChildUnencodedStringPkJDO bidirChild, 
      StartEnd startEnd)
      throws EntityNotFoundException {
    pojo.setId("yar");
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    Flight f = new Flight();
    pojo.addFlight(f);
    pojo.addBidirChild(bidirChild);
    startEnd.end();
    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals(pojoEntity.getKey(), flightEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddQueriedParentToBidirChild(HasOneToManyJDO pojo, BidirectionalChildJDO bidir, StartEnd startEnd)
      throws EntityNotFoundException {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    bidir.setParent(pojo);
    pm.makePersistent(bidir);
    pojo.getBidirChildren().add(bidir);
    startEnd.end();
    assertEquals(1, countForClass(bidir.getClass()));
    Entity e = ds.prepare(new Query(bidir.getClass().getSimpleName())).asSingleEntity();
    assertNotNull(e.getParent());
    startEnd.start();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddFetchedParentToBidirChild(HasOneToManyJDO pojo, BidirectionalChildJDO bidir, StartEnd startEnd)
      throws EntityNotFoundException {
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
    bidir.setParent(pojo);
    pm.makePersistent(bidir);
    pojo.getBidirChildren().add(bidir);
    startEnd.end();
    assertEquals(1, countForClass(bidir.getClass()));
    Entity e = ds.prepare(new Query(bidir.getClass().getSimpleName())).asSingleEntity();
    assertNotNull(e.getParent());
    startEnd.start();
    pojo = (HasOneToManyJDO) ((List<?>)pm.newQuery(pojo.getClass()).execute()).get(0);
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testReplaceBidirColl(HasOneToManyJDO parent,
                            BidirectionalChildJDO bidir1,
                            Collection<BidirectionalChildJDO> newColl, StartEnd startEnd) {
    bidir1.setParent(parent);
    parent.addBidirChild(bidir1);

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();
    String childKey = bidir1.getId();
    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), KeyFactory.stringToKey(parent.getId()));
    parent.setBidirChildren(newColl);
    startEnd.end();

    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), KeyFactory.stringToKey(parent.getId()));
    assertEquals(2, parent.getBidirChildren().size());
    Iterator<BidirectionalChildJDO> childIter = parent.getBidirChildren().iterator();
    assertFalse(childKey.equals(childIter.next().getId()));
    assertFalse(childKey.equals(childIter.next().getId()));
    startEnd.end();
    assertEquals(2, countForClass(newColl.iterator().next().getClass()));
  }

  private static final class PutPolicy implements DatastoreServiceInterceptor.Policy {
    private final List<Object[]> putParamList = Utils.newArrayList();
    public void intercept(Object o, Method method, Object[] params) {
      if (method.getName().equals("put")) {
        putParamList.add(params);
      }
    }
  }

  PutPolicy setupPutPolicy(HasOneToManyJDO pojo, BidirectionalChildJDO bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = new PutPolicy();
    DatastoreServiceInterceptor.install(getStoreManager(), policy);
    try {
      pmf.close();
      switchDatasource(startEnd.getPmfName());
      Flight flight = new Flight();
      pojo.addFlight(flight);
      pojo.addBidirChild(bidir);
      HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
      pojo.addHasKeyPk(hasKeyPk);

      startEnd.start();
      pm.makePersistent(pojo);
      startEnd.end();
      // 1 put for the parent, 3 puts for the children, 1 more put
      // to add the child keys back on the parent
      assertEquals(5, policy.putParamList.size());
      policy.putParamList.clear();
      return policy;
    } catch (Throwable t) {
      DatastoreServiceInterceptor.uninstall();
      throw t;
    }
  }

  void testOnlyOnePutOnChildUpdate(HasOneToManyJDO pojo, BidirectionalChildJDO bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.getFlights().iterator().next().setMe(88);
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      pojo.getHasKeyPks().iterator().next().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for each child update
    assertEquals(3, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnParentAndChildUpdate(HasOneToManyJDO pojo, BidirectionalChildJDO bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.getFlights().iterator().next().setMe(88);
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      pojo.getHasKeyPks().iterator().next().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for the parent update, 1 put for each child update
    assertEquals(4, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnChildDelete(HasOneToManyJDO pojo, BidirectionalChildJDO bidir, StartEnd startEnd)
      throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.nullFlights();
      pojo.nullBidirChildren();
      pojo.nullHasKeyPks();
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put to remove the keys
    assertEquals(1, policy.putParamList.size());
  }

  void testNonTxnAddOfChildToParentFailsPartwayThrough(HasOneToManyJDO pojo)
      throws Throwable {
    Flight flight1 = new Flight();
    pojo.addFlight(flight1);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    final String kind = kindForObject(pojo);
    DatastoreServiceInterceptor.Policy policy = new DatastoreServiceInterceptor.Policy() {
      public void intercept(Object o, Method method, Object[] params) {
        if (method.getName().equals("put") && ((Entity) params[0]).getKind().equals(kind)) {
          throw new ConcurrentModificationException("kaboom");
        }
      }
    };
    DatastoreServiceInterceptor.install(getStoreManager(), policy);
    Flight flight2 = new Flight();
    try {
      pmf.close();
      switchDatasource(PersistenceManagerFactoryName.nontransactional);
      pojo = pm.getObjectById(pojo.getClass(), pojo.getId());
      pojo.addFlight(flight2);
      pm.close();
      fail("expected exception");
    } catch (ConcurrentModificationException cme) {
      // good
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // prove that the book entity exists
    ds.get(KeyFactory.stringToKey(flight2.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    // the parent has a reference to the first book that was already there
    // but no reference to the second book
    assertEquals(
        Collections.singletonList(KeyFactory.stringToKey(flight1.getId())),
        pojoEntity.getProperty("flights"));
  }


  void testSaveWithOrderBy(HasOneToManyListWithOrderByJDO pojo, StartEnd startEnd)
      throws EntityNotFoundException {
    getExecutionContext().getNucleusContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.allowMultipleRelationsOfSameType", true);
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
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
//    startEnd.start();
//    pm.makePersistent(pojo);
//    startEnd.end();
//    startEnd.start();
//    assertEquals(0, pojo.indexOf(f1));
//    assertEquals(1, pojo.indexOf(f2));
//    assertEquals(2, pojo.indexOf(f3));
//    assertEquals(0, pojo.indexOf(bidir1));
//    assertEquals(1, pojo.indexOf(bidir2));
//    assertEquals(2, pojo.indexOf(bidir3));
//    startEnd.end();
//
//    startEnd.start();
//    pojo = pm.getObjectById(pojo.getClass(), pojo.getKey());
//    assertEquals(0, pojo.indexOf(f1));
//    assertEquals(1, pojo.indexOf(f2));
//    assertEquals(2, pojo.indexOf(f3));
//    assertEquals(0, pojo.indexOf(bidir1));
//    assertEquals(1, pojo.indexOf(bidir2));
//    assertEquals(2, pojo.indexOf(bidir3));
//    startEnd.end();
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
