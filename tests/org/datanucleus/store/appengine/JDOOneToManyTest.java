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
import org.datanucleus.test.HasOneToManyWithNonDeletingCascadeJDO;
import org.datanucleus.test.HasOneToManyWithOrderByJDO;
import org.easymock.EasyMock;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManyTest extends JDOTestCase {

  public void testInsert_NewParentAndChild() throws EntityNotFoundException {
    BidirectionalChildJDO bidirChild = new BidirectionalChildJDO();
    bidirChild.setChildVal("yam");

    Flight f = newFlight();

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");

    HasOneToManyJDO parent = new HasOneToManyJDO();
    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getFlights().add(f);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    assertNotNull(bidirChild.getId());
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.decodeKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.decodeKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity flightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.decodeKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(parent.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    HasOneToManyJDO pojo = new HasOneToManyJDO();
    pojo.setVal("yar");

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    assertNotNull(pojo.getId());
    assertTrue(pojo.getFlights().isEmpty());
    assertTrue(pojo.getHasKeyPks().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());
    commitTxn();
    beginTxn();
    Flight f = newFlight();
    pojo.getFlights().add(f);

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");
    pojo.getHasKeyPks().add(hasKeyPk);

    BidirectionalChildJDO bidirChild = new BidirectionalChildJDO();
    bidirChild.setChildVal("yam");
    pojo.getBidirChildren().add(bidirChild);

    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    commitTxn();
    
    Entity bidirChildEntity = ldth.ds.get(KeyFactory.decodeKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.decodeKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());

    Entity flightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.decodeKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    BidirectionalChildJDO bidir = new BidirectionalChildJDO();

    HasOneToManyJDO pojo = new HasOneToManyJDO();
    pojo.getFlights().add(f);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
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

    Entity FlightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(FlightEntity);
    assertEquals("yam", FlightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), FlightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity bidirEntity = ldth.ds.get(KeyFactory.decodeKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    BidirectionalChildJDO bidir = new BidirectionalChildJDO();

    HasOneToManyJDO pojo = new HasOneToManyJDO();
    pojo.getFlights().add(f);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    beginTxn();
    pojo = pm.getObjectById(HasOneToManyJDO.class, pojo.getId());
    pojo.getFlights().get(0).setOrigin("yam");
    pojo.getHasKeyPks().get(0).setStr("yar");
    pojo.getBidirChildren().get(0).setChildVal("yap");
    commitTxn();

    Entity FlightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(FlightEntity);
    assertEquals("yam", FlightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), FlightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity bidirEntity = ldth.ds.get(KeyFactory.decodeKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_NullOutChildren() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    BidirectionalChildJDO bidir = new BidirectionalChildJDO();

    HasOneToManyJDO pojo = new HasOneToManyJDO();
    pojo.getFlights().add(f);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    assertCountsInDatastore(1, 1);

    beginTxn();
    String flightId = f.getId();
    Key hasKeyPkKey = hasKeyPk.getKey();
    String bidirChildId = bidir.getId();

    pojo.setFlights(null);
    pojo.setHasKeyPks(null);
    pojo.setBidirChildren(null);
    pm.makePersistent(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.decodeKey(flightId));
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
      ldth.ds.get(KeyFactory.decodeKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(1, 0);
  }

  public void testUpdate_ClearOutChildren() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    BidirectionalChildJDO bidir = new BidirectionalChildJDO();

    HasOneToManyJDO pojo = new HasOneToManyJDO();
    pojo.getFlights().add(f);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    String flightId = f.getId();
    Key hasKeyPkId = hasKeyPk.getKey();
    String bidirChildId = bidir.getId();
    assertCountsInDatastore(1, 1);

    beginTxn();
    pojo.getFlights().clear();
    pojo.getHasKeyPks().clear();
    pojo.getBidirChildren().clear();
    pm.makePersistent(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.decodeKey(flightId));
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
      ldth.ds.get(KeyFactory.decodeKey(bidirChildId));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(1, 0);
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

    Entity FlightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
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

    Entity FlightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(FlightEntity);
    assertEquals(HasOneToManyWithNonDeletingCascadeJDO.class.getName(), 1,
                 countForClass(HasOneToManyWithNonDeletingCascadeJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));
  }

  // TODO(maxr) Make this pass once Andy adds the extension attribute to the
  // Order annotation.
  public void testFindWithOrderBy() throws EntityNotFoundException {
    Entity pojoEntity = new Entity(HasOneToManyWithOrderByJDO.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity1 = newFlightEntity(pojoEntity.getKey(), "bos1", "mia2", "name 1");
    ldth.ds.put(flightEntity1);

    Entity flightEntity2 = newFlightEntity(pojoEntity.getKey(), "bos2", "mia2", "name 2");
    ldth.ds.put(flightEntity2);

    Entity flightEntity3 = newFlightEntity(pojoEntity.getKey(), "bos1", "mia1", "name 0");
    ldth.ds.put(flightEntity3);

    beginTxn();
    HasOneToManyWithOrderByJDO pojo = pm.getObjectById(HasOneToManyWithOrderByJDO.class,
                                                       KeyFactory.encodeKey(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getFlightsByAuthorAndTitle());
    assertEquals(3, pojo.getFlightsByAuthorAndTitle().size());
    assertEquals("title 2", pojo.getFlightsByAuthorAndTitle().get(0).getName());
    assertEquals("title 0", pojo.getFlightsByAuthorAndTitle().get(1).getName());
    assertEquals("title 1", pojo.getFlightsByAuthorAndTitle().get(2).getName());

    assertNotNull(pojo.getFlightsByIdAndAuthor());
    assertEquals(3, pojo.getFlightsByIdAndAuthor().size());
    assertEquals("title 0", pojo.getFlightsByIdAndAuthor().get(0).getName());
    assertEquals("title 2", pojo.getFlightsByIdAndAuthor().get(1).getName());
    assertEquals("title 1", pojo.getFlightsByIdAndAuthor().get(2).getName());

    assertNotNull(pojo.getFlightsByAuthorAndId());
    assertEquals(3, pojo.getFlightsByAuthorAndId().size());
    assertEquals("title 2", pojo.getFlightsByAuthorAndId().get(0).getName());
    assertEquals("title 1", pojo.getFlightsByAuthorAndId().get(1).getName());
    assertEquals("title 0", pojo.getFlightsByAuthorAndId().get(2).getName());

    commitTxn();
  }

  private Entity newFlightEntity(
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

  public void testFind() throws EntityNotFoundException {
    Entity pojoEntity = new Entity(HasOneToManyJDO.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos1", "mia2", "name 1");
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(BidirectionalChildJDO.class.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ldth.ds.put(bidirEntity);

    beginTxn();
    HasOneToManyJDO pojo = pm.getObjectById(
        HasOneToManyJDO.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getFlights());
    assertEquals(1, pojo.getFlights().size());
    assertEquals("bos1", pojo.getFlights().get(0).getOrigin());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().get(0).getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().get(0).getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().get(0).getParent());
    commitTxn();
  }

  public void testQuery() {
    Entity pojoEntity = new Entity(HasOneToManyJDO.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia2", "name");
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(BidirectionalChildJDO.class.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ldth.ds.put(bidirEntity);

    javax.jdo.Query q = pm.newQuery(
        "select from " + HasOneToManyJDO.class.getName() + " where id == key parameters String key");
    beginTxn();
    @SuppressWarnings("unchecked")
    List<HasOneToManyJDO> result =
        (List<HasOneToManyJDO>) q.execute(KeyFactory.encodeKey(pojoEntity.getKey()));
    assertEquals(1, result.size());
    HasOneToManyJDO pojo = result.get(0);
    assertNotNull(pojo.getFlights());
    assertEquals(1, pojo.getFlights().size());
    assertEquals("bos", pojo.getFlights().get(0).getOrigin());
    assertEquals(1, pojo.getFlights().size());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().get(0).getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().get(0).getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().get(0).getParent());
    commitTxn();
  }

  public void testChildFetchedLazily() throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    try {
      setUp();

      Entity pojoEntity = new Entity(HasOneToManyJDO.class.getSimpleName());
      ldth.ds.put(pojoEntity);

      Entity FlightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia", "name");
      ldth.ds.put(FlightEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity bidirEntity = new Entity(BidirectionalChildJDO.class.getSimpleName(), pojoEntity.getKey());
      bidirEntity.setProperty("childVal", "yap");
      ldth.ds.put(bidirEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(ds.beginTransaction()).andReturn(txn);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(ds.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      beginTxn();
      HasOneToManyJDO pojo = pm.getObjectById(HasOneToManyJDO.class, KeyFactory.encodeKey(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  public void testDeleteParentDeletesChild() {
    Entity pojoEntity = new Entity(HasOneToManyJDO.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = newFlightEntity(pojoEntity.getKey(), "bos", "mia", "name");
    flightEntity.setProperty("flights_INTEGER_IDX", 1);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    hasKeyPkEntity.setProperty("hasKeyPks_INTEGER_IDX", 1);
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(BidirectionalChildJDO.class.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    bidirEntity.setProperty("bidirChildren_INTEGER_IDX", 1);
    ldth.ds.put(bidirEntity);

    beginTxn();
    HasOneToManyJDO pojo = pm.getObjectById(HasOneToManyJDO.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    pm.deletePersistent(pojo);
    commitTxn();
    assertCountsInDatastore(0, 0);
  }

  private int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(new Query(clazz.getSimpleName())).countEntities();
  }

  private void assertCountsInDatastore(int expectedParent, int expectedChildren) {
    assertEquals(
        HasOneToManyJDO.class.getName(), expectedParent, countForClass(HasOneToManyJDO.class));
    assertEquals(
        BidirectionalChildJDO.class.getName(), expectedChildren,
        countForClass(BidirectionalChildJDO.class));
    assertEquals(
        Flight.class.getName(), expectedChildren, countForClass(Flight.class));
    assertEquals(
        HasKeyPkJDO.class.getName(), expectedChildren, countForClass(HasKeyPkJDO.class));
  }

  private Flight newFlight() {
    Flight f = new Flight();
    f.setOrigin("bos");
    f.setDest("mia");
    f.setName("jimmy");
    f.setMe(22);
    f.setYou(26);
    f.setFlightNumber(99);
    return f;
  }

}