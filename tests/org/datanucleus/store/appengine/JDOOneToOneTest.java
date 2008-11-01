// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasOneToOneJDO;
import org.easymock.EasyMock;

import java.util.List;

import javax.jdo.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToOneTest extends JDOTestCase {

  public void testInsert_NewParentAndChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);

    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(pojo.getId());

    Entity flightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals(2L, flightEntity.getProperty("me"));
    assertEquals(3L, flightEntity.getProperty("you"));
    assertEquals(44L, flightEntity.getProperty("flight_number"));
    assertEquals(KeyFactory.decodeKey(f.getId()), flightEntity.getKey());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(flightEntity.getKey(), pojoEntity.getProperty("flight_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
  }

  public void testInsert_NewParentExistingChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    pm.makePersistent(f);
    pm.makePersistent(hasKeyPk);
    assertNotNull(f.getId());

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);

    pm.makePersistent(pojo);

    assertNotNull(pojo.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(f.getId()), pojoEntity.getProperty("flight_id"));
    assertEquals(hasKeyPk.getKey(), pojoEntity.getProperty("haskeypk_id"));
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    HasOneToOneJDO pojo = new HasOneToOneJDO();

    pm.makePersistent(pojo);
    assertNotNull(pojo.getId());
    assertNull(pojo.getFlight());
    assertNull(pojo.getHasKeyPK());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertTrue(pojoEntity.getProperties().keySet().contains("flight_id"));
    assertNull(pojoEntity.getProperty("flight_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));

    Flight f = newFlight();
    pojo.setFlight(f);
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    pm.currentTransaction().begin();
    pojo.setHasKeyPK(hasKeyPk);
    pm.currentTransaction().commit();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(f.getId()), pojoEntity.getProperty("flight_id"));
    assertEquals(hasKeyPk.getKey(), pojoEntity.getProperty("haskeypk_id"));
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);

    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(pojo.getId());

    pm.currentTransaction().begin();
    f.setOrigin("yam");
    hasKeyPk.setStr("yar");
    pm.currentTransaction().commit();

    Entity flightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);

    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(pojo.getId());

    pm.currentTransaction().begin();
    pojo = pm.getObjectById(HasOneToOneJDO.class, pojo.getId());
    pojo.getFlight().setOrigin("yam");
    pojo.getHasKeyPK().setStr("yar");
    pm.currentTransaction().commit();

    Entity flightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
  }

  public void testUpdate_NullOutChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);

    pm.makePersistent(pojo);

    pm.currentTransaction().begin();
    pojo.setFlight(null);
    pojo.setHasKeyPK(null);
    pm.currentTransaction().commit();

    try {
      ldth.ds.get(KeyFactory.decodeKey(f.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasKeyPk.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertTrue(pojoEntity.getProperties().keySet().contains("flight_id"));
    assertNull(pojoEntity.getProperty("flight_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));
  }

  public void testFind() throws EntityNotFoundException {
    Entity flightEntity = Flight.newFlightEntity("jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);
    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);
    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    pojoEntity.setProperty("flight_id", flightEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    ldth.ds.put(pojoEntity);

    HasOneToOneJDO pojo = pm.getObjectById(HasOneToOneJDO.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getFlight());
    assertEquals("bos", pojo.getFlight().getOrigin());
    assertEquals("mia", pojo.getFlight().getDest());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
  }

  public void testQuery() {
    Entity flightEntity = Flight.newFlightEntity("jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    pojoEntity.setProperty("flight_id", flightEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    ldth.ds.put(pojoEntity);

    Query q = pm.newQuery("select from " + HasOneToOneJDO.class.getName()
        + " where id == key parameters String key");
    @SuppressWarnings("unchecked")
    List<HasOneToOneJDO> result =
        (List<HasOneToOneJDO>) q.execute(KeyFactory.encodeKey(pojoEntity.getKey()));
    assertEquals(1, result.size());
    HasOneToOneJDO pojo = result.get(0);
    assertNotNull(pojo.getFlight());
    assertEquals("bos", pojo.getFlight().getOrigin());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
  }

  public void testChildFetchedLazily() throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    try {
      setUp();

      Entity flightEntity = Flight.newFlightEntity("jimmy", "bos", "mia", 5, 4, 33);
      ldth.ds.put(flightEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
      pojoEntity.setProperty("flight_id", flightEntity.getKey());
      pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
      ldth.ds.put(pojoEntity);

      // the only get we're going to perform is for the pojo
      EasyMock.expect(ds.get(pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      HasOneToOneJDO pojo = pm.getObjectById(HasOneToOneJDO.class, KeyFactory.encodeKey(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  private Flight newFlight() {
    Flight flight = new Flight();
    flight.setName("jimmy");
    flight.setOrigin("bos");
    flight.setDest("mia");
    flight.setMe(2);
    flight.setYou(3);
    flight.setFlightNumber(44);
    return flight;
  }
}
