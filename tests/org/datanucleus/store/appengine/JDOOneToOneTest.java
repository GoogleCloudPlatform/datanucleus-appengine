// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasOneToOneJDO;
import org.datanucleus.test.HasOneToOneParentJDO;
import org.datanucleus.test.HasOneToOneParentKeyPkJDO;
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
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
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

    Entity hasParentEntity = ldth.ds.get(KeyFactory.decodeKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.decodeKey(hasParent.getKey()), hasParentEntity.getKey());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getKey(), hasParentKeyPkEntity.getKey());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(flightEntity.getKey(), pojoEntity.getProperty("flight_id"));
    assertEquals(hasKeyPkEntity.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(hasParentEntity.getKey(), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPkEntity.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_NewParentExistingChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();

    pm.makePersistent(f);
    pm.makePersistent(hasKeyPk);
    assertNotNull(f.getId());

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    pm.makePersistent(pojo);

    assertNotNull(pojo.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(f.getId()), pojoEntity.getProperty("flight_id"));
    assertEquals(hasKeyPk.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(KeyFactory.decodeKey(hasParent.getKey()), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPk.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    HasOneToOneJDO pojo = new HasOneToOneJDO();

    pm.makePersistent(pojo);
    assertNotNull(pojo.getId());
    assertNull(pojo.getFlight());
    assertNull(pojo.getHasKeyPK());
    assertNull(pojo.getHasParent());
    assertNull(pojo.getHasParentKeyPK());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertTrue(pojoEntity.getProperties().keySet().contains("flight_id"));
    assertNull(pojoEntity.getProperty("flight_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparent_id"));
    assertNull(pojoEntity.getProperty("hasparent_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparentkeypk_id"));
    assertNull(pojoEntity.getProperty("hasparentkeypk_id"));

    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();
    pm.currentTransaction().begin();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParent.setParent(pojo);
    pm.currentTransaction().commit();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(KeyFactory.decodeKey(f.getId()), pojoEntity.getProperty("flight_id"));
    assertEquals(hasKeyPk.getKey(), pojoEntity.getProperty("haskeypk_id"));
    assertEquals(KeyFactory.decodeKey(hasParent.getKey()), pojoEntity.getProperty("hasparent_id"));
    assertEquals(hasParentKeyPk.getKey(), pojoEntity.getProperty("hasparentkeypk_id"));

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);

    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);

    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParent.setParent(pojo);

    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(pojo.getId());

    pm.currentTransaction().begin();
    f.setOrigin("yam");
    hasKeyPk.setStr("yar");
    hasParent.setStr("yag");
    hasParentKeyPk.setStr("yap");
    pm.currentTransaction().commit();

    Entity flightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));

    Entity hasParentEntity = ldth.ds.get(KeyFactory.decodeKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals("yag", hasParentEntity.getProperty("str"));

    Entity hasParentPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentPkEntity);
    assertEquals("yap", hasParentPkEntity.getProperty("str"));

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();
    HasOneToOneJDO pojo = new HasOneToOneJDO();

    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParent.setParent(pojo);

    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(pojo.getId());

    pm.currentTransaction().begin();
    pojo = pm.getObjectById(HasOneToOneJDO.class, pojo.getId());
    pojo.getFlight().setOrigin("yam");
    pojo.getHasKeyPK().setStr("yar");
    pojo.getHasParent().setStr("yag");
    pojo.getHasParentKeyPK().setStr("yap");
    pm.currentTransaction().commit();

    Entity flightEntity = ldth.ds.get(KeyFactory.decodeKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));

    Entity hasParentEntity = ldth.ds.get(KeyFactory.decodeKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals("yag", hasParentEntity.getProperty("str"));

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals("yap", hasParentKeyPkEntity.getProperty("str"));

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_NullOutChild() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParent.setParent(pojo);

    pm.makePersistent(pojo);

    pm.currentTransaction().begin();
    pojo.setFlight(null);
    pojo.setHasKeyPK(null);
    pojo.setHasParent(null);
    pojo.setHasParentKeyPK(null);
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

    try {
      ldth.ds.get(KeyFactory.decodeKey(hasParent.getKey()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasParentKeyPk.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertTrue(pojoEntity.getProperties().keySet().contains("flight_id"));
    assertNull(pojoEntity.getProperty("flight_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("haskeypk_id"));
    assertNull(pojoEntity.getProperty("haskeypk_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparent_id"));
    assertNull(pojoEntity.getProperty("hasparent_id"));
    assertTrue(pojoEntity.getProperties().keySet().contains("hasparentkeypk_id"));
    assertNull(pojoEntity.getProperty("hasparentkeypk_id"));

    assertCountsInDatastore(1, 0);
  }

  public void testFind() throws EntityNotFoundException {
    Entity flightEntity = Flight.newFlightEntity("jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity = new Entity(HasOneToOneParentJDO.class.getSimpleName());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity = new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    pojoEntity.setProperty("flight_id", flightEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
    pojoEntity.setProperty("hasparentkeypk_id", hasParentKeyPkEntity.getKey());
    ldth.ds.put(pojoEntity);


    HasOneToOneJDO pojo = pm.getObjectById(HasOneToOneJDO.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getFlight());
    assertEquals("bos", pojo.getFlight().getOrigin());
    assertEquals("mia", pojo.getFlight().getDest());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
    assertNotNull(pojo.getHasParent());
    assertEquals("yap", pojo.getHasParent().getStr());
    assertNotNull(pojo.getHasParentKeyPK());
    assertEquals("yag", pojo.getHasParentKeyPK().getStr());
  }

  public void testQuery() {
    Entity flightEntity = Flight.newFlightEntity("jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity = new Entity(HasOneToOneParentJDO.class.getSimpleName());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity = new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    pojoEntity.setProperty("flight_id", flightEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
    pojoEntity.setProperty("hasparentkeypk_id", hasParentKeyPkEntity.getKey());
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
    assertNotNull(pojo.getHasParent());
    assertEquals("yap", pojo.getHasParent().getStr());
    assertNotNull(pojo.getHasParentKeyPK());
    assertEquals("yag", pojo.getHasParentKeyPK().getStr());
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

      Entity hasParentEntity = new Entity(HasOneToOneParentJDO.class.getSimpleName());
      hasParentEntity.setProperty("str", "yap");
      ldth.ds.put(hasParentEntity);

      Entity hasParentPkEntity = new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName());
      hasParentPkEntity.setProperty("str", "yag");
      ldth.ds.put(hasParentPkEntity);

      Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
      pojoEntity.setProperty("flight_id", flightEntity.getKey());
      pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
      pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
      pojoEntity.setProperty("hasparentkeypk_id", hasParentPkEntity.getKey());
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

  public void testDeleteParentDeletesChild() {
    Entity flightEntity = Flight.newFlightEntity("jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity = new Entity(HasOneToOneParentJDO.class.getSimpleName());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentPkEntity = new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName());
    hasParentPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentPkEntity);

    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    pojoEntity.setProperty("flight_id", flightEntity.getKey());
    pojoEntity.setProperty("haskeypk_id", hasKeyPkEntity.getKey());
    pojoEntity.setProperty("hasparent_id", hasParentEntity.getKey());
    pojoEntity.setProperty("hasparentkeypk_id", hasParentPkEntity.getKey());
    ldth.ds.put(pojoEntity);

    pm.currentTransaction().begin();
    HasOneToOneJDO pojo = pm.getObjectById(HasOneToOneJDO.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    pm.deletePersistent(pojo);
    pm.currentTransaction().commit();

    assertCountsInDatastore(0, 0);
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

  private int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(
        new com.google.apphosting.api.datastore.Query(clazz.getSimpleName())).countEntities();
  }

  private void assertCountsInDatastore(int expectedParent, int expectedChildren) {
    assertEquals(expectedParent, countForClass(HasOneToOneJDO.class));
    assertEquals(expectedChildren, countForClass(Flight.class));
    assertEquals(expectedChildren, countForClass(HasKeyPkJDO.class));
    assertEquals(expectedChildren, countForClass(HasOneToOneParentJDO.class));
    assertEquals(expectedChildren, countForClass(HasOneToOneParentKeyPkJDO.class));
  }
}
