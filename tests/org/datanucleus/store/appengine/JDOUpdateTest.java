// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasVersionWithFieldJDO;
import org.datanucleus.test.NullDataJDO;
import org.datanucleus.test.Person;
import org.datanucleus.test.Name;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasAncestorJDO;
import org.datanucleus.test.HasKeyAncestorKeyStringPkJDO;

import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.JDOOptimisticVerificationException;
import javax.jdo.JDOUserException;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 * @author Max Ross <maxr@google.com>
 */
public class JDOUpdateTest extends JDOTestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "OPT_VERSION";

  public void testSimpleUpdate() throws EntityNotFoundException {
    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());

    flight.setName("2");
    commitTxn();

    Entity flightCheck = ldth.ds.get(key);
    assertEquals("yam", flightCheck.getProperty("origin"));
    assertEquals("bam", flightCheck.getProperty("dest"));
    assertEquals("2", flightCheck.getProperty("name"));
    assertEquals(1L, flightCheck.getProperty("you"));
    assertEquals(2L, flightCheck.getProperty("me"));
    // verify that the version got bumped
    assertEquals(2L,
        flightCheck.getProperty(DEFAULT_VERSION_PROPERTY_NAME));
  }

  public void testSimpleUpdateWithNamedKey() throws EntityNotFoundException {
    Key key = ldth.ds.put(Flight.newFlightEntity("named key", "1", "yam", "bam", 1, 2));

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());

    flight.setName("2");
    commitTxn();

    Entity flightCheck = ldth.ds.get(key);
    assertEquals("yam", flightCheck.getProperty("origin"));
    assertEquals("bam", flightCheck.getProperty("dest"));
    assertEquals("2", flightCheck.getProperty("name"));
    assertEquals(1L, flightCheck.getProperty("you"));
    assertEquals(2L, flightCheck.getProperty("me"));
    // verify that the version got bumped
    assertEquals(2L,
        flightCheck.getProperty(DEFAULT_VERSION_PROPERTY_NAME));
    assertEquals("named key", flightCheck.getKey().getName());
  }

  public void testOptimisticLocking_Update_NoField() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ldth.ds.put(flightEntity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    flight.setName("2");
    flightEntity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 2L);
    // we update the flight directly in the datastore right before commit
    ldth.ds.put(flightEntity);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    }
  }

  public void testOptimisticLocking_Delete_NoField() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity flightEntity = Flight.newFlightEntity("1", "yam", "bam", 1, 2);
    Key key = ldth.ds.put(flightEntity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    flight.setName("2");
    flightEntity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 2L);
    // we remove the flight from the datastore right before commit
    ldth.ds.delete(key);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    }
  }

  public void testOptimisticLocking_Update_HasVersionField() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionWithFieldJDO hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);

    hvwf.setValue("value");
    commitTxn();
    beginTxn();
    hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);
    assertEquals(2L, hvwf.getVersion());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    hvwf.setValue("another value");
    // we update the entity directly in the datastore right before commit
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    ldth.ds.put(entity);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    }
    // make sure the version didn't change on the model object
    assertEquals(2L, JDOHelper.getVersion(hvwf));
  }

  public void testOptimisticLocking_Delete_HasVersionField() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity entity = new Entity(HasVersionWithFieldJDO.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionWithFieldJDO hvwf = pm.getObjectById(HasVersionWithFieldJDO.class, keyStr);

    // delete the entity in the datastore right before we commit
    ldth.ds.delete(key);
    hvwf.setValue("value");
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (JDOOptimisticVerificationException jove) {
      // good
    }
    // make sure the version didn't change on the model object
    assertEquals(1L, JDOHelper.getVersion(hvwf));
  }

  public void testNonTransactionalUpdate() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    Flight f = pm.getObjectById(Flight.class, KeyFactory.keyToString(key));
    f.setYou(77);
    pm.close();
    Entity flightEntity = ldth.ds.get(key);
    assertEquals(77L, flightEntity.getProperty("you"));
    pm = pmf.getPersistenceManager();
  }

  public void testChangeStringPK_SetNonKeyString() throws EntityNotFoundException {
    Key key = ldth.ds.put(Flight.newFlightEntity("named key", "1", "yam", "bam", 1, 2));

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Flight flight = pm.getObjectById(Flight.class, keyStr);

    assertEquals(keyStr, flight.getId());
    assertEquals("yam", flight.getOrigin());
    assertEquals("bam", flight.getDest());
    assertEquals("1", flight.getName());
    assertEquals(1, flight.getYou());
    assertEquals(2, flight.getMe());

    flight.setName("2");
    flight.setId("foo");
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }

    Entity flightCheck = ldth.ds.get(key);
    assertEquals("yam", flightCheck.getProperty("origin"));
    assertEquals("bam", flightCheck.getProperty("dest"));
    assertEquals("1", flightCheck.getProperty("name"));
    assertEquals(1L, flightCheck.getProperty("you"));
    assertEquals(2L, flightCheck.getProperty("me"));
    // verify that the version got bumped
    assertEquals(1L,
        flightCheck.getProperty(DEFAULT_VERSION_PROPERTY_NAME));
    assertEquals("named key", flightCheck.getKey().getName());
  }

  public void testChangeStringPK_SetNull() throws EntityNotFoundException {
    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    beginTxn();
    Flight f = pm.getObjectById(Flight.class, KeyFactory.keyToString(key));
    f.setId(null);
    pm.makePersistent(f);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
    assertEquals(1, countForClass(Flight.class));
  }

  public void testChangePK_SetKeyString() throws EntityNotFoundException {
    Key key = ldth.ds.put(Flight.newFlightEntity("1", "yam", "bam", 1, 2));
    beginTxn();
    Flight f = pm.getObjectById(Flight.class, KeyFactory.keyToString(key));
    f.setId(KeyFactory.keyToString(KeyFactory.createKey(Flight.class.getSimpleName(), "yar")));
    f.setYou(77);
    pm.makePersistent(f);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
    assertEquals(1, countForClass(Flight.class));
    Entity e = ldth.ds.get(key);
    assertEquals(1L, e.getProperty("you"));
    assertEquals(1, countForClass(Flight.class));
  }

  public void testChangeKeyPK_SetDifferentKey() throws EntityNotFoundException {
    Key key = ldth.ds.put(new Entity(HasKeyPkJDO.class.getSimpleName()));

    beginTxn();
    HasKeyPkJDO pojo = pm.getObjectById(HasKeyPkJDO.class, key);

    pojo.setKey(KeyFactory.createKey(HasKeyPkJDO.class.getSimpleName(), 33));
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
    assertEquals(1, countForClass(HasKeyPkJDO.class));
  }

  public void testChangeKeyPK_SetNull() throws EntityNotFoundException {
    Key key = ldth.ds.put(new Entity(HasKeyPkJDO.class.getSimpleName()));
    beginTxn();
    HasKeyPkJDO pojo = pm.getObjectById(HasKeyPkJDO.class, key);
    pojo.setKey(null);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
    assertEquals(1, countForClass(HasKeyPkJDO.class));
  }

  public void testUpdateList_Add() throws EntityNotFoundException {
    NullDataJDO pojo = new NullDataJDO();
    List<String> list = Utils.newArrayList("a", "b");
    pojo.setList(list);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    pojo = pm.getObjectById(NullDataJDO.class, pojo.getId());
    pojo.getList().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("list")).size());
  }

  public void testUpdateList_Reset() throws EntityNotFoundException {
    NullDataJDO pojo = new NullDataJDO();
    List<String> list = Utils.newArrayList("a", "b");
    pojo.setList(list);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    pojo = pm.getObjectById(NullDataJDO.class, pojo.getId());
    list = Utils.newArrayList("a", "b", "zoom");
    pojo.setList(list);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("list")).size());
  }

  public void testUpdateArray_Reset() throws EntityNotFoundException {
    NullDataJDO pojo = new NullDataJDO();
    String[] array = new String[] {"a", "b"};
    pojo.setArray(array);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    pojo = pm.getObjectById(NullDataJDO.class, pojo.getId());
    array = new String[] {"a", "b", "c"};
    pojo.setArray(array);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("array")).size());
  }

  public void testUpdateArray_ModifyExistingElement() throws EntityNotFoundException {
    NullDataJDO pojo = new NullDataJDO();
    String[] array = new String[] {"a", "b"};
    pojo.setArray(array);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    pojo = pm.getObjectById(NullDataJDO.class, pojo.getId());
    pojo.getArray()[0] = "c";
    pojo.setArray(array);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals("c", ((List<?>)e.getProperty("array")).get(0));
  }

  public void testEmbeddable() throws EntityNotFoundException {
    Person p = new Person();
    p.setName(new Name());
    p.getName().setFirst("jimmy");
    p.getName().setLast("jam");
    p.setAnotherName(new Name());
    p.getAnotherName().setFirst("anotherjimmy");
    p.getAnotherName().setLast("anotherjam");
    makePersistentInTxn(p);

    assertNotNull(p.getId());

    beginTxn();
    p = pm.getObjectById(Person.class, p.getId());
    p.getName().setLast("not jam");
    commitTxn();

    Entity entity = ldth.ds.get(KeyFactory.stringToKey(p.getId()));
    assertNotNull(entity);
    assertEquals("jimmy", entity.getProperty("first"));
    assertEquals("not jam", entity.getProperty("last"));
    assertEquals("anotherjimmy", entity.getProperty("anotherFirst"));
    assertEquals("anotherjam", entity.getProperty("anotherLast"));
  }

  public void testUpdateStrPrimaryKey_SetNewName() {
    Key key = ldth.ds.put(Flight.newFlightEntity("name", "bos", "mia", 3, 4, 44));

    beginTxn();
    Flight f = pm.getObjectById(Flight.class, key.getId());
    f.setId("other");
    pm.makePersistent(f);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUpdateStrPrimaryKey_SetNewKey() {
    Key key = ldth.ds.put(Flight.newFlightEntity("name", "bos", "mia", 3, 4, 44));

    beginTxn();
    Flight f = pm.getObjectById(Flight.class, key.getId());
    f.setId(KeyFactory.keyToString(KeyFactory.createKey(key.getKind(), 33)));
    pm.makePersistent(f);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUpdateStrPrimaryKey_NullKey() {
    Key key = ldth.ds.put(Flight.newFlightEntity("name", "bos", "mia", 3, 4, 44));

    beginTxn();
    Flight f = pm.getObjectById(Flight.class, key.getId());
    f.setId(null);
    pm.makePersistent(f);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUpdateStrAncestor_SetNewName() {
    Key parentKey = ldth.ds.put(new Entity(String.class.getSimpleName()));
    Key key = ldth.ds.put(new Entity(HasAncestorJDO.class.getSimpleName(), parentKey));

    beginTxn();
    HasAncestorJDO pojo = pm.getObjectById(HasAncestorJDO.class, KeyFactory.keyToString(key));
    pojo.setAncestorId("other");
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUpdateStrAncestor_SetNewKey() {
    Key parentKey = ldth.ds.put(new Entity(Flight.class.getSimpleName()));
    Key key = ldth.ds.put(new Entity(HasAncestorJDO.class.getSimpleName(), parentKey));

    beginTxn();
    HasAncestorJDO pojo = pm.getObjectById(HasAncestorJDO.class, KeyFactory.keyToString(key));
    pojo.setAncestorId(KeyFactory.keyToString(KeyFactory.createKey(key.getKind(), 33)));

    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUpdateStrAncestor_NullKey() {
    Key parentKey = ldth.ds.put(new Entity(Flight.class.getSimpleName()));
    Key key = ldth.ds.put(new Entity(HasAncestorJDO.class.getSimpleName(), parentKey));

    beginTxn();
    HasAncestorJDO pojo = pm.getObjectById(HasAncestorJDO.class, KeyFactory.keyToString(key));
    pojo.setAncestorId(null);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUpdateKeyAncestor_SetNewKey() {
    Key parentKey = ldth.ds.put(new Entity(Flight.class.getSimpleName()));
    Key key = ldth.ds.put(new Entity(HasKeyAncestorKeyStringPkJDO.class.getSimpleName(), parentKey));

    beginTxn();
    HasKeyAncestorKeyStringPkJDO pojo = pm.getObjectById(
        HasKeyAncestorKeyStringPkJDO.class, KeyFactory.keyToString(key));
    pojo.setAncestorKey(KeyFactory.createKey(key.getKind(), 33));

    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testUpdateKeyAncestor_NullKey() {
    Key parentKey = ldth.ds.put(new Entity(Flight.class.getSimpleName()));
    Key key = ldth.ds.put(new Entity(HasKeyAncestorKeyStringPkJDO.class.getSimpleName(), parentKey));

    beginTxn();
    HasKeyAncestorKeyStringPkJDO pojo = pm.getObjectById(
        HasKeyAncestorKeyStringPkJDO.class, KeyFactory.keyToString(key));
    pojo.setAncestorKey(null);
    try {
      commitTxn();
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      rollbackTxn();
    }
  }
}
