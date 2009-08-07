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
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasOneToOneChildAtMultipleLevelsJDO;
import org.datanucleus.test.HasOneToOneJDO;
import org.datanucleus.test.HasOneToOneLongPkJDO;
import org.datanucleus.test.HasOneToOneLongPkParentJDO;
import org.datanucleus.test.HasOneToOneLongPkParentKeyPkJDO;
import org.datanucleus.test.HasOneToOneParentJDO;
import org.datanucleus.test.HasOneToOneParentKeyPkJDO;
import org.datanucleus.test.HasOneToOneStringPkJDO;
import org.datanucleus.test.HasOneToOneStringPkParentJDO;
import org.datanucleus.test.HasOneToOneStringPkParentKeyPkJDO;
import org.easymock.EasyMock;

import java.util.List;

import javax.jdo.JDOFatalUserException;
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

    makePersistentInTxn(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(pojo.getId());

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals(2L, flightEntity.getProperty("me"));
    assertEquals(3L, flightEntity.getProperty("you"));
    assertEquals(44L, flightEntity.getProperty("flight_number"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.stringToKey(hasParent.getKey()), hasParentEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getKey());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getKey(), hasParentKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getKey());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_NewParentExistingChild_Unidirectional() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();

    persistInTxn(f);
    persistInTxn(hasKeyPk);
    persistInTxn(hasParent);
    persistInTxn(hasParentKeyPk);
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);

    beginTxn();
    try {
      // this fails because it attempts to establish a parent for an object
      // that was originally saved without a parent
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
    // can't assert this because the local datastore doesn't really support
    // txns so the parent ends up actually being persisted
    // assertCountsInDatastore(0, 1);
  }

  public void testInsert_NewParentExistingChild_Bidirectional() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();

    persistInTxn(f);
    persistInTxn(hasKeyPk);
    persistInTxn(hasParent);
    persistInTxn(hasParentKeyPk);
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    pojo.setHasParentKeyPK(hasParentKeyPk);

    beginTxn();
    try {
      // this fails because it tries to establish a parent for an object that
      // was originally saved without a parent
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    HasOneToOneJDO pojo = new HasOneToOneJDO();

    beginTxn();
    pm.makePersistent(pojo);
    assertNotNull(pojo.getId());
    assertNull(pojo.getFlight());
    assertNull(pojo.getHasKeyPK());
    assertNull(pojo.getHasParent());
    assertNull(pojo.getHasParentKeyPK());
    commitTxn();
    
    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();
    beginTxn();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParent.setParent(pojo);
    commitTxn();

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getKey());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentKeyPkEntity);
    assertKeyParentEquals(pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getKey());

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

    beginTxn();
    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(pojo.getId());
    commitTxn();
    beginTxn();
    f.setOrigin("yam");
    hasKeyPk.setStr("yar");
    hasParent.setStr("yag");
    hasParentKeyPk.setStr("yap");
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals("yag", hasParentEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getKey());

    Entity hasParentPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentPkEntity);
    assertEquals("yap", hasParentPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentPkEntity, hasParentKeyPk.getKey());

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

    beginTxn();
    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(pojo.getId());
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(HasOneToOneJDO.class, pojo.getId());
    pojo.getFlight().setOrigin("yam");
    pojo.getHasKeyPK().setStr("yar");
    pojo.getHasParent().setStr("yag");
    pojo.getHasParentKeyPK().setStr("yap");
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("yam", flightEntity.getProperty("origin"));
    assertKeyParentEquals(pojo.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals("yag", hasParentEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentEntity, hasParent.getKey());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals("yap", hasParentKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getKey());

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

    beginTxn();
    pm.makePersistent(pojo);
    String flightId = f.getId();
    Key hasKeyPkKey = hasKeyPk.getKey();
    String hasParentKey = hasParent.getKey();
    Key hasParentKeyPkKey = hasParentKeyPk.getKey();
    commitTxn();

    beginTxn();
    try {
      pojo.setFlight(null);
      pojo.setHasKeyPK(null);
      pojo.setHasParent(null);
      pojo.setHasParentKeyPK(null);
    } finally {
      commitTxn();
    }

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
      ldth.ds.get(KeyFactory.stringToKey(hasParentKey));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasParentKeyPkKey);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));

    assertCountsInDatastore(1, 0);
  }

  public void testFind() throws EntityNotFoundException {
    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = Flight.newFlightEntity(
        pojoEntity.getKey(), null, "jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity =
        new Entity(HasOneToOneParentJDO.class.getSimpleName(), pojoEntity.getKey());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity =
        new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    beginTxn();
    HasOneToOneJDO pojo =
        pm.getObjectById(HasOneToOneJDO.class, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getFlight());
    assertEquals("bos", pojo.getFlight().getOrigin());
    assertEquals("mia", pojo.getFlight().getDest());
    assertNotNull(pojo.getHasKeyPK());
    assertEquals("yar", pojo.getHasKeyPK().getStr());
    assertNotNull(pojo.getHasParent());
    assertEquals("yap", pojo.getHasParent().getStr());
    assertNotNull(pojo.getHasParentKeyPK());
    assertEquals(pojo, pojo.getHasParent().getParent());
    assertEquals("yag", pojo.getHasParentKeyPK().getStr());
    assertEquals(pojo, pojo.getHasParentKeyPK().getParent());
    commitTxn();
  }

  public void testQuery() {
    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = Flight.newFlightEntity(
        pojoEntity.getKey(), null, "jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity =
        new Entity(HasOneToOneParentJDO.class.getSimpleName(), pojoEntity.getKey());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentKeyPkEntity =
        new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasParentKeyPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentKeyPkEntity);

    Query q = pm.newQuery("select from " + HasOneToOneJDO.class.getName()
        + " where id == key parameters String key");
    beginTxn();
    @SuppressWarnings("unchecked")
    List<HasOneToOneJDO> result =
        (List<HasOneToOneJDO>) q.execute(KeyFactory.keyToString(pojoEntity.getKey()));
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
    commitTxn();
  }

  public void testChildFetchedLazily() throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    Transaction txn;
    try {
      setUp();

      Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
      ldth.ds.put(pojoEntity);

      Entity flightEntity = Flight.newFlightEntity(
          pojoEntity.getKey(), null, "jimmy", "bos", "mia", 5, 4, 33);
      ldth.ds.put(flightEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity hasParentEntity =
          new Entity(HasOneToOneParentJDO.class.getSimpleName(), pojoEntity.getKey());
      hasParentEntity.setProperty("str", "yap");
      ldth.ds.put(hasParentEntity);

      Entity hasParentKeyPkEntity =
          new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
      hasParentKeyPkEntity.setProperty("str", "yag");
      ldth.ds.put(hasParentKeyPkEntity);

      // the only get we're going to perform is for the pojo
      txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(txn.getId()).andReturn("1").times(2);
      txn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(txn);
      EasyMock.expect(ds.beginTransaction()).andReturn(txn);
      EasyMock.expect(ds.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      beginTxn();
      HasOneToOneJDO pojo = pm.getObjectById(HasOneToOneJDO.class, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
    EasyMock.verify(txn);
  }

  public void testDeleteParentDeletesChild() {
    Entity pojoEntity = new Entity(HasOneToOneJDO.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity flightEntity = new Entity(Flight.class.getSimpleName(), pojoEntity.getKey());
    Flight.addData(flightEntity, "jimmy", "bos", "mia", 5, 4, 33);
    ldth.ds.put(flightEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity hasParentEntity = new Entity(HasOneToOneParentJDO.class.getSimpleName(), pojoEntity.getKey());
    hasParentEntity.setProperty("str", "yap");
    ldth.ds.put(hasParentEntity);

    Entity hasParentPkEntity = new Entity(HasOneToOneParentKeyPkJDO.class.getSimpleName(), pojoEntity.getKey());
    hasParentPkEntity.setProperty("str", "yag");
    ldth.ds.put(hasParentPkEntity);

    beginTxn();
    HasOneToOneJDO pojo = pm.getObjectById(HasOneToOneJDO.class, KeyFactory.keyToString(pojoEntity.getKey()));
    pm.deletePersistent(pojo);
    commitTxn();
    assertCountsInDatastore(0, 0);
  }

  public void testNonTransactionalUpdate() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);

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

    pm.close();

    assertCountsInDatastore(1, 1);

    pm = pmf.getPersistenceManager();
    pojo = pm.getObjectById(HasOneToOneJDO.class, pojo.getId());
    pojo.setFlight(null);
    pojo.setHasKeyPK(null);
    pojo.setHasParent(null);
    pojo.setHasParentKeyPK(null);
    pm.close();
    pm = pmf.getPersistenceManager();
    
    assertCountsInDatastore(1, 0);
  }

  public void testChangeParent() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = newFlight();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f1);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    HasOneToOneJDO pojo2 = new HasOneToOneJDO();
    beginTxn();
    pojo2.setFlight(f1);
    try {
      pm.makePersistent(pojo2);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testNewParentNewChild_SetNamedKeyOnChild() throws EntityNotFoundException {
    HasOneToOneJDO pojo = new HasOneToOneJDO();
    Flight f1 = newFlight();
    pojo.setFlight(f1);
    f1.setId(KeyFactory.keyToString(KeyFactory.createKey(Flight.class.getSimpleName(), "named key")));
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    assertEquals("named key", flightEntity.getKey().getName());
  }

  public void testNewParentNewChild_LongKeyOnParent() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneLongPkParentJDO hasParent = new HasOneToOneLongPkParentJDO();
    HasOneToOneLongPkParentKeyPkJDO hasParentKeyPk = new HasOneToOneLongPkParentKeyPkJDO();

    HasOneToOneLongPkJDO pojo = new HasOneToOneLongPkJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    makePersistentInTxn(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(pojo.getId());

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals(2L, flightEntity.getProperty("me"));
    assertEquals(3L, flightEntity.getProperty("you"));
    assertEquals(44L, flightEntity.getProperty("flight_number"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.stringToKey(hasParent.getKey()), hasParentEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentEntity, hasParent.getKey());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getKey(), hasParentKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getKey());

    Entity pojoEntity = ldth.ds.get(TestUtils.createKey(pojo, pojo.getId()));
    assertNotNull(pojoEntity);

    assertEquals(1, countForClass(HasOneToOneLongPkJDO.class));
    assertEquals(1, countForClass(Flight.class));
    assertEquals(1, countForClass(HasKeyPkJDO.class));
    assertEquals(1, countForClass(HasOneToOneLongPkParentJDO.class));
    assertEquals(1, countForClass(HasOneToOneLongPkParentKeyPkJDO.class));
  }

  public void testNewParentNewChild_StringKeyOnParent() throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneStringPkParentJDO hasParent = new HasOneToOneStringPkParentJDO();
    HasOneToOneStringPkParentKeyPkJDO hasParentKeyPk = new HasOneToOneStringPkParentKeyPkJDO();

    HasOneToOneStringPkJDO pojo = new HasOneToOneStringPkJDO();
    pojo.setId("yar");
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);

    makePersistentInTxn(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(pojo.getId());

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals(2L, flightEntity.getProperty("me"));
    assertEquals(3L, flightEntity.getProperty("you"));
    assertEquals(44L, flightEntity.getProperty("flight_number"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), flightEntity, f.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasKeyPkEntity, hasKeyPk.getKey());

    Entity hasParentEntity = ldth.ds.get(KeyFactory.stringToKey(hasParent.getKey()));
    assertNotNull(hasParentEntity);
    assertEquals(KeyFactory.stringToKey(hasParent.getKey()), hasParentEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentEntity, hasParent.getKey());

    Entity hasParentKeyPkEntity = ldth.ds.get(hasParentKeyPk.getKey());
    assertNotNull(hasParentKeyPkEntity);
    assertEquals(hasParentKeyPk.getKey(), hasParentKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getClass(), pojo.getId(), hasParentKeyPkEntity, hasParentKeyPk.getKey());

    Entity pojoEntity = ldth.ds.get(TestUtils.createKey(pojo, pojo.getId()));
    assertNotNull(pojoEntity);

    assertEquals(1, countForClass(HasOneToOneStringPkJDO.class));
    assertEquals(1, countForClass(Flight.class));
    assertEquals(1, countForClass(HasKeyPkJDO.class));
    assertEquals(1, countForClass(HasOneToOneStringPkParentJDO.class));
    assertEquals(1, countForClass(HasOneToOneStringPkParentKeyPkJDO.class));
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = new Flight();
    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pm.makePersistent(f1);
    f1 = pm.detachCopy(f1);
    pm.close();
    pm = pmf.getPersistenceManager();
    pojo.setFlight(f1);
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
    assertNull(pojo.getFlight());
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSamePm() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Flight f1 = new Flight();
    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pm.makePersistent(f1);
    pojo.setFlight(f1);
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
    assertNull(pojo.getFlight());
  }

  public void testChildAtMultipleLevels() {
    HasOneToOneChildAtMultipleLevelsJDO pojo = new HasOneToOneChildAtMultipleLevelsJDO();
    Flight f1 = new Flight();
    pojo.setFlight(f1);
    HasOneToOneChildAtMultipleLevelsJDO child = new HasOneToOneChildAtMultipleLevelsJDO();
    Flight f2 = new Flight();
    child.setFlight(f2);
    pojo.setChild(child);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    pojo = pm.getObjectById(HasOneToOneChildAtMultipleLevelsJDO.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(child.getFlight(), f2);
    commitTxn();
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

  private void assertCountsInDatastore(int expectedParent, int expectedChildren) {
    assertEquals(expectedParent, countForClass(HasOneToOneJDO.class));
    assertEquals(expectedChildren, countForClass(Flight.class));
    assertEquals(expectedChildren, countForClass(HasKeyPkJDO.class));
    assertEquals(expectedChildren, countForClass(HasOneToOneParentJDO.class));
    assertEquals(expectedChildren, countForClass(HasOneToOneParentKeyPkJDO.class));
  }

  private void persistInTxn(Object obj) {
    beginTxn();
    pm.makePersistent(obj);
    commitTxn();
  }

}
