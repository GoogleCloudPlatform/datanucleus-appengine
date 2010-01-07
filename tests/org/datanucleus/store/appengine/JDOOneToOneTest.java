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
import org.datanucleus.test.HasEncodedStringPkJDO;
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
    testInsert_NewParentAndChild(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsert_NewParentAndChild(NEW_PM_START_END);
  }
  private void testInsert_NewParentAndChild(StartEnd startEnd) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();
    HasEncodedStringPkJDO notDependent = new HasEncodedStringPkJDO();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParentKeyPk.setParent(pojo);
    pojo.setNotDependent(notDependent);

    makePersistentInTxn(pojo, startEnd);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(notDependent.getId());
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

    Entity notDependentEntity = ldth.ds.get(KeyFactory.stringToKey(notDependent.getId()));
    assertNotNull(notDependentEntity);

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    assertCountsInDatastore(1, 1);
    assertEquals(1, countForClass(notDependent.getClass()));
  }

  public void testInsert_NewParentExistingChild_Unidirectional() throws EntityNotFoundException {
    testInsert_NewParentExistingChild_Unidirectional(TXN_START_END);
  }
  public void testInsert_NewParentExistingChild_Unidirectional_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsert_NewParentExistingChild_Unidirectional(NEW_PM_START_END);
  }
  private void testInsert_NewParentExistingChild_Unidirectional(StartEnd startEnd) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();

    startEnd.start();
    pm.makePersistent(f);
    startEnd.end();
    startEnd.start();
    pm.makePersistent(hasKeyPk);
    startEnd.end();
    startEnd.start();
    pm.makePersistent(hasParent);
    startEnd.end();
    startEnd.start();
    pm.makePersistent(hasParentKeyPk);
    startEnd.end();
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());

    HasOneToOneJDO pojo = new HasOneToOneJDO();

    startEnd.start();
    f = pm.makePersistent(f);
    hasKeyPk = pm.makePersistent(hasKeyPk);
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    try {
      // this fails because it attempts to establish a parent for an object
      // that was originally saved without a parent
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      if (pm.currentTransaction().isActive()) {
        rollbackTxn();
      }
    }
    assertCountsInDatastore(0, 1);
  }

  public void testInsert_NewParentExistingChild_Bidirectional() throws EntityNotFoundException {
    testInsert_NewParentExistingChild_Bidirectional(TXN_START_END);
  }
  public void testInsert_NewParentExistingChild_Bidirectional_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsert_NewParentExistingChild_Bidirectional(NEW_PM_START_END);
  }
  private void testInsert_NewParentExistingChild_Bidirectional(StartEnd startEnd) throws EntityNotFoundException {
    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();

    makePersistentInTxn(f, startEnd);
    makePersistentInTxn(hasKeyPk, startEnd);
    makePersistentInTxn(hasParent, startEnd);
    makePersistentInTxn(hasParentKeyPk, startEnd);
    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    pojo.setHasParentKeyPK(hasParentKeyPk);

    startEnd.start();
    try {
      // this fails because it tries to establish a parent for an object that
      // was originally saved without a parent
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      if (pm.currentTransaction().isActive()) {
        rollbackTxn();
      }
    }
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    testInsert_ExistingParentNewChild(TXN_START_END);
  }
  public void testInsert_ExistingParentNewChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsert_ExistingParentNewChild(NEW_PM_START_END);
  }
  private void testInsert_ExistingParentNewChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToOneJDO pojo = new HasOneToOneJDO();

    startEnd.start();
    pm.makePersistent(pojo);
    assertNotNull(pojo.getId());
    assertNull(pojo.getFlight());
    assertNull(pojo.getHasKeyPK());
    assertNull(pojo.getHasParent());
    assertNull(pojo.getHasParentKeyPK());
    pojo = pm.detachCopy(pojo);
    startEnd.end();
    
    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();
    startEnd.start();
    pojo = pm.makePersistent(pojo);
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParent.setParent(pojo);
    startEnd.end();

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

  // These tests fail because we are inserting a new child object as part
  // of attaching the parent.  Since we're attaching the parent, the children
  // get inserted first, which associates the entity group of the children
  // with the txn and then causes an exception when we go to update the
  // parent.  We need some way to force the parent to update first.
  public void fail_testInsert_ExistingParentNewChild_UpdateDetached() throws EntityNotFoundException {
    testInsert_ExistingParentNewChild_UpdateDetached(TXN_START_END);
  }
  public void fail_testInsert_ExistingParentNewChild_UpdateDetached_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsert_ExistingParentNewChild_UpdateDetached(NEW_PM_START_END);
  }
  private void testInsert_ExistingParentNewChild_UpdateDetached(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToOneJDO pojo = new HasOneToOneJDO();

    startEnd.start();
    pm.makePersistent(pojo);
    assertNotNull(pojo.getId());
    assertNull(pojo.getFlight());
    assertNull(pojo.getHasKeyPK());
    assertNull(pojo.getHasParent());
    assertNull(pojo.getHasParentKeyPK());
    pojo = pm.detachCopy(pojo);
    startEnd.end();

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);

    Flight f = newFlight();
    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    HasOneToOneParentJDO hasParent = new HasOneToOneParentJDO();
    HasOneToOneParentKeyPkJDO hasParentKeyPk = new HasOneToOneParentKeyPkJDO();
    startEnd.start();
    pojo.setFlight(f);
    pojo.setHasKeyPK(hasKeyPk);
    pojo.setHasParent(hasParent);
    hasParent.setParent(pojo);
    pojo.setHasParentKeyPK(hasParentKeyPk);
    hasParent.setParent(pojo);
    pojo = pm.makePersistent(pojo);
    startEnd.end();

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
    testUpdate_UpdateChildWithMerge(TXN_START_END);
  }
  public void testUpdate_UpdateChildWithMerge_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdate_UpdateChildWithMerge(NEW_PM_START_END);
  }
  private void testUpdate_UpdateChildWithMerge(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(pojo.getId());
    f = pm.detachCopy(f);
    hasKeyPk = pm.detachCopy(hasKeyPk);
    hasParent = pm.detachCopy(hasParent);
    hasParentKeyPk = pm.detachCopy(hasParentKeyPk);
    startEnd.end();
    startEnd.start();
    f.setOrigin("yam");
    hasKeyPk.setStr("yar");
    hasParent.setStr("yag");
    hasParentKeyPk.setStr("yap");
    f = pm.makePersistent(f);
    hasKeyPk = pm.makePersistent(hasKeyPk);
    hasParent = pm.makePersistent(hasParent);
    hasParentKeyPk = pm.makePersistent(hasParentKeyPk);
    startEnd.end();

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
    testUpdate_UpdateChild(TXN_START_END);
  }
  public void testUpdate_UpdateChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdate_UpdateChild(NEW_PM_START_END);
  }
  public void testUpdate_UpdateChild(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    pm.makePersistent(pojo);

    assertNotNull(f.getId());
    assertNotNull(hasKeyPk.getKey());
    assertNotNull(hasParentKeyPk.getKey());
    assertNotNull(hasParent.getKey());
    assertNotNull(pojo.getId());
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(HasOneToOneJDO.class, pojo.getId());
    pojo.getFlight().setOrigin("yam");
    pojo.getHasKeyPK().setStr("yar");
    pojo.getHasParent().setStr("yag");
    pojo.getHasParentKeyPK().setStr("yap");
    startEnd.end();

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
    testUpdate_NullOutChild(TXN_START_END);
  }
  public void testUpdate_NullOutChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdate_NullOutChild(NEW_PM_START_END);
  }
  private void testUpdate_NullOutChild(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
    pm.makePersistent(pojo);
    String flightId = f.getId();
    Key hasKeyPkKey = hasKeyPk.getKey();
    String hasParentKey = hasParent.getKey();
    Key hasParentKeyPkKey = hasParentKeyPk.getKey();
    startEnd.end();

    startEnd.start();
    try {
      pojo = pm.makePersistent(pojo);
      pojo.setFlight(null);
      pojo.setHasKeyPK(null);
      pojo.setHasParent(null);
      pojo.setHasParentKeyPK(null);
    } finally {
      startEnd.end();
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

    ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertCountsInDatastore(1, 0);
  }

  public void testFind() throws EntityNotFoundException {
    testFind(TXN_START_END);
  }
  public void testFind_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testFind(NEW_PM_START_END);
  }
  private void testFind(StartEnd startEnd) throws EntityNotFoundException {
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

    startEnd.start();
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
    startEnd.end();
  }

  public void testQuery() {
    testQuery(TXN_START_END);
  }
  public void testQuery_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testQuery(NEW_PM_START_END);
  }
  public void testQuery(StartEnd startEnd) {
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
    startEnd.start();
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
    startEnd.end();
  }


  public void testChildFetchedLazily() throws Exception {
    // force a new pmf to get created after we've installed our own
    // DatastoreService mock
    pmf.close();
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
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
      // need to close the pmf before we restore the original datastore service
      pmf.close();
      DatastoreServiceFactoryInternal.setDatastoreService(null);
    }
    EasyMock.verify(ds);
    EasyMock.verify(txn);
  }

  public void testDeleteParentDeletesChild() {
    testDeleteParentDeletesChild(TXN_START_END);
  }
  public void testDeleteParentDeletesChild_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testDeleteParentDeletesChild(NEW_PM_START_END);
  }
  private void testDeleteParentDeletesChild(StartEnd startEnd) {
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

    Entity notDependentEntity = new Entity(HasEncodedStringPkJDO.class.getSimpleName(), pojoEntity.getKey());
    ldth.ds.put(notDependentEntity);

    startEnd.start();
    HasOneToOneJDO pojo = pm.getObjectById(HasOneToOneJDO.class, KeyFactory.keyToString(pojoEntity.getKey()));
    pm.deletePersistent(pojo);
    startEnd.end();
    assertCountsInDatastore(0, 0);
    assertEquals(1, countForClass(HasEncodedStringPkJDO.class));
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
    testChangeParent(TXN_START_END);
  }
  public void testChangeParent_NoTxn() {
    testChangeParent(NEW_PM_START_END);
  }
  private void testChangeParent(StartEnd startEnd) {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    Flight f1 = newFlight();

    HasOneToOneJDO pojo = new HasOneToOneJDO();
    pojo.setFlight(f1);
    startEnd.start();
    pm.makePersistent(pojo);
    f1 = pm.detachCopy(f1);
    startEnd.end();

    HasOneToOneJDO pojo2 = new HasOneToOneJDO();
    startEnd.start();
    pojo2.setFlight(f1);
    try {
      pm.makePersistent(pojo2);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      if (pm.currentTransaction().isActive()) {
        rollbackTxn();
      }
    }
  }

  public void testNewParentNewChild_SetNamedKeyOnChild() throws EntityNotFoundException {
    testNewParentNewChild_SetNamedKeyOnChild(TXN_START_END);
  }
  public void testNewParentNewChild_SetNamedKeyOnChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testNewParentNewChild_SetNamedKeyOnChild(NEW_PM_START_END);
  }
  private void testNewParentNewChild_SetNamedKeyOnChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToOneJDO pojo = new HasOneToOneJDO();
    Flight f1 = newFlight();
    pojo.setFlight(f1);
    f1.setId(KeyFactory.keyToString(KeyFactory.createKey(Flight.class.getSimpleName(), "named key")));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    Entity flightEntity = ldth.ds.get(KeyFactory.stringToKey(f1.getId()));
    assertEquals("named key", flightEntity.getKey().getName());
  }

  public void testNewParentNewChild_LongKeyOnParent() throws EntityNotFoundException {
    testNewParentNewChild_LongKeyOnParent(TXN_START_END);
  }
  public void testNewParentNewChild_LongKeyOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testNewParentNewChild_LongKeyOnParent(NEW_PM_START_END);
  }
  private void testNewParentNewChild_LongKeyOnParent(StartEnd startEnd) throws EntityNotFoundException {
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

    makePersistentInTxn(pojo, startEnd);

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
    testNewParentNewChild_StringKeyOnParent(TXN_START_END);
  }
  public void testNewParentNewChild_StringKeyOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testNewParentNewChild_StringKeyOnParent(NEW_PM_START_END);
  }
  private void testNewParentNewChild_StringKeyOnParent(StartEnd startEnd) throws EntityNotFoundException {
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

    makePersistentInTxn(pojo, startEnd);

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

    assertEquals(0, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Flight.class));
  }

  public void testChildAtMultipleLevels() {
    testChildAtMultipleLevels(TXN_START_END);
  }
  public void testChildAtMultipleLevels_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    pm.getFetchPlan().setMaxFetchDepth(3);
    testChildAtMultipleLevels(NEW_PM_START_END);
  }
  private void testChildAtMultipleLevels(StartEnd startEnd) {
    HasOneToOneChildAtMultipleLevelsJDO pojo = new HasOneToOneChildAtMultipleLevelsJDO();
    Flight f1 = new Flight();
    pojo.setFlight(f1);
    HasOneToOneChildAtMultipleLevelsJDO child = new HasOneToOneChildAtMultipleLevelsJDO();
    Flight f2 = new Flight();
    child.setFlight(f2);
    pojo.setChild(child);
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.getObjectById(HasOneToOneChildAtMultipleLevelsJDO.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(child.getFlight(), f2);
    startEnd.end();
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

}
