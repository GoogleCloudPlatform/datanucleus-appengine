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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.repackaged.com.google.common.collect.Lists;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.BidirectionalChildJDO;
import org.datanucleus.test.BidirectionalChildListJDO;
import org.datanucleus.test.BidirectionalChildLongPkListJDO;
import org.datanucleus.test.BidirectionalChildStringPkListJDO;
import org.datanucleus.test.BidirectionalChildUnencodedStringPkListJDO;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasChildWithSeparateNameFieldJDO;
import org.datanucleus.test.HasEncodedStringPkSeparateNameFieldJDO;
import org.datanucleus.test.HasKeyPkJDO;
import org.datanucleus.test.HasLongPkOneToManyBidirChildrenJDO;
import org.datanucleus.test.HasMultipleBidirChildrenJDO;
import org.datanucleus.test.HasOneToManyChildAtMultipleLevelsJDO;
import org.datanucleus.test.HasOneToManyKeyPkListJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.HasOneToManyListWithOrderByJDO;
import org.datanucleus.test.HasOneToManyLongPkListJDO;
import org.datanucleus.test.HasOneToManyStringPkListJDO;
import org.datanucleus.test.HasOneToManyUnencodedStringPkListJDO;
import org.datanucleus.test.HasUnencodedStringPkOneToManyBidirChildrenJDO;

import java.util.Collection;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManyListTest extends JDOOneToManyTestCase {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    testInsertNewParentAndChild(TXN_START_END);
  }
  public void testInsertNewParentAndChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsertNewParentAndChild(NEW_PM_START_END);
  }
  private void testInsertNewParentAndChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    testInsert_NewParentAndChild(parent, bidirChild, startEnd);
  }

  public void testInsertExistingParentNewChild() throws EntityNotFoundException {
    testInsertExistingParentNewChild(TXN_START_END);
  }
  public void testInsertExistingParentNewChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsertExistingParentNewChild(NEW_PM_START_END);
  }
  private void testInsertExistingParentNewChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    testInsert_ExistingParentNewChild(parent, bidirChild, startEnd);
  }

  public void testSwapAtPosition() throws EntityNotFoundException {
    testSwapAtPosition(TXN_START_END);
  }
  public void testSwapAtPosition_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testSwapAtPosition(NEW_PM_START_END);
  }
  private void testSwapAtPosition(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    testSwapAtPosition(parent, bidirChild, bidirChild2, startEnd);
  }

  public void testRemoveAtPosition() throws EntityNotFoundException {
    testRemoveAtPosition(TXN_START_END);
  }
  public void testRemoveAtPosition_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testRemoveAtPosition(NEW_PM_START_END);
  }
  private void testRemoveAtPosition(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild3 = new BidirectionalChildListJDO();
    testRemoveAtPosition(parent, bidirChild, bidirChild2, bidirChild3, startEnd);
  }

  public void testAddAtPosition() throws EntityNotFoundException {
    testAddAtPosition(TXN_START_END);
  }
  public void testAddAtPosition_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testAddAtPosition(NEW_PM_START_END);
  }
  private void testAddAtPosition(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidirChild = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    testAddAtPosition(parent, bidirChild, bidirChild2, startEnd);
  }

  public void testUpdateUpdateChildWithMerge() throws EntityNotFoundException {
    testUpdateUpdateChildWithMerge(TXN_START_END);
  }
  public void testUpdateUpdateChildWithMerge_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdateUpdateChildWithMerge(NEW_PM_START_END);
  }
  private void testUpdateUpdateChildWithMerge(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_UpdateChildWithMerge(pojo, bidir, startEnd);
  }

  public void testUpdateUpdateChild() throws EntityNotFoundException {
    testUpdateUpdateChild(TXN_START_END);
  }
  public void testUpdateUpdateChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdateUpdateChild(NEW_PM_START_END);
  }
  private void testUpdateUpdateChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_UpdateChild(pojo, bidir, startEnd);
  }

  public void testUpdateNullOutChildren() throws EntityNotFoundException {
    testUpdateNullOutChildren(TXN_START_END);
  }
  public void testUpdateNullOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdateNullOutChildren(NEW_PM_START_END);
  }
  private void testUpdateNullOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_NullOutChildren(pojo, bidir, startEnd);
  }

  public void testUpdateClearOutChildren() throws EntityNotFoundException {
    testUpdateClearOutChildren(TXN_START_END);
  }
  public void testUpdateClearOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdateClearOutChildren(NEW_PM_START_END);
  }
  private void testUpdateClearOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir = new BidirectionalChildListJDO();
    testUpdate_ClearOutChildren(pojo, bidir, startEnd);
  }

  public void testFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(TXN_START_END);
  }
  public void testFindWithOrderBy_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testFindWithOrderBy(NEW_PM_START_END);
  }
  private void testFindWithOrderBy(StartEnd startEnd) throws EntityNotFoundException {
    testFindWithOrderBy(HasOneToManyListWithOrderByJDO.class, startEnd);
  }

  public void testSaveWithOrderBy() throws EntityNotFoundException {
    testSaveWithOrderBy(TXN_START_END);
  }
  public void testSaveWithOrderBy_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testSaveWithOrderBy(NEW_PM_START_END);
  }
  private void testSaveWithOrderBy(StartEnd startEnd) throws EntityNotFoundException {
    testSaveWithOrderBy(new HasOneToManyListWithOrderByJDO(), startEnd);
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
    testFind(HasOneToManyListJDO.class, BidirectionalChildListJDO.class, startEnd);
  }

  public void testQuery() throws EntityNotFoundException {
    testQuery(TXN_START_END);
  }
  public void testQuery_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testQuery(NEW_PM_START_END);
  }
  private void testQuery(StartEnd startEnd) throws EntityNotFoundException {
    testQuery(HasOneToManyListJDO.class, BidirectionalChildListJDO.class, startEnd);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyListJDO.class, BidirectionalChildListJDO.class);
  }

  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(TXN_START_END);
  }
  public void testDeleteParentDeletesChild_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testDeleteParentDeletesChild(NEW_PM_START_END);
  }
  private void testDeleteParentDeletesChild(StartEnd startEnd) throws Exception {
    testDeleteParentDeletesChild(HasOneToManyListJDO.class, BidirectionalChildListJDO.class, startEnd);
  }

  public void testIndexOf() throws Exception {
    testIndexOf(TXN_START_END);
  }
  public void testIndexOf_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testIndexOf(NEW_PM_START_END);
  }
  public void testIndexOf(StartEnd startEnd) throws Exception {
    HasOneToManyListJDO pojo = new HasOneToManyListJDO();
    BidirectionalChildListJDO bidir1 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidir2 = new BidirectionalChildListJDO();

    Flight f1 = newFlight();
    Flight f2 = newFlight();

    HasKeyPkJDO hasKeyPk1 = new HasKeyPkJDO();
    HasKeyPkJDO hasKeyPk2 = new HasKeyPkJDO();

    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addFlight(f1);
    pojo.addFlight(f2);
    pojo.addHasKeyPk(hasKeyPk1);
    pojo.addHasKeyPk(hasKeyPk2);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(HasOneToManyListJDO.class, pojo.getId());
    bidir1 = pm.getObjectById(bidir1.getClass(), bidir1.getId());
    bidir2 = pm.getObjectById(bidir2.getClass(), bidir2.getId());
    f1 = pm.getObjectById(f1.getClass(), f1.getId());
    f2 = pm.getObjectById(f2.getClass(), f2.getId());
    hasKeyPk1 = pm.getObjectById(hasKeyPk1.getClass(), hasKeyPk1.getKey());
    hasKeyPk2 = pm.getObjectById(hasKeyPk2.getClass(), hasKeyPk2.getKey());
    assertEquals(0, pojo.getBidirChildren().indexOf(bidir1));
    assertEquals(1, pojo.getBidirChildren().indexOf(bidir2));
    assertEquals(0, pojo.getFlights().indexOf(f1));
    assertEquals(1, pojo.getFlights().indexOf(f2));
    assertEquals(0, pojo.getHasKeyPks().indexOf(hasKeyPk1));
    assertEquals(1, pojo.getHasKeyPks().indexOf(hasKeyPk2));
    startEnd.end();
  }

  public void testRemoveAll() throws EntityNotFoundException {
    testRemoveAll(TXN_START_END);
  }
  public void testRemoveAll_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testRemoveAll(NEW_PM_START_END);
  }
  private void testRemoveAll(StartEnd startEnd) throws EntityNotFoundException {
    testRemoveAll(new HasOneToManyListJDO(), new BidirectionalChildListJDO(),
                  new BidirectionalChildListJDO(), new BidirectionalChildListJDO(), startEnd);
  }

  public void testRemoveAll_LongPkOnParent() throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(TXN_START_END);
  }
  public void testRemoveAll_LongPkOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testRemoveAll_LongPkOnParent(NEW_PM_START_END);
  }
  private void testRemoveAll_LongPkOnParent(StartEnd startEnd) throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(new HasOneToManyLongPkListJDO(), new BidirectionalChildLongPkListJDO(),
                  new BidirectionalChildLongPkListJDO(), new BidirectionalChildLongPkListJDO(),
                  startEnd);
  }

  public void testRemoveAll_UnencodedStringPkOnParent() throws EntityNotFoundException {
    testRemoveAll_UnencodedStringPkOnParent(TXN_START_END);
  }
  public void testRemoveAll_UnencodedStringPkOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testRemoveAll_UnencodedStringPkOnParent(NEW_PM_START_END);
  }
  private void testRemoveAll_UnencodedStringPkOnParent(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyUnencodedStringPkListJDO parent = new HasOneToManyUnencodedStringPkListJDO();
    parent.setId("parent id");
    testRemoveAll_UnencodedStringPkOnParent(parent, new BidirectionalChildUnencodedStringPkListJDO(),
                  new BidirectionalChildUnencodedStringPkListJDO(), new BidirectionalChildUnencodedStringPkListJDO(),
                  startEnd);
  }

  public void testChangeParent() {
    testChangeParent(TXN_START_END);
  }
  public void testChangeParent_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testChangeParent(NEW_PM_START_END);
  }
  private void testChangeParent(StartEnd startEnd) {
    testChangeParent(new HasOneToManyListJDO(), new HasOneToManyListJDO(), startEnd);
  }

  public void testNewParentNewChild_NamedKeyOnChild() throws EntityNotFoundException {
    testNewParentNewChild_NamedKeyOnChild(TXN_START_END);
  }
  public void testNewParentNewChild_NamedKeyOnChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testNewParentNewChild_NamedKeyOnChild(NEW_PM_START_END);
  }
  private void testNewParentNewChild_NamedKeyOnChild(StartEnd startEnd) throws EntityNotFoundException {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyListJDO(), startEnd);
  }

  public void testInsert_NewParentAndChild_LongPk() throws EntityNotFoundException {
    testInsert_NewParentAndChild_LongPk(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_LongPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsert_NewParentAndChild_LongPk(NEW_PM_START_END);
  }
  private void testInsert_NewParentAndChild_LongPk(StartEnd startEnd) throws EntityNotFoundException {
    BidirectionalChildLongPkListJDO bidirChild = new BidirectionalChildLongPkListJDO();
    bidirChild.setChildVal("yam");

    Flight f = newFlight();

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");

    HasOneToManyLongPkListJDO parent = new HasOneToManyLongPkListJDO();
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
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX_longpk"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX_longpk"));
    }

    Entity parentEntity = ds.get(TestUtils.createKey(parent, parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Lists.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Lists.newArrayList(flightEntity.getKey()), parentEntity.getProperty("flights"));
    assertEquals(Lists.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertEquals(HasOneToManyLongPkListJDO.class.getName(), 1, countForClass(HasOneToManyLongPkListJDO.class));
    assertEquals(BidirectionalChildLongPkListJDO.class.getName(), 1, countForClass(
        BidirectionalChildLongPkListJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));
    assertEquals(HasKeyPkJDO.class.getName(), 1, countForClass(HasKeyPkJDO.class));
  }

  public void testInsert_NewParentAndChild_StringPk() throws EntityNotFoundException {
    testInsert_NewParentAndChild_StringPk(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_StringPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsert_NewParentAndChild_StringPk(NEW_PM_START_END);
  }
  private void testInsert_NewParentAndChild_StringPk(StartEnd startEnd) throws EntityNotFoundException {
    BidirectionalChildStringPkListJDO bidirChild = new BidirectionalChildStringPkListJDO();
    bidirChild.setChildVal("yam");

    Flight f = newFlight();

    HasKeyPkJDO hasKeyPk = new HasKeyPkJDO();
    hasKeyPk.setStr("yag");

    HasOneToManyStringPkListJDO parent = new HasOneToManyStringPkListJDO();
    parent.setId("yar");
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
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity flightEntity = ds.get(KeyFactory.stringToKey(f.getId()));
    assertNotNull(flightEntity);
    assertEquals("bos", flightEntity.getProperty("origin"));
    assertEquals("mia", flightEntity.getProperty("dest"));
    assertEquals("jimmy", flightEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(f.getId()), flightEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), flightEntity, f.getId());
    if (isIndexed()) {
      assertEquals(0L, flightEntity.getProperty("flights_INTEGER_IDX"));
    }

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getKey());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getKey(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getKey());
    if (isIndexed()) {
      assertEquals(0L, hasKeyPkEntity.getProperty("hasKeyPks_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(TestUtils.createKey(parent, parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Lists.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Lists.newArrayList(flightEntity.getKey()), parentEntity.getProperty("flights"));
    assertEquals(Lists.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertEquals(HasOneToManyStringPkListJDO.class.getName(), 1, countForClass(
        HasOneToManyStringPkListJDO.class));
    assertEquals(BidirectionalChildStringPkListJDO.class.getName(), 1, countForClass(
        BidirectionalChildStringPkListJDO.class));
    assertEquals(Flight.class.getName(), 1, countForClass(Flight.class));
    assertEquals(HasKeyPkJDO.class.getName(), 1, countForClass(HasKeyPkJDO.class));
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSamePm() {
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManyListJDO());
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm() {
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManyListJDO());
  }

  public void testLongPkOneToManyBidirChildren() {
    testLongPkOneToManyBidirChildren(TXN_START_END);
  }
  public void testLongPkOneToManyBidirChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testLongPkOneToManyBidirChildren(NEW_PM_START_END);
  }
  private void testLongPkOneToManyBidirChildren(StartEnd startEnd) {
    HasLongPkOneToManyBidirChildrenJDO pojo = new HasLongPkOneToManyBidirChildrenJDO();
    HasLongPkOneToManyBidirChildrenJDO.ChildA
        a = new HasLongPkOneToManyBidirChildrenJDO.ChildA();
    pojo.setChildAList(Utils.newArrayList(a));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.getObjectById(HasLongPkOneToManyBidirChildrenJDO.class, pojo.getId());
    assertEquals(1, pojo.getChildAList().size());
    assertEquals(pojo, pojo.getChildAList().get(0).getParent());
    startEnd.end();
  }

  public void testUnencodedStringPkOneToManyBidirChildren() {
    testUnencodedStringPkOneToManyBidirChildren(TXN_START_END);
  }
  public void testUnencodedStringPkOneToManyBidirChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUnencodedStringPkOneToManyBidirChildren(NEW_PM_START_END);
  }
  private void testUnencodedStringPkOneToManyBidirChildren(StartEnd startEnd) {
    HasUnencodedStringPkOneToManyBidirChildrenJDO pojo = new HasUnencodedStringPkOneToManyBidirChildrenJDO();
    pojo.setId("yar");
    HasUnencodedStringPkOneToManyBidirChildrenJDO.ChildA
        a = new HasUnencodedStringPkOneToManyBidirChildrenJDO.ChildA();
    pojo.setChildAList(Utils.newArrayList(a));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.getObjectById(HasUnencodedStringPkOneToManyBidirChildrenJDO.class, pojo.getId());
    assertEquals(1, pojo.getChildAList().size());
    assertEquals(pojo, pojo.getChildAList().get(0).getParent());
    startEnd.end();
  }

  public void testFetchOfOneToManyParentWithKeyPk() {
    testFetchOfOneToManyParentWithKeyPk(TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithKeyPk_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testFetchOfOneToManyParentWithKeyPk(NEW_PM_START_END);
  }
  private void testFetchOfOneToManyParentWithKeyPk(StartEnd startEnd) {
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkListJDO(), startEnd);
  }

  public void testFetchOfOneToManyParentWithLongPk() {
    testFetchOfOneToManyParentWithLongPk(TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithLongPk_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testFetchOfOneToManyParentWithLongPk(NEW_PM_START_END);
  }
  private void testFetchOfOneToManyParentWithLongPk(StartEnd startEnd) {
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkListJDO(), startEnd);
  }

  public void testFetchOfOneToManyParentWithUnencodedStringPk() {
    testFetchOfOneToManyParentWithUnencodedStringPk(TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithUnencodedStringPk_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testFetchOfOneToManyParentWithUnencodedStringPk(NEW_PM_START_END);
  }
  private void testFetchOfOneToManyParentWithUnencodedStringPk(StartEnd startEnd) {
    testFetchOfOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkListJDO(), startEnd);
  }

  public void testAddChildToOneToManyParentWithLongPk() throws EntityNotFoundException {
    testAddChildToOneToManyParentWithLongPk(TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithLongPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testAddChildToOneToManyParentWithLongPk(NEW_PM_START_END);
  }
  private void testAddChildToOneToManyParentWithLongPk(StartEnd startEnd) throws EntityNotFoundException {
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyLongPkListJDO(), new BidirectionalChildLongPkListJDO(), startEnd);
  }

  public void testAddChildToOneToManyParentWithUnencodedStringPk() throws EntityNotFoundException {
    testAddChildToOneToManyParentWithUnencodedStringPk(TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithUnencodedStringPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testAddChildToOneToManyParentWithUnencodedStringPk(NEW_PM_START_END);
  }
  private void testAddChildToOneToManyParentWithUnencodedStringPk(StartEnd startEnd) throws EntityNotFoundException {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkListJDO(), new BidirectionalChildUnencodedStringPkListJDO(),
        startEnd);
  }

  public void testOneToManyChildAtMultipleLevels() {
    testOneToManyChildAtMultipleLevels(TXN_START_END);
  }
  public void testOneToManyChildAtMultipleLevels_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testOneToManyChildAtMultipleLevels(NEW_PM_START_END);
  }
  public void testOneToManyChildAtMultipleLevels(StartEnd startEnd) {
    HasOneToManyChildAtMultipleLevelsJDO pojo = new HasOneToManyChildAtMultipleLevelsJDO();
    Flight f1 = new Flight();
    pojo.setFlights(Utils.newArrayList(f1));
    HasOneToManyChildAtMultipleLevelsJDO child = new HasOneToManyChildAtMultipleLevelsJDO();
    Flight f2 = new Flight();
    child.setFlights(Utils.newArrayList(f2));
    pojo.setChild(child);
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    assertEquals(2, countForClass(Flight.class));
    pojo = pm.getObjectById(HasOneToManyChildAtMultipleLevelsJDO.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(1, pojo.getFlights().size());
    assertTrue(pojo.getFlights().get(0).customEquals(f1));
    assertTrue(child.getFlights().get(0).customEquals(f2));
    assertEquals(1, child.getFlights().size());
    startEnd.end();
  }

  public void testAddQueriedParentToBidirChild() throws EntityNotFoundException {
    testAddQueriedParentToBidirChild(TXN_START_END);
  }
  public void testAddQueriedParentToBidirChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testAddQueriedParentToBidirChild(NEW_PM_START_END);
  }
  private void testAddQueriedParentToBidirChild(StartEnd startEnd) throws EntityNotFoundException {
    testAddQueriedParentToBidirChild(
        new HasOneToManyListJDO(), new BidirectionalChildListJDO(), startEnd);
  }

  public void testAddFetchedParentToBidirChild() throws EntityNotFoundException {
    testAddFetchedParentToBidirChild(TXN_START_END);
  }
  public void testAddFetchedParentToBidirChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testAddFetchedParentToBidirChild(NEW_PM_START_END);
  }
  private void testAddFetchedParentToBidirChild(StartEnd startEnd) throws EntityNotFoundException {
    testAddFetchedParentToBidirChild(
        new HasOneToManyListJDO(), new BidirectionalChildListJDO(), startEnd);
  }

  public void testMultipleBidirChildren() {
    testMultipleBidirChildren(TXN_START_END);
  }
  public void testMultipleBidirChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testMultipleBidirChildren(NEW_PM_START_END);
  }
  private void testMultipleBidirChildren(StartEnd startEnd) {
    HasMultipleBidirChildrenJDO pojo = new HasMultipleBidirChildrenJDO();

    HasMultipleBidirChildrenJDO.BidirChild1 c1 = new HasMultipleBidirChildrenJDO.BidirChild1();
    HasMultipleBidirChildrenJDO.BidirChild2 c2 = new HasMultipleBidirChildrenJDO.BidirChild2();

    pojo.getChild1().add(c1);
    pojo.getChild2().add(c2);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.getObjectById(HasMultipleBidirChildrenJDO.class, pojo.getId());
    assertEquals(1, pojo.getChild1().size());
    assertEquals(1, pojo.getChild2().size());
    startEnd.end();
  }

  public void testReplaceBidirColl() {
    testReplaceBidirColl(TXN_START_END);
  }
  public void testReplaceBidirColl_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testReplaceBidirColl(NEW_PM_START_END);
  }
  private void testReplaceBidirColl(StartEnd startEnd) {
    Collection<BidirectionalChildJDO> childList = Utils.<BidirectionalChildJDO>newArrayList(
        new BidirectionalChildListJDO(), new BidirectionalChildListJDO());
    testReplaceBidirColl(
        new HasOneToManyListJDO(), new BidirectionalChildListJDO(), childList, startEnd);
  }

  public void testDeleteChildWithSeparateNameField() {
    testDeleteChildWithSeparateNameField(TXN_START_END);
  }
  public void testDeleteChildWithSeparateNameField_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testDeleteChildWithSeparateNameField(NEW_PM_START_END);
  }
  private void testDeleteChildWithSeparateNameField(StartEnd startEnd) {
    HasChildWithSeparateNameFieldJDO parent = new HasChildWithSeparateNameFieldJDO();
    HasEncodedStringPkSeparateNameFieldJDO child = new HasEncodedStringPkSeparateNameFieldJDO();
    child.setName("the name");
    parent.getChildren().add(child);
    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();
    startEnd.start();
    parent = pm.getObjectById(HasChildWithSeparateNameFieldJDO.class, parent.getId());
    pm.deletePersistent(parent);
    startEnd.end();
  }

  public void testOnlyOneParentPutOnParentAndChildUpdate() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(TXN_START_END);
  }
  public void testOnlyOneParentPutOnParentAndChildUpdate_NoTxn() throws Throwable {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testOnlyOneParentPutOnParentAndChildUpdate(NEW_PM_START_END);
  }
  private void testOnlyOneParentPutOnParentAndChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(
        new HasOneToManyListJDO(), new BidirectionalChildListJDO(), startEnd);
  }

  public void testOnlyOnePutOnChildUpdate() throws Throwable {
    testOnlyOnePutOnChildUpdate(TXN_START_END);
  }
  public void testOnlyOnePutOnChildUpdate_NoTxn() throws Throwable {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testOnlyOnePutOnChildUpdate(NEW_PM_START_END);
  }
  private void testOnlyOnePutOnChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOnePutOnChildUpdate(
        new HasOneToManyListJDO(), new BidirectionalChildListJDO(), startEnd);
  }

  public void testOnlyOneParentPutOnChildDelete() throws Throwable {
    testOnlyOneParentPutOnChildDelete(TXN_START_END);
  }
  public void testOnlyOneParentPutOnChildDelete_NoTxn() throws Throwable {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testOnlyOneParentPutOnChildDelete(NEW_PM_START_END);
  }
  private void testOnlyOneParentPutOnChildDelete(StartEnd startEnd) throws Throwable {
    testOnlyOneParentPutOnChildDelete(
        new HasOneToManyListJDO(), new BidirectionalChildListJDO(), startEnd);
  }

  public void testNonTxnAddOfChildToParentFailsPartwayThrough() throws Throwable {
    testNonTxnAddOfChildToParentFailsPartwayThrough(new HasOneToManyListJDO());
  }

  public void xtestRemove2ObjectsAtIndex() {
    testRemove2ObjectsAtIndex(TXN_START_END);
  }
  public void xtestRemove2ObjectsAtIndex_NoTxn() {
    testRemove2ObjectsAtIndex(NEW_PM_START_END);
  }
  private void testRemove2ObjectsAtIndex(StartEnd startEnd) {
    BidirectionalChildListJDO bidirChild1 = new BidirectionalChildListJDO();
    BidirectionalChildListJDO bidirChild2 = new BidirectionalChildListJDO();
    Flight f1 = newFlight();
    Flight f2 = newFlight();

    HasKeyPkJDO hasKeyPk1 = new HasKeyPkJDO();
    HasKeyPkJDO hasKeyPk2 = new HasKeyPkJDO();

    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    parent.addBidirChild(bidirChild1);
    bidirChild1.setParent(parent);
    parent.addBidirChild(bidirChild2);
    bidirChild2.setParent(parent);
    parent.addFlight(f1);
    parent.addFlight(f2);
    parent.addHasKeyPk(hasKeyPk1);
    parent.addHasKeyPk(hasKeyPk2);

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();

    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    parent.getFlights().remove(0);
    parent.getFlights().remove(0);
//    parent.getBidirChildren().remove(0);
//    parent.getBidirChildren().remove(0);
//    parent.getHasKeyPks().remove(0);
//    parent.getHasKeyPks().remove(0);
    startEnd.end();

    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertTrue(parent.getFlights().isEmpty());
//    assertTrue(parent.getBidirChildren().isEmpty());
//    assertTrue(parent.getHasKeyPks().isEmpty());
    startEnd.end();
  }

  @Override
  boolean isIndexed() {
    return true;
  }
}
