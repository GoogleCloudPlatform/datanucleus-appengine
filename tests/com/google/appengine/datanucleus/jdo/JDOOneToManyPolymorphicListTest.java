/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.PolymorphicTestUtils;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkListJDO;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirBottom;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirBottomLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirBottomLongPkChildKey;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirBottomStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirBottomUnencodedStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirBottomUnencodedStringPkChildKey;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirMiddle;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirMiddleLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirMiddleLongPkChildKey;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirMiddleStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirMiddleUnencodedStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirMiddleUnencodedStringPkChildKey;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirTop;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirTopLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirTopLongPkChildKey;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirTopStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirTopUnencodedStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildListJDO.BidirTopUnencodedStringPkChildKey;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyChildAtMultipleLevels;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyList;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListKeyPk;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListLongPk;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListLongPkChildKeyPk;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListStringPk;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListUnencodedStringPk;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListUnencodedStringPkChildKey;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyMultipleBidirChildren;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyWithSeparateNameFieldJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyWithUnsupportedInheritanceList;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJDO.HasPolymorphicRelationsListWithOrderByJDO;
import com.google.appengine.datanucleus.test.SubclassesJDO.CompleteTableParentNoChildStrategy;
import com.google.appengine.datanucleus.test.SubclassesJDO.CompleteTableParentWithCompleteTableChild;
import com.google.appengine.datanucleus.test.SubclassesJDO.CompleteTableParentWithNewTableChild;
import com.google.appengine.datanucleus.test.SubclassesJDO.CompleteTableParentWithSubclassTableChild;
import com.google.appengine.datanucleus.test.SubclassesJDO.NewTableParentWithCompleteTableChild;
import com.google.appengine.datanucleus.test.SubclassesJDO.NewTableParentWithNewTableChild;
import com.google.appengine.datanucleus.test.SubclassesJDO.NewTableParentWithSubclassTableChild;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirBottom;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirBottomEncodedStringPkSeparateNameField;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirMiddle;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirMiddleEncodedStringPkSeparateNameField;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTop;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTopEncodedStringPkSeparateNameField;
import com.google.appengine.repackaged.com.google.common.collect.Lists;

import junit.framework.Assert;


import java.util.Collection;

import org.datanucleus.store.ExecutionContext;

import static com.google.appengine.datanucleus.PolymorphicTestUtils.getEntityKind;

public class JDOOneToManyPolymorphicListTest extends JDOOneToManyPolymorphicTestCase {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    testInsertNewParentAndChild(TXN_START_END);
  }
  
  public void testInsertNewParentAndChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testInsertNewParentAndChild(NEW_PM_START_END);
  }
  
  private void testInsertNewParentAndChild(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    int expectedParent = 1, expectedChild = 1;
    testInsert_NewParentAndChild(new HasOneToManyList(), new BidirTop(), startEnd, UnidirLevel.Bottom,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
    testInsert_NewParentAndChild(new HasOneToManyList(), new BidirMiddle(), startEnd, UnidirLevel.Top,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
    testInsert_NewParentAndChild(new HasOneToManyList(), new BidirBottom(), startEnd, UnidirLevel.Middle,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
  }

  public void testInsertExistingParentNewChild() throws EntityNotFoundException {
    testInsertExistingParentNewChild(TXN_START_END);
  }
  public void testInsertExistingParentNewChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testInsertExistingParentNewChild(NEW_PM_START_END);
  }
  private void testInsertExistingParentNewChild(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    int expectedParent = 1, expectedChild = 1;
    testInsert_ExistingParentNewChild(new HasOneToManyList(), new BidirBottom(), startEnd, UnidirLevel.Middle,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
    testInsert_ExistingParentNewChild(new HasOneToManyList(), new BidirTop(), startEnd, UnidirLevel.Bottom,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
    testInsert_ExistingParentNewChild(new HasOneToManyList(), new BidirMiddle(), startEnd, UnidirLevel.Top,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
  }
  
  public void testSwapAtPosition() throws EntityNotFoundException {
    testSwapAtPosition(TXN_START_END);
  }
  public void testSwapAtPosition_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testSwapAtPosition(NEW_PM_START_END);
  }
  private void testSwapAtPosition(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    int expectedParent = 1, expectedChild = 1;

    testSwapAtPosition(new HasOneToManyList(), new BidirTop(), new BidirTop(), startEnd,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
    testSwapAtPosition(new HasOneToManyList(), new BidirTop(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
    testSwapAtPosition(new HasOneToManyList(), new BidirMiddle(), new BidirBottom(), startEnd,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
    testSwapAtPosition(new HasOneToManyList(), new BidirMiddle(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, expectedParent++, expectedChild++);
  }
  
  public void testRemoveAtPosition() throws EntityNotFoundException {
    testRemoveAtPosition(TXN_START_END);
  }
  public void testRemoveAtPosition_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testRemoveAtPosition(NEW_PM_START_END);
  }
  private void testRemoveAtPosition(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    int count = 1;

    testRemoveAtPosition(new HasOneToManyList(), new BidirTop(), new BidirMiddle(), new BidirTop(), startEnd,
	expectedBidirKind, expectedUnidirKind, count++);
    testRemoveAtPosition(new HasOneToManyList(), new BidirMiddle(), new BidirTop(), new BidirBottom(), startEnd,
	expectedBidirKind, expectedUnidirKind, count++);
    testRemoveAtPosition(new HasOneToManyList(), new BidirBottom(), new BidirBottom(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, count++);
  }

  public void testAddAtPosition() throws EntityNotFoundException {
    testAddAtPosition(TXN_START_END);
  }
  public void testAddAtPosition_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testAddAtPosition(NEW_PM_START_END);
  }
  private void testAddAtPosition(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    int count = 1;
    testAddAtPosition(new HasOneToManyList(), new BidirTop(), new BidirBottom(), startEnd,
	expectedBidirKind, expectedUnidirKind, count++);
    testAddAtPosition(new HasOneToManyList(), new BidirMiddle(), new BidirTop(), startEnd,
	expectedBidirKind, expectedUnidirKind, count++);
    testAddAtPosition(new HasOneToManyList(), new BidirBottom(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, count++);
  }

  public void testUpdateUpdateChildWithMerge() throws EntityNotFoundException {
    testUpdateUpdateChildWithMerge(TXN_START_END);
  }
  public void testUpdateUpdateChildWithMerge_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testUpdateUpdateChildWithMerge(NEW_PM_START_END);
  }
  private void testUpdateUpdateChildWithMerge(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    int count = 1;
    testUpdate_UpdateChildWithMerge(new HasOneToManyList(), new BidirTop(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Bottom, count++);
    testUpdate_UpdateChildWithMerge(new HasOneToManyList(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Top, count++);
    testUpdate_UpdateChildWithMerge(new HasOneToManyList(), new BidirBottom(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Middle, count++);
  }

  public void testUpdateUpdateChild() throws EntityNotFoundException {
    testUpdateUpdateChild(TXN_START_END);
  }
  public void testUpdateUpdateChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testUpdateUpdateChild(NEW_PM_START_END);
  }
  private void testUpdateUpdateChild(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    int count = 1;

    testUpdate_UpdateChild(new HasOneToManyList(), new BidirTop(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Middle, count++);
    testUpdate_UpdateChild(new HasOneToManyList(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Top, count++);
    testUpdate_UpdateChild(new HasOneToManyList(), new BidirBottom(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Bottom, count++);
  }

  public void testUpdateNullOutChildren() throws EntityNotFoundException {
    testUpdateNullOutChildren(TXN_START_END);
  }
  
  public void testUpdateNullOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testUpdateNullOutChildren(NEW_PM_DETACH_ON_CLOSE_START_END);
  }
  private void testUpdateNullOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    int count = 1;

    testUpdate_NullOutChildren(new HasOneToManyList(), new BidirBottom(), startEnd,
	UnidirLevel.Bottom, count++);
    testUpdate_NullOutChildren(new HasOneToManyList(), new BidirMiddle(), startEnd,
	UnidirLevel.Middle, count++);
    testUpdate_NullOutChildren(new HasOneToManyList(), new BidirTop(), startEnd,
	UnidirLevel.Top, count++);
  }
  
  public void testUpdateClearOutChildren() throws EntityNotFoundException {
    testUpdateClearOutChildren(TXN_START_END);
  }
  public void testUpdateClearOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testUpdateClearOutChildren(NEW_PM_DETACH_ON_CLOSE_START_END);
  }
  private void testUpdateClearOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    int count = 1;

    testUpdate_ClearOutChildren(new HasOneToManyList(), new BidirTop(), startEnd,
	UnidirLevel.Bottom, count++);
    testUpdate_ClearOutChildren(new HasOneToManyList(), new BidirBottom(), startEnd,
	UnidirLevel.Middle, count++);
    testUpdate_ClearOutChildren(new HasOneToManyList(), new BidirMiddle(), startEnd,
	UnidirLevel.Top, count++);
  }

  public void testFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(TXN_START_END);
  }
  public void testFindWithOrderBy_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testFindWithOrderBy(NEW_PM_START_END);
  }
  private void testFindWithOrderBy(StartEnd startEnd) throws EntityNotFoundException {
    testFindWithOrderBy(HasPolymorphicRelationsListWithOrderByJDO.class, startEnd);
  }
  
  public void testSaveWithOrderBy() throws EntityNotFoundException {
    testSaveWithOrderBy(TXN_START_END);
  }
  public void testSaveWithOrderBy_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testSaveWithOrderBy(NEW_PM_START_END);
  }
  private void testSaveWithOrderBy(StartEnd startEnd) throws EntityNotFoundException {
    testSaveWithOrderBy(new HasPolymorphicRelationsListWithOrderByJDO(), startEnd);
  }

  public void testFind() throws EntityNotFoundException {
    testFind(TXN_START_END);
  }
  public void testFind_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testFind(NEW_PM_START_END);
  }
  private void testFind(StartEnd startEnd) throws EntityNotFoundException {
    testFind(HasOneToManyList.class, BidirMiddle.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Top);
    testFind(HasOneToManyList.class, BidirBottom.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Middle);
    testFind(HasOneToManyList.class, BidirTop.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Bottom);
  }

  public void testQuery() throws EntityNotFoundException {
    testQuery(TXN_START_END);
  }
  public void testQuery_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testQuery(NEW_PM_START_END);
  }
  private void testQuery(StartEnd startEnd) throws EntityNotFoundException {
    testQuery(HasOneToManyList.class, BidirTop.class, startEnd, 
	getEntityKind(BidirTop.class), UnidirLevel.Middle);
    testQuery(HasOneToManyList.class, BidirBottom.class, startEnd, 
	getEntityKind(BidirTop.class), UnidirLevel.Top);
    testQuery(HasOneToManyList.class, BidirMiddle.class, startEnd, 
	getEntityKind(BidirTop.class), UnidirLevel.Bottom);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyList.class, BidirTop.class);
    testChildFetchedLazily(HasOneToManyList.class, BidirMiddle.class);
    testChildFetchedLazily(HasOneToManyList.class, BidirBottom.class);
  }

  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(TXN_START_END);
  }
  public void testDeleteParentDeletesChild_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testDeleteParentDeletesChild(NEW_PM_START_END);
  }
  private void testDeleteParentDeletesChild(StartEnd startEnd) throws Exception {
    testDeleteParentDeletesChild(HasOneToManyList.class, BidirTop.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Top);
    testDeleteParentDeletesChild(HasOneToManyList.class, BidirMiddle.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Middle);
    testDeleteParentDeletesChild(HasOneToManyList.class, BidirBottom.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Bottom);
  }

  public void testIndexOf() throws Exception {
    testIndexOf(TXN_START_END);
  }
  public void testIndexOf_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testIndexOf(NEW_PM_START_END);
  }
  public void testIndexOf(StartEnd startEnd) throws Exception {
    HasOneToManyList pojo = new HasOneToManyList();
    BidirTop bidir1 = new BidirMiddle();
    BidirTop bidir2 = new BidirBottom();
    BidirTop bidir3 = new BidirTop();

    UnidirTop unidir1 = newUnidir(UnidirLevel.Middle);
    UnidirTop unidir2 = newUnidir(UnidirLevel.Top);
    
    pojo.addBidirChild(bidir1);
    pojo.addBidirChild(bidir2);
    pojo.addBidirChild(bidir3);
    pojo.addUnidirChild(unidir1);
    pojo.addUnidirChild(unidir2);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();

    startEnd.start();
    pojo = pm.getObjectById(HasOneToManyList.class, pojo.getId());
    bidir1 = pm.getObjectById(bidir1.getClass(), bidir1.getId());
    bidir2 = pm.getObjectById(bidir2.getClass(), bidir2.getId());
    bidir3 = pm.getObjectById(bidir3.getClass(), bidir3.getId());
    unidir1 = pm.getObjectById(unidir1.getClass(), unidir1.getId());
    unidir2 = pm.getObjectById(unidir2.getClass(), unidir2.getId());
    assertEquals(0, pojo.getBidirChildren().indexOf(bidir1));
    assertEquals(1, pojo.getBidirChildren().indexOf(bidir2));
    assertEquals(0, pojo.getUnidirChildren().indexOf(unidir1));
    assertEquals(1, pojo.getUnidirChildren().indexOf(unidir2));
    startEnd.end();
  }

  public void testRemoveAll() throws EntityNotFoundException {
    testRemoveAll(TXN_START_END);
  }
  public void testRemoveAll_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testRemoveAll(NEW_PM_START_END);
  }
  private void testRemoveAll(StartEnd startEnd) throws EntityNotFoundException {
    testRemoveAll(new HasOneToManyList(), new BidirBottom(),
                  new BidirMiddle(), new BidirTop(), startEnd);
  }

  public void testRemoveAll_LongPkOnParent() throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(TXN_START_END);
  }
  public void testRemoveAll_LongPkOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testRemoveAll_LongPkOnParent(NEW_PM_DETACH_ON_CLOSE_START_END);
  }
  private void testRemoveAll_LongPkOnParent(StartEnd startEnd) throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(new HasOneToManyListLongPk(), new BidirTopLongPk(),
                  new BidirMiddleLongPk(), new BidirBottomLongPk(),
                  startEnd);
  }
  
  public void testRemoveAll_UnencodedStringPkOnParent() throws EntityNotFoundException {
    testRemoveAll_UnencodedStringPkOnParent(TXN_START_END);
  }
  public void testRemoveAll_UnencodedStringPkOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testRemoveAll_UnencodedStringPkOnParent(NEW_PM_START_END);
  }
  private void testRemoveAll_UnencodedStringPkOnParent(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyUnencodedStringPkJDO parent = new HasOneToManyListUnencodedStringPk();
    parent.setId("parent id");
    testRemoveAll_UnencodedStringPkOnParent(parent, new BidirBottomUnencodedStringPk(),
                  new BidirTopUnencodedStringPk(), new BidirMiddleUnencodedStringPk(),
                  startEnd);
  }

  public void testChangeParent() {
    testChangeParent(TXN_START_END);
  }
  public void testChangeParent_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testChangeParent(NEW_PM_START_END);
  }
  private void testChangeParent(StartEnd startEnd) {
    testChangeParent(new HasOneToManyList(), new HasOneToManyList(), startEnd,
	UnidirLevel.Top);
    testChangeParent(new HasOneToManyList(), new HasOneToManyList(), startEnd,
	UnidirLevel.Middle);
    testChangeParent(new HasOneToManyList(), new HasOneToManyList(), startEnd,
	UnidirLevel.Bottom);
  }

  public void testNewParentNewChild_NamedKeyOnChild() throws EntityNotFoundException {
    testNewParentNewChild_NamedKeyOnChild(TXN_START_END);
  }
  public void testNewParentNewChild_NamedKeyOnChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testNewParentNewChild_NamedKeyOnChild(NEW_PM_START_END);
  }
  private void testNewParentNewChild_NamedKeyOnChild(StartEnd startEnd) throws EntityNotFoundException {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyList(), startEnd, UnidirLevel.Middle);
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyList(), startEnd, UnidirLevel.Bottom);
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyList(), startEnd, UnidirLevel.Top);
  }

  public void testInsert_NewParentAndChild_LongPk() throws EntityNotFoundException {
    testInsert_NewParentAndChild_LongPk(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_LongPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testInsert_NewParentAndChild_LongPk(NEW_PM_START_END);
  }
  private void testInsert_NewParentAndChild_LongPk(StartEnd startEnd) throws EntityNotFoundException {
    int count = 1;
    testInsert_NewParentAndChild_LongPk(startEnd, new BidirBottomLongPk(), UnidirLevel.Top, count++);
    testInsert_NewParentAndChild_LongPk(startEnd, new BidirMiddleLongPk(), UnidirLevel.Bottom, count++);
    testInsert_NewParentAndChild_LongPk(startEnd, new BidirTopLongPk(), UnidirLevel.Middle, count++);
  }  
  
  private void testInsert_NewParentAndChild_LongPk(StartEnd startEnd,
      BidirTopLongPk bidirChild, UnidirLevel unidirLevel, int count) throws EntityNotFoundException {
    bidirChild.setChildVal("yam");

    UnidirTop unidir = newUnidir(unidirLevel);
    String expectedStr = unidir.getStr();
    String expectedName = unidir.getName();

    HasOneToManyListLongPk parent = new HasOneToManyListLongPk();
    parent.addBidirChild(bidirChild);
    bidirChild.setParent(parent);
    parent.addUnidirChild(unidir);
    parent.setVal("yar");

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(unidir.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(getEntityKind(BidirTopLongPk.class), bidirChildEntity.getKind());
    assertEquals(bidirChild.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(getEntityKind(UnidirTop.class), unidirChildEntity.getKind());
    assertEquals(unidir.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(unidirLevel.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals(expectedStr, unidirChildEntity.getProperty("str"));
    assertEquals(expectedName, unidirChildEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(unidir.getId()), unidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(parent.getClass(), parent.getId(), unidirChildEntity, unidir.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX_longpk"));
    }

    Entity parentEntity = ds.get(KeyFactory.createKey(getEntityKind(parent.getClass()), parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Lists.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Lists.newArrayList(unidirChildEntity.getKey()), parentEntity.getProperty("unidirChildren"));

    assertEquals(HasOneToManyLongPkListJDO.class.getName(), count, countForClass(HasOneToManyListLongPk.class));
    assertEquals(BidirTopLongPk.class.getName(), count, countForClass(BidirTopLongPk.class));
    assertEquals(UnidirTop.class.getName(), count, countForClass(UnidirTop.class));
  }

  public void testInsert_NewParentAndChild_StringPk() throws EntityNotFoundException {
    testInsert_NewParentAndChild_StringPk(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_StringPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testInsert_NewParentAndChild_StringPk(NEW_PM_START_END);
  }
  
  private void testInsert_NewParentAndChild_StringPk(StartEnd startEnd) throws EntityNotFoundException {
    int count = 1;
    testInsert_NewParentAndChild_StringPk(startEnd, new BidirBottomStringPk(), UnidirLevel.Bottom, "yar", count++);
    testInsert_NewParentAndChild_StringPk(startEnd, new BidirMiddleStringPk(), UnidirLevel.Middle, "yas", count++);
    testInsert_NewParentAndChild_StringPk(startEnd, new BidirTopStringPk(), UnidirLevel.Top, "yat", count++);
  }
  
  private void testInsert_NewParentAndChild_StringPk(StartEnd startEnd,
      BidirTopStringPk bidirChild, UnidirLevel unidirLevel, String id, int count) throws EntityNotFoundException {
    bidirChild.setChildVal("yam");

    UnidirTop unidir = newUnidir(unidirLevel);
    String expectedStr = unidir.getStr();
    String expectedName = unidir.getName();

    HasOneToManyListStringPk parent = new HasOneToManyListStringPk();
    parent.setId(id);
    parent.addBidirChild(bidirChild);
    bidirChild.setParent(parent);
    parent.addUnidirChild(unidir);
    parent.setVal("yar");

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(unidir.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(getEntityKind(BidirTopStringPk.class), bidirChildEntity.getKind());
    assertEquals(bidirChild.getClass().getName(), bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());
    if (isIndexed()) {
      assertEquals(0L, bidirChildEntity.getProperty("bidirChildren_INTEGER_IDX"));
    }

    Entity unidirChildEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirChildEntity);
    assertEquals(getEntityKind(UnidirTop.class), unidirChildEntity.getKind());
    assertEquals(unidir.getPropertyCount() + getIndexPropertyCount(), unidirChildEntity.getProperties().size());
    assertEquals(unidirLevel.discriminator, unidirChildEntity.getProperty("TYPE"));
    assertEquals(expectedStr, unidirChildEntity.getProperty("str"));
    assertEquals(expectedName, unidirChildEntity.getProperty("name"));
    assertEquals(KeyFactory.stringToKey(unidir.getId()), unidirChildEntity.getKey());
    PolymorphicTestUtils.assertKeyParentEquals(parent.getClass(), parent.getId(), unidirChildEntity, unidir.getId());
    if (isIndexed()) {
      assertEquals(0L, unidirChildEntity.getProperty("unidirChildren_INTEGER_IDX"));
    }

    Entity parentEntity = ds.get(KeyFactory.createKey(getEntityKind(parent.getClass()), parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Lists.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Lists.newArrayList(unidirChildEntity.getKey()), parentEntity.getProperty("unidirChildren"));

    assertEquals(HasOneToManyListStringPk.class.getName(), count,  countForClass(HasOneToManyListStringPk.class));
    assertEquals(BidirTopStringPk.class.getName(), count, countForClass(BidirTopStringPk.class));
    assertEquals(UnidirTop.class.getName(), count, countForClass(UnidirTop.class));
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSamePm() {
    int count = 1;
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManyList(), UnidirLevel.Top, count++);
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManyList(), UnidirLevel.Middle, count++);
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManyList(), UnidirLevel.Bottom, count++);
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm() {
    int count = 1;
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManyList(), UnidirLevel.Middle, count++);
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManyList(), UnidirLevel.Bottom, count++);
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManyList(), UnidirLevel.Top, count++);
  }

  
  public void testLongPkOneToManyBidirChildren() {
    testLongPkOneToManyBidirChildren(TXN_START_END);
  }


  public void testLongPkOneToManyBidirChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testLongPkOneToManyBidirChildren(NEW_PM_START_END);
  }
  private void testLongPkOneToManyBidirChildren(StartEnd startEnd) {
    testLongPkOneToManyBidirChildren(startEnd, new BidirTopLongPkChildKey());
    testLongPkOneToManyBidirChildren(startEnd, new BidirMiddleLongPkChildKey());
    testLongPkOneToManyBidirChildren(startEnd, new BidirBottomLongPkChildKey());
  }
  private void testLongPkOneToManyBidirChildren(StartEnd startEnd,
      BidirTopLongPkChildKey child) {
    HasOneToManyListLongPkChildKeyPk pojo = new HasOneToManyListLongPkChildKeyPk();
    pojo.setChildren(Utils.newArrayList(child));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.getObjectById(HasOneToManyListLongPkChildKeyPk.class, pojo.getId());
    assertEquals(1, pojo.getChildren().size());
    assertEquals(pojo, pojo.getChildren().get(0).getParent());
    startEnd.end();
  }
  
  public void testUnencodedStringPkOneToManyBidirChildren() {
    testUnencodedStringPkOneToManyBidirChildren(TXN_START_END);
  }
  public void testUnencodedStringPkOneToManyBidirChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testUnencodedStringPkOneToManyBidirChildren(NEW_PM_START_END);
  }
  private void testUnencodedStringPkOneToManyBidirChildren(StartEnd startEnd) {
    testUnencodedStringPkOneToManyBidirChildren(startEnd, new BidirMiddleUnencodedStringPkChildKey(), "yar");
    testUnencodedStringPkOneToManyBidirChildren(startEnd, new BidirBottomUnencodedStringPkChildKey(), "yas");
    testUnencodedStringPkOneToManyBidirChildren(startEnd, new BidirTopUnencodedStringPkChildKey(), "yat");
  }
  
  private void testUnencodedStringPkOneToManyBidirChildren(StartEnd startEnd,
      BidirTopUnencodedStringPkChildKey child, String id) {
    HasOneToManyListUnencodedStringPkChildKey pojo = new HasOneToManyListUnencodedStringPkChildKey();
    pojo.setId(id);
    pojo.setChildren(Utils.newArrayList(child));
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.getObjectById(HasOneToManyListUnencodedStringPkChildKey.class, pojo.getId());
    assertEquals(1, pojo.getChildren().size());
    assertEquals(pojo, pojo.getChildren().get(0).getParent());
    startEnd.end();
  }

  public void testFetchOfOneToManyParentWithKeyPk() {
    testFetchOfOneToManyParentWithKeyPk(TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithKeyPk_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testFetchOfOneToManyParentWithKeyPk(NEW_PM_START_END);
  }
  private void testFetchOfOneToManyParentWithKeyPk(StartEnd startEnd) {
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyListKeyPk(), startEnd);
  }

  public void testFetchOfOneToManyParentWithLongPk() {
    testFetchOfOneToManyParentWithLongPk(TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithLongPk_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testFetchOfOneToManyParentWithLongPk(NEW_PM_START_END);
  }
  private void testFetchOfOneToManyParentWithLongPk(StartEnd startEnd) {
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyListLongPk(), startEnd);
  }

  public void testFetchOfOneToManyParentWithUnencodedStringPk() {
    testFetchOfOneToManyParentWithUnencodedStringPk(TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithUnencodedStringPk_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testFetchOfOneToManyParentWithUnencodedStringPk(NEW_PM_START_END);
  }
  private void testFetchOfOneToManyParentWithUnencodedStringPk(StartEnd startEnd) {
    testFetchOfOneToManyParentWithUnencodedStringPk(
        new HasOneToManyListUnencodedStringPk(), startEnd);
  }

  public void testAddChildToOneToManyParentWithLongPk() throws EntityNotFoundException {
    testAddChildToOneToManyParentWithLongPk(TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithLongPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testAddChildToOneToManyParentWithLongPk(NEW_PM_START_END);
  }
  private void testAddChildToOneToManyParentWithLongPk(StartEnd startEnd) throws EntityNotFoundException {
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyListLongPk(), new BidirTopLongPk(), startEnd, UnidirLevel.Top);
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyListLongPk(), new BidirMiddleLongPk(), startEnd, UnidirLevel.Middle);
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyListLongPk(), new BidirBottomLongPk(), startEnd, UnidirLevel.Bottom);
  }

  public void testAddChildToOneToManyParentWithUnencodedStringPk() throws EntityNotFoundException {
    testAddChildToOneToManyParentWithUnencodedStringPk(TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithUnencodedStringPk_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testAddChildToOneToManyParentWithUnencodedStringPk(NEW_PM_START_END);
  }
  private void testAddChildToOneToManyParentWithUnencodedStringPk(StartEnd startEnd) throws EntityNotFoundException {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyListUnencodedStringPk(), new BidirMiddleUnencodedStringPk(),
        startEnd, UnidirLevel.Bottom, "yar");
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyListUnencodedStringPk(), new BidirTopUnencodedStringPk(),
        startEnd, UnidirLevel.Top, "yas");
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyListUnencodedStringPk(), new BidirBottomUnencodedStringPk(),
        startEnd, UnidirLevel.Middle, "yat");
  }

  public void testOneToManyChildAtMultipleLevels() {
    testOneToManyChildAtMultipleLevels(TXN_START_END);
  }
  public void testOneToManyChildAtMultipleLevels_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testOneToManyChildAtMultipleLevels(NEW_PM_START_END);
  }
  private void testOneToManyChildAtMultipleLevels(StartEnd startEnd) {
    testOneToManyChildAtMultipleLevels(startEnd, UnidirLevel.Top, UnidirLevel.Bottom, 1);
    testOneToManyChildAtMultipleLevels(startEnd, UnidirLevel.Middle, UnidirLevel.Bottom, 2);
    testOneToManyChildAtMultipleLevels(startEnd, UnidirLevel.Bottom, UnidirLevel.Bottom, 3);
  }
  private void testOneToManyChildAtMultipleLevels(StartEnd startEnd, 
      UnidirLevel unidirLevel1, UnidirLevel unidirLevel2, int count) {
    HasOneToManyChildAtMultipleLevels pojo = new HasOneToManyChildAtMultipleLevels();
    UnidirTop unidir1 = newUnidir(unidirLevel1);
    pojo.setUnidirChildren(Utils.newArrayList(unidir1));
    HasOneToManyChildAtMultipleLevels child = new HasOneToManyChildAtMultipleLevels();
    UnidirTop unidir2 = newUnidir(unidirLevel2);
    child.setUnidirChildren(Utils.newArrayList(unidir2));
    pojo.setChild(child);
    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    assertEquals(2 * count, countForClass(UnidirTop.class));
    pojo = pm.getObjectById(HasOneToManyChildAtMultipleLevels.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertTrue(pojo.getUnidirChildren().get(0).customEquals(unidir1));
    assertTrue(child.getUnidirChildren().get(0).customEquals(unidir2));
    assertEquals(1, child.getUnidirChildren().size());
    startEnd.end();
  }
  public void testAddQueriedParentToBidirChild() throws EntityNotFoundException {
    testAddQueriedParentToBidirChild(TXN_START_END);
  }
  public void testAddQueriedParentToBidirChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testAddQueriedParentToBidirChild(NEW_PM_START_END);
  }
  private void testAddQueriedParentToBidirChild(StartEnd startEnd) throws EntityNotFoundException {
    testAddQueriedParentToBidirChild(new HasOneToManyList(), new BidirMiddle(), startEnd, getEntityKind(BidirTop.class));
    testAddQueriedParentToBidirChild(new HasOneToManyList(), new BidirTop(), startEnd, getEntityKind(BidirTop.class));
    testAddQueriedParentToBidirChild(new HasOneToManyList(), new BidirBottom(), startEnd, getEntityKind(BidirTop.class));
  }

  public void testAddFetchedParentToBidirChild() throws EntityNotFoundException {
    testAddFetchedParentToBidirChild(TXN_START_END);
  }
  public void testAddFetchedParentToBidirChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testAddFetchedParentToBidirChild(NEW_PM_START_END);
  }
  private void testAddFetchedParentToBidirChild(StartEnd startEnd) throws EntityNotFoundException {
    testAddFetchedParentToBidirChild(new HasOneToManyList(), new BidirTop(), startEnd, getEntityKind(BidirTop.class));
    testAddFetchedParentToBidirChild(new HasOneToManyList(), new BidirMiddle(), startEnd, getEntityKind(BidirTop.class));
    testAddFetchedParentToBidirChild(new HasOneToManyList(), new BidirBottom(), startEnd, getEntityKind(BidirTop.class));
  }

  public void testMultipleBidirChildren() {
    testMultipleBidirChildren(TXN_START_END);
  }
  public void testMultipleBidirChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testMultipleBidirChildren(NEW_PM_START_END);
  }
  private void testMultipleBidirChildren(StartEnd startEnd) {
    testMultipleBidirChildren(startEnd, 
	new HasOneToManyMultipleBidirChildren.BidirChildTop1(),
	new HasOneToManyMultipleBidirChildren.BidirChildMiddle2());
    testMultipleBidirChildren(startEnd, 
	new HasOneToManyMultipleBidirChildren.BidirChildBottom1(),
	new HasOneToManyMultipleBidirChildren.BidirChildTop2());
    testMultipleBidirChildren(startEnd, 
	new HasOneToManyMultipleBidirChildren.BidirChildMiddle1(),
	new HasOneToManyMultipleBidirChildren.BidirChildBottom2());
  }
  private void testMultipleBidirChildren(StartEnd startEnd, 
      HasOneToManyMultipleBidirChildren.BidirChildTop1 c1,
      HasOneToManyMultipleBidirChildren.BidirChildTop2 c2) {
    HasOneToManyMultipleBidirChildren pojo = new HasOneToManyMultipleBidirChildren();

    pojo.getChild1().add(c1);
    pojo.getChild2().add(c2);

    startEnd.start();
    pm.makePersistent(pojo);
    startEnd.end();
    startEnd.start();
    pojo = pm.getObjectById(HasOneToManyMultipleBidirChildren.class, pojo.getId());
    assertEquals(1, pojo.getChild1().size());
    assertEquals(c1.getId(), pojo.getChild1().get(0).getId());
    assertEquals(c1.getClass(),  pojo.getChild1().get(0).getClass());
    assertEquals(1, pojo.getChild2().size());
    assertEquals(c2.getId(), pojo.getChild2().get(0).getId());
    assertEquals(c2.getClass(),  pojo.getChild2().get(0).getClass());
    startEnd.end();
  }

  public void testReplaceBidirColl() {
    testReplaceBidirColl(TXN_START_END);
  }
  public void testReplaceBidirColl_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testReplaceBidirColl(NEW_PM_START_END);
  }
  private void testReplaceBidirColl(StartEnd startEnd) {
    Collection<BidirTop> childList = Utils.<BidirTop>newArrayList(
        new BidirMiddle(), new BidirBottom());
    testReplaceBidirColl(
        new HasOneToManyList(), new BidirTop(), childList, startEnd);
  }

  public void testDeleteChildWithSeparateNameField() {
    testDeleteChildWithSeparateNameField(TXN_START_END);
  }
  public void testDeleteChildWithSeparateNameField_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testDeleteChildWithSeparateNameField(NEW_PM_START_END);
  }
  private void testDeleteChildWithSeparateNameField(StartEnd startEnd) {
    testDeleteChildWithSeparateNameField(startEnd, new UnidirMiddleEncodedStringPkSeparateNameField());
    testDeleteChildWithSeparateNameField(startEnd, new UnidirTopEncodedStringPkSeparateNameField());
    testDeleteChildWithSeparateNameField(startEnd, new UnidirBottomEncodedStringPkSeparateNameField());
  }

  private void testDeleteChildWithSeparateNameField(StartEnd startEnd,
      UnidirTopEncodedStringPkSeparateNameField child) {
    HasOneToManyWithSeparateNameFieldJDO parent = new HasOneToManyWithSeparateNameFieldJDO();
    child.setName("the name");
    parent.getChildren().add(child);
    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();
    startEnd.start();
    parent = pm.getObjectById(HasOneToManyWithSeparateNameFieldJDO.class, parent.getId());
    pm.deletePersistent(parent);
    startEnd.end();
  }

  public void testOnlyOneParentPutOnParentAndChildUpdate() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(TXN_START_END);
  }
  public void testOnlyOneParentPutOnParentAndChildUpdate_NoTxn() throws Throwable {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testOnlyOneParentPutOnParentAndChildUpdate(NEW_PM_START_END);
  }
  private void testOnlyOneParentPutOnParentAndChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(
        new HasOneToManyList(), new BidirTop(), startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(
        new HasOneToManyList(), new BidirMiddle(), startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(
        new HasOneToManyList(), new BidirBottom(), startEnd);
  }

  public void testOnlyOnePutOnChildUpdate() throws Throwable {
    testOnlyOnePutOnChildUpdate(TXN_START_END);
  }
  public void testOnlyOnePutOnChildUpdate_NoTxn() throws Throwable {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testOnlyOnePutOnChildUpdate(NEW_PM_START_END);
  }
  private void testOnlyOnePutOnChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOnePutOnChildUpdate(
        new HasOneToManyList(), new BidirBottom(), startEnd);
    testOnlyOnePutOnChildUpdate(
        new HasOneToManyList(), new BidirMiddle(), startEnd);
    testOnlyOnePutOnChildUpdate(
        new HasOneToManyList(), new BidirTop(), startEnd);
  }

  public void testOnlyOneParentPutOnChildDelete() throws Throwable {
    testOnlyOneParentPutOnChildDelete(TXN_START_END);
  }
  public void testOnlyOneParentPutOnChildDelete_NoTxn() throws Throwable {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testOnlyOneParentPutOnChildDelete(NEW_PM_START_END);
  }
  private void testOnlyOneParentPutOnChildDelete(StartEnd startEnd) throws Throwable {
    testOnlyOneParentPutOnChildDelete(
        new HasOneToManyList(), new BidirTop(), startEnd);
    testOnlyOneParentPutOnChildDelete(
        new HasOneToManyList(), new BidirBottom(), startEnd);
    testOnlyOneParentPutOnChildDelete(
        new HasOneToManyList(), new BidirMiddle(), startEnd);
  }

  public void testNonTxnAddOfChildToParentFailsPartwayThrough() throws Throwable {
    testNonTxnAddOfChildToParentFailsPartwayThrough(new HasOneToManyList());
  }

  public void testUnsupportedInheritanceMappings() {
    testUnsupportedInheritanceMappings(TXN_START_END);
  }
  public void testUnsupportedInheritanceMappings_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testUnsupportedInheritanceMappings(NEW_PM_START_END);
  }
  private void testUnsupportedInheritanceMappings(StartEnd startEnd) {
    HasOneToManyWithUnsupportedInheritanceList parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren1().add(new CompleteTableParentWithCompleteTableChild.Child());
    makePersistentWithExpectedException(startEnd, parent);

    parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren2().add(new CompleteTableParentWithNewTableChild.Child());
    makePersistentWithExpectedException(startEnd, parent);

    parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren3().add(new CompleteTableParentWithSubclassTableChild.Child());
    makePersistentWithExpectedException(startEnd, parent);

    parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren4().add(new CompleteTableParentNoChildStrategy.Child());
    makePersistentWithExpectedException(startEnd, parent);

    parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren5().add(new NewTableParentWithCompleteTableChild.Child());
    makePersistentWithExpectedException(startEnd, parent);

    parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren6().add(new NewTableParentWithSubclassTableChild.Child());
    makePersistentWithExpectedException(startEnd, parent);

    parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren7().add(new NewTableParentWithNewTableChild.Child());
    makePersistentWithExpectedException(startEnd, parent);
  }
  private void makePersistentWithExpectedException(StartEnd startEnd,
      HasOneToManyWithUnsupportedInheritanceList parent) {
    startEnd.start();
    try {
      pm.makePersistent(parent);
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException uso) {
      Assert.assertTrue(uso.getMessage().contains("superclass-table"));
    }
    startEnd.end();
  }
  
  public void xtestRemove2ObjectsAtIndex() {
    testRemove2ObjectsAtIndex(TXN_START_END);
  }
  public void xtestRemove2ObjectsAtIndex_NoTxn() {
    testRemove2ObjectsAtIndex(NEW_PM_START_END);
  }

  private void testRemove2ObjectsAtIndex(StartEnd startEnd) {
    BidirTop bidirChild1 = new BidirTop();
    BidirTop bidirChild2 = new BidirTop();
    UnidirTop unidir1 = new UnidirTop();
    UnidirTop unidir2 = new UnidirTop();

    HasOneToManyList parent = new HasOneToManyList();
    parent.addBidirChild(bidirChild1);
    bidirChild1.setParent(parent);
    parent.addBidirChild(bidirChild2);
    bidirChild2.setParent(parent);
    parent.addUnidirChild(unidir1);
    parent.addUnidirChild(unidir2);

    startEnd.start();
    pm.makePersistent(parent);
    startEnd.end();

    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    parent.getUnidirChildren().remove(0);
    parent.getUnidirChildren().remove(0);
    startEnd.end();

    startEnd.start();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertTrue(parent.getUnidirChildren().isEmpty());
    startEnd.end();
  }
  
  @Override
  boolean isIndexed() {
    return true;
  }  
  
  @Override
  protected void registerSubclasses() {
    // Make sure all subclasses of UnidirTop, ... are known. Only the meta data
    // of the top class in the inheritance tree (element type of the collections)
    // will be known when getting the pojo. This would work if UnidirTop and
    // BidirTopLongPk would use the inheritance mapping strategy CLASS_NAME, 
    // but it uses VALUE_MAP. This problem exists with RDBMS datanucleus plugin as well.
    getExecutionContext().getStoreManager().addClass(UnidirMiddle.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(UnidirBottom.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(BidirMiddleLongPk.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(BidirBottomLongPk.class.getName(),
	getExecutionContext().getClassLoaderResolver());
  }
}
