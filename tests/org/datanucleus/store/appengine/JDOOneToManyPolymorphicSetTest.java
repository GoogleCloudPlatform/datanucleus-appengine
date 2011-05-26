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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.EntityNotFoundException;

import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirBottom;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirBottomLongPk;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirBottomUnencodedStringPk;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirMiddle;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirMiddleLongPk;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirMiddleUnencodedStringPk;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirTop;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirTopLongPk;
import org.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirTopUnencodedStringPkJDO;
import org.datanucleus.test.HasPolymorphicRelationsSetJDO.HasOneToManyKeyPkSet;
import org.datanucleus.test.HasPolymorphicRelationsSetJDO.HasOneToManyLongPkSet;
import org.datanucleus.test.HasPolymorphicRelationsSetJDO.HasOneToManySet;
import org.datanucleus.test.HasPolymorphicRelationsSetJDO.HasOneToManyUnencodedStringPkSet;
import org.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirBottom;
import org.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirMiddle;
import org.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTop;

import java.util.Collection;

import static org.datanucleus.store.appengine.PolymorphicTestUtils.getEntityKind;

public class JDOOneToManyPolymorphicSetTest extends JDOOneToManyPolymorphicTestCase {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    testInsertNewParentAndChild(TXN_START_END);
  }
  public void testInsertNewParentAndChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsertNewParentAndChild(NEW_PM_START_END);
  }
  private void testInsertNewParentAndChild(StartEnd startEnd) throws EntityNotFoundException {
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    
    testInsert_NewParentAndChild(new HasOneToManySet(), new BidirTop(), startEnd, UnidirLevel.Bottom,
	expectedBidirKind, expectedUnidirKind, 1, 1);
    testInsert_NewParentAndChild(new HasOneToManySet(), new BidirMiddle(), startEnd, UnidirLevel.Top,
	expectedBidirKind, expectedUnidirKind, 2, 2);
    testInsert_NewParentAndChild(new HasOneToManySet(), new BidirBottom(), startEnd, UnidirLevel.Middle,
	expectedBidirKind, expectedUnidirKind, 3, 3);
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
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);

    testInsert_ExistingParentNewChild(new HasOneToManySet(), new BidirBottom(), startEnd, UnidirLevel.Middle,
	expectedBidirKind, expectedUnidirKind, 1, 1);
    testInsert_ExistingParentNewChild(new HasOneToManySet(), new BidirTop(), startEnd, UnidirLevel.Bottom,
	expectedBidirKind, expectedUnidirKind, 2, 2);
    testInsert_ExistingParentNewChild(new HasOneToManySet(), new BidirMiddle(), startEnd, UnidirLevel.Top,
	expectedBidirKind, expectedUnidirKind, 3, 3);
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
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);

    testUpdate_UpdateChildWithMerge(new HasOneToManySet(), new BidirTop(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Bottom, 1);
    testUpdate_UpdateChildWithMerge(new HasOneToManySet(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Top, 2);
    testUpdate_UpdateChildWithMerge(new HasOneToManySet(), new BidirBottom(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Middle, 3);
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
    String expectedBidirKind = getEntityKind(BidirTop.class);
    String expectedUnidirKind  = getEntityKind(UnidirTop.class);
    testUpdate_UpdateChild(new HasOneToManySet(), new BidirTop(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Middle, 1);
    testUpdate_UpdateChild(new HasOneToManySet(), new BidirMiddle(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Top, 2);
    testUpdate_UpdateChild(new HasOneToManySet(), new BidirBottom(), startEnd,
	expectedBidirKind, expectedUnidirKind, UnidirLevel.Bottom, 3);
  }


  public void testUpdateNullOutChildren() throws EntityNotFoundException {
    testUpdateNullOutChildren(TXN_START_END);
  }
  public void testUpdateNullOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdateNullOutChildren(NEW_PM_DETACH_ON_CLOSE_START_END);
  }
  private void testUpdateNullOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    testUpdate_NullOutChildren(new HasOneToManySet(), new BidirBottom(), startEnd,
	UnidirLevel.Bottom, 1);
    testUpdate_NullOutChildren(new HasOneToManySet(), new BidirMiddle(), startEnd,
	UnidirLevel.Middle, 2);
    testUpdate_NullOutChildren(new HasOneToManySet(), new BidirTop(), startEnd,
	UnidirLevel.Top, 3);
  }

  public void testUpdateClearOutChildren() throws EntityNotFoundException {
    testUpdateClearOutChildren(TXN_START_END);
  }
  public void testUpdateClearOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testUpdateClearOutChildren(NEW_PM_DETACH_ON_CLOSE_START_END);
  }
  private void testUpdateClearOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    testUpdate_ClearOutChildren(new HasOneToManySet(), new BidirTop(), startEnd,
	UnidirLevel.Bottom, 1);
    testUpdate_ClearOutChildren(new HasOneToManySet(), new BidirBottom(), startEnd,
	UnidirLevel.Middle, 2);
    testUpdate_ClearOutChildren(new HasOneToManySet(), new BidirMiddle(), startEnd,
	UnidirLevel.Top, 3);
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
    testFind(HasOneToManySet.class, BidirMiddle.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Top);
    testFind(HasOneToManySet.class, BidirBottom.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Middle);
    testFind(HasOneToManySet.class, BidirTop.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Bottom);
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
    testQuery(HasOneToManySet.class, BidirTop.class, startEnd, 
	getEntityKind(BidirTop.class), UnidirLevel.Middle);
    testQuery(HasOneToManySet.class, BidirBottom.class, startEnd, 
	getEntityKind(BidirTop.class), UnidirLevel.Top);
    testQuery(HasOneToManySet.class, BidirMiddle.class, startEnd, 
	getEntityKind(BidirTop.class), UnidirLevel.Bottom);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManySet.class, BidirTop.class);
    testChildFetchedLazily(HasOneToManySet.class, BidirMiddle.class);
    testChildFetchedLazily(HasOneToManySet.class, BidirBottom.class);
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
    testDeleteParentDeletesChild(HasOneToManySet.class, BidirTop.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Top);
    testDeleteParentDeletesChild(HasOneToManySet.class, BidirMiddle.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Middle);
    testDeleteParentDeletesChild(HasOneToManySet.class, BidirBottom.class, startEnd,
	getEntityKind(BidirTop.class), UnidirLevel.Bottom);
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
    testRemoveAll(new HasOneToManySet(), new BidirTop(),
                  new BidirMiddle(), new BidirBottom(), startEnd);
  }

  public void testRemoveAll_LongPkOnParent() throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(TXN_START_END);
  }
  public void testRemoveAll_LongPkOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testRemoveAll_LongPkOnParent(NEW_PM_DETACH_ON_CLOSE_START_END);
  }
  private void testRemoveAll_LongPkOnParent(StartEnd startEnd) throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(new HasOneToManyLongPkSet(), new BidirTopLongPk(),
                  new BidirMiddleLongPk(), new BidirBottomLongPk(), startEnd);
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
    HasOneToManyUnencodedStringPkSet parent = new HasOneToManyUnencodedStringPkSet();
    parent.setId("parent id");
    testRemoveAll_UnencodedStringPkOnParent(parent, new BidirTopUnencodedStringPkJDO(),
                  new BidirBottomUnencodedStringPk(),
                  new BidirMiddleUnencodedStringPk(), startEnd);
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
    testChangeParent(new HasOneToManySet(), new HasOneToManySet(), startEnd,
	UnidirLevel.Top);
    testChangeParent(new HasOneToManySet(), new HasOneToManySet(), startEnd,
	UnidirLevel.Middle);
    testChangeParent(new HasOneToManySet(), new HasOneToManySet(), startEnd,
	UnidirLevel.Bottom);
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
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManySet(), startEnd, UnidirLevel.Middle);
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManySet(), startEnd, UnidirLevel.Top);
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManySet(), startEnd, UnidirLevel.Bottom);
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSamePm() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManySet(), UnidirLevel.Middle, 1);
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManySet(), UnidirLevel.Top, 2);
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManySet(), UnidirLevel.Bottom, 3);

  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm() {
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManySet(), UnidirLevel.Middle, 1);
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManySet(), UnidirLevel.Bottom, 2);
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManySet(), UnidirLevel.Top, 3);
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
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkSet(), startEnd);
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
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkSet(), startEnd);
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
    testFetchOfOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSet(), startEnd);
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
        new HasOneToManyLongPkSet(), new BidirTopLongPk(), startEnd, UnidirLevel.Top);
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyLongPkSet(), new BidirMiddleLongPk(), startEnd, UnidirLevel.Middle);
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyLongPkSet(), new BidirBottomLongPk(), startEnd, UnidirLevel.Bottom);
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
        new HasOneToManyUnencodedStringPkSet(), new BidirMiddleUnencodedStringPk(),
        startEnd, UnidirLevel.Middle, "yar");
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkSet(), new BidirTopUnencodedStringPkJDO(),
        startEnd, UnidirLevel.Bottom, "yas");
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkSet(), new BidirBottomUnencodedStringPk(),
        startEnd, UnidirLevel.Top, "yat");
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
    testAddQueriedParentToBidirChild(new HasOneToManySet(), new BidirMiddle(), startEnd, 
	getEntityKind(BidirTop.class));
    testAddQueriedParentToBidirChild(new HasOneToManySet(), new BidirTop(), startEnd, 
	getEntityKind(BidirTop.class));
    testAddQueriedParentToBidirChild(new HasOneToManySet(), new BidirBottom(), startEnd, 
	getEntityKind(BidirTop.class));
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
    testAddFetchedParentToBidirChild(new HasOneToManySet(), new BidirTop(), startEnd, 
	getEntityKind(BidirTop.class));
    testAddFetchedParentToBidirChild(new HasOneToManySet(), new BidirMiddle(), startEnd, 
	getEntityKind(BidirTop.class));
    testAddFetchedParentToBidirChild(new HasOneToManySet(), new BidirBottom(), startEnd, 
	getEntityKind(BidirTop.class));
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
    Collection<BidirTop> childList = Utils.<BidirTop>newHashSet(
        new BidirMiddle(), new BidirBottom());
    testReplaceBidirColl(new HasOneToManySet(), new BidirTop(), 
	childList, startEnd);
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
        new HasOneToManySet(), new BidirMiddle(), startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(
        new HasOneToManySet(), new BidirTop(), startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(
        new HasOneToManySet(), new BidirBottom(), startEnd);
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
    testOnlyOnePutOnChildUpdate(new HasOneToManySet(), new BidirBottom(), startEnd);
    testOnlyOnePutOnChildUpdate(new HasOneToManySet(), new BidirMiddle(), startEnd);
    testOnlyOnePutOnChildUpdate(new HasOneToManySet(), new BidirTop(), startEnd);
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
    testOnlyOneParentPutOnChildDelete(new HasOneToManySet(), new BidirTop(), startEnd);
    testOnlyOneParentPutOnChildDelete(new HasOneToManySet(), new BidirBottom(), startEnd);
    testOnlyOneParentPutOnChildDelete(new HasOneToManySet(), new BidirMiddle(), startEnd);
  }

  public void testNonTxnAddOfChildToParentFailsPartwayThrough() throws Throwable {
    testNonTxnAddOfChildToParentFailsPartwayThrough(new HasOneToManySet());
  }
  
  @Override
  boolean isIndexed() {
    return false;
  }
  @Override
  protected void registerSubclasses() {
    // Make sure all subclasses of UnidirTop, ... are known. Only the meta data
    // of the top class in the inheritance tree (element type of the collections)
    // is known otherwise when getting the pojo. This would work if UnidirTop and
    // BidirTopLongPk would use the inheritance mapping strategy CLASS_NAME, 
    // but it uses VALUE_MAP. This problem exists with RDBMS datanucleus plugin as well.
    getObjectManager().getStoreManager().addClass(UnidirMiddle.class.getName(),
	getObjectManager().getClassLoaderResolver());
    getObjectManager().getStoreManager().addClass(UnidirBottom.class.getName(),
	getObjectManager().getClassLoaderResolver());
    getObjectManager().getStoreManager().addClass(BidirMiddleLongPk.class.getName(),
	getObjectManager().getClassLoaderResolver());
    getObjectManager().getStoreManager().addClass(BidirBottomLongPk.class.getName(),
	getObjectManager().getClassLoaderResolver());
  }
}
