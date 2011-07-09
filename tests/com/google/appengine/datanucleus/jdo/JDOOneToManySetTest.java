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

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.BidirectionalChildJDO;
import com.google.appengine.datanucleus.test.BidirectionalChildLongPkSetJDO;
import com.google.appengine.datanucleus.test.BidirectionalChildSetJDO;
import com.google.appengine.datanucleus.test.BidirectionalChildUnencodedStringPkSetJDO;
import com.google.appengine.datanucleus.test.HasOneToManyKeyPkSetJDO;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkSetJDO;
import com.google.appengine.datanucleus.test.HasOneToManySetJDO;
import com.google.appengine.datanucleus.test.HasOneToManyUnencodedStringPkSetJDO;


import java.util.Collection;

import org.datanucleus.store.ExecutionContext;
import org.datanucleus.util.NucleusLogger;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManySetTest extends JDOOneToManyTestCase {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    testInsertNewParentAndChild(TXN_START_END);
  }
  public void testInsertNewParentAndChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testInsertNewParentAndChild(NEW_PM_START_END);
  }
  private void testInsertNewParentAndChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManySetJDO parent = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidirChild = new BidirectionalChildSetJDO();
    testInsert_NewParentAndChild(parent, bidirChild, startEnd);
  }

  public void testInsertExistingParentNewChild() throws EntityNotFoundException {
    testInsertExistingParentNewChild(TXN_START_END);
  }
  public void testInsertExistingParentNewChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    NucleusLogger.GENERAL.info(">> testInsertExistingParentNewChild_NoTxn START");
    testInsertExistingParentNewChild(NEW_PM_START_END);
    NucleusLogger.GENERAL.info(">> testInsertExistingParentNewChild_NoTxn END");
  }
  private void testInsertExistingParentNewChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManySetJDO parent = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidirChild = new BidirectionalChildSetJDO();
    testInsert_ExistingParentNewChild(parent, bidirChild, startEnd);
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
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_UpdateChildWithMerge(pojo, bidir, startEnd);
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
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_UpdateChild(pojo, bidir, startEnd);
  }

  public void testUpdateNullOutChildren() throws EntityNotFoundException {
    testUpdateNullOutChildren(TXN_START_END);
  }
  public void testUpdateNullOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testUpdateNullOutChildren(NEW_PM_START_END);
  }
  private void testUpdateNullOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_NullOutChildren(pojo, bidir, startEnd);
  }

  public void testUpdateClearOutChildren() throws EntityNotFoundException {
    testUpdateClearOutChildren(TXN_START_END);
  }
  public void testUpdateClearOutChildren_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testUpdateClearOutChildren(NEW_PM_START_END);
  }
  private void testUpdateClearOutChildren(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_ClearOutChildren(pojo, bidir, startEnd);
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
    testFind(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class, startEnd);
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
    testQuery(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class, startEnd);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
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
    testDeleteParentDeletesChild(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class, startEnd);
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
    testRemoveAll(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(),
                  new BidirectionalChildSetJDO(), new BidirectionalChildSetJDO(), startEnd);
  }

  public void testRemoveAll_LongPkOnParent() throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(TXN_START_END);
  }
  public void testRemoveAll_LongPkOnParent_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testRemoveAll_LongPkOnParent(NEW_PM_START_END);
  }
  private void testRemoveAll_LongPkOnParent(StartEnd startEnd) throws EntityNotFoundException {
    testRemoveAll_LongPkOnParent(new HasOneToManyLongPkSetJDO(), new BidirectionalChildLongPkSetJDO(),
                  new BidirectionalChildLongPkSetJDO(), new BidirectionalChildLongPkSetJDO(), startEnd);
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
    HasOneToManyUnencodedStringPkSetJDO parent = new HasOneToManyUnencodedStringPkSetJDO();
    parent.setId("parent id");
    testRemoveAll_UnencodedStringPkOnParent(parent, new BidirectionalChildUnencodedStringPkSetJDO(),
                  new BidirectionalChildUnencodedStringPkSetJDO(),
                  new BidirectionalChildUnencodedStringPkSetJDO(), startEnd);
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
    testChangeParent(new HasOneToManySetJDO(), new HasOneToManySetJDO(), startEnd);
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
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManySetJDO(), startEnd);
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSamePm() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(ExecutionContext.PROP_DETACH_ON_CLOSE, true);
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManySetJDO());
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm() {
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManySetJDO());
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
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkSetJDO(), startEnd);
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
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkSetJDO(), startEnd);
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
    testFetchOfOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSetJDO(), startEnd);
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
        new HasOneToManyLongPkSetJDO(), new BidirectionalChildLongPkSetJDO(), startEnd);
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
        new HasOneToManyUnencodedStringPkSetJDO(), new BidirectionalChildUnencodedStringPkSetJDO(),
        startEnd);
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
    testAddQueriedParentToBidirChild(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(),
                                     startEnd);
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
    testAddFetchedParentToBidirChild(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(), startEnd);
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
    Collection<BidirectionalChildJDO> childSet = Utils.<BidirectionalChildJDO>newHashSet(
        new BidirectionalChildSetJDO(), new BidirectionalChildSetJDO());
    testReplaceBidirColl(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(), childSet, startEnd);
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
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(), startEnd);
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
    testOnlyOnePutOnChildUpdate(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(), startEnd);
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
    testOnlyOneParentPutOnChildDelete(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(), startEnd);
  }

  public void testNonTxnAddOfChildToParentFailsPartwayThrough() throws Throwable {
    testNonTxnAddOfChildToParentFailsPartwayThrough(new HasOneToManySetJDO());
  }
  
  @Override
  boolean isIndexed() {
    return false;
  }
}