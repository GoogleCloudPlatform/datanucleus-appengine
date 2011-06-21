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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.datanucleus.test.BidirectionalChildArrayJDO;
import com.google.appengine.datanucleus.test.HasOneToManyArrayJDO;
import com.google.appengine.datanucleus.test.HasOneToManyArrayWithOrderByJDO;


/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManyArrayTest extends JDOOneToManyTestCase {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    testInsertNewParentAndChild(TXN_START_END);
  }
  public void testInsertNewParentAndChild_NoTxn() throws EntityNotFoundException {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getObjectManager().setDetachOnClose(true);
    testInsertNewParentAndChild(NEW_PM_START_END);
  }
  private void testInsertNewParentAndChild(StartEnd startEnd) throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    testInsert_NewParentAndChild(parent, bidirChild, startEnd);
  }

  // TODO(maxr) This doesn't work for rdbms either.  Find out why.
  public void xxxtestInsertExistingParentNewChild() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    testInsert_ExistingParentNewChild(parent, bidirChild, TXN_START_END);
  }

  // TODO(maxr): Doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestSwapAtPosition_Array() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild2 = new BidirectionalChildArrayJDO();
    testSwapAtPosition(parent, bidirChild, bidirChild2, TXN_START_END);
  }

  // TODO(maxr): Doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestRemoveAtPosition_Array() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild2 = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild3 = new BidirectionalChildArrayJDO();
    testRemoveAtPosition(parent, bidirChild, bidirChild2, bidirChild3, TXN_START_END);
  }

  // TODO(maxr): Doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestAddAtPosition_Array() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild2 = new BidirectionalChildArrayJDO();
    testAddAtPosition(parent, bidirChild, bidirChild2, TXN_START_END);
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
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
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
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
    testUpdate_UpdateChild(pojo, bidir, startEnd);
  }

  // TODO(maxr) This doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestUpdateNullOutChildren() throws EntityNotFoundException {
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
    testUpdate_NullOutChildren(pojo, bidir, TXN_START_END);
  }

  // DataNucleus doesn't detect changes to array elements
  // so this won't work.
  public void xxxtestUpdateClearOutChildren() throws EntityNotFoundException {
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
    testUpdate_ClearOutChildren(pojo, bidir, TXN_START_END);
  }

  // TODO(maxr) Write tests for the array case
  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
  }

  // TODO(maxr) Write tests for the array case
  public void testUpdate_ClearOutChild_NoDelete() throws EntityNotFoundException {
  }

  // Not currently supported by DataNucleus (OrderMetaData only looks
  // for Collection, not array)
  public void xxxtestFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(HasOneToManyArrayWithOrderByJDO.class, TXN_START_END);
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
    testFind(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class, startEnd);
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
    testQuery(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class, startEnd);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class);
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
    testDeleteParentDeletesChild(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class, startEnd);
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
    testChangeParent(new HasOneToManyArrayJDO(), new HasOneToManyArrayJDO(), startEnd);
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
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyArrayJDO(), startEnd);
  }

  @Override
  boolean isIndexed() {
    return true;
  }
}