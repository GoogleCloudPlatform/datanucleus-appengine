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

import com.google.appengine.api.datastore.EntityNotFoundException;

import org.datanucleus.test.BidirectionalChildJDO;
import org.datanucleus.test.BidirectionalChildLongPkSetJDO;
import org.datanucleus.test.BidirectionalChildSetJDO;
import org.datanucleus.test.BidirectionalChildSetUnencodedStringPkJDO;
import org.datanucleus.test.HasOneToManyKeyPkSetJDO;
import org.datanucleus.test.HasOneToManyLongPkSetJDO;
import org.datanucleus.test.HasOneToManySetJDO;
import org.datanucleus.test.HasOneToManyUnencodedStringPkSetJDO;

import java.util.Collection;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManySetTest extends JDOOneToManyTestCase {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    HasOneToManySetJDO parent = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidirChild = new BidirectionalChildSetJDO();
    testInsert_NewParentAndChild(parent, bidirChild);
  }

  public void testInsertExistingParentNewChild() throws EntityNotFoundException {
    HasOneToManySetJDO parent = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidirChild = new BidirectionalChildSetJDO();
    testInsert_ExistingParentNewChild(parent, bidirChild);
  }

  public void testUpdateUpdateChildWithMerge() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_UpdateChildWithMerge(pojo, bidir);
  }

  public void testUpdateUpdateChild() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_UpdateChild(pojo, bidir);
  }

  public void testUpdateNullOutChildren() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_NullOutChildren(pojo, bidir);
  }

  public void testUpdateClearOutChildren() throws EntityNotFoundException {
    HasOneToManySetJDO pojo = new HasOneToManySetJDO();
    BidirectionalChildSetJDO bidir = new BidirectionalChildSetJDO();
    testUpdate_ClearOutChildren(pojo, bidir);
  }

  public void testFind() throws EntityNotFoundException {
    testFind(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  public void testQuery() throws EntityNotFoundException {
    testQuery(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(HasOneToManySetJDO.class, BidirectionalChildSetJDO.class);
  }

  public void testRemoveAll() throws EntityNotFoundException {
    testRemoveAll(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(),
                  new BidirectionalChildSetJDO(), new BidirectionalChildSetJDO());
  }

  public void testChangeParent() {
    testChangeParent(new HasOneToManySetJDO(), new HasOneToManySetJDO());
  }

  public void testNewParentNewChild_NamedKeyOnChild() throws EntityNotFoundException {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManySetJDO());
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSamePm() {
    testAddAlreadyPersistedChildToParent_NoTxnSamePm(new HasOneToManySetJDO());
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentPm() {
    testAddAlreadyPersistedChildToParent_NoTxnDifferentPm(new HasOneToManySetJDO());
  }

  public void testFetchOfOneToManyParentWithKeyPk() {
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkSetJDO());
  }

  public void testFetchOfOneToManyParentWithLongPk() {
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkSetJDO());
  }

  public void testFetchOfOneToManyParentWithUnencodedStringPk() {
    testFetchOfOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSetJDO());
  }

  public void testAddChildToOneToManyParentWithLongPk() throws EntityNotFoundException {
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkSetJDO(), new BidirectionalChildLongPkSetJDO());
  }

  public void testAddChildToOneToManyParentWithUnencodedStringPk() throws EntityNotFoundException {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkSetJDO(), new BidirectionalChildSetUnencodedStringPkJDO());
  }

  public void testAddQueriedParentToBidirChild() throws EntityNotFoundException {
    testAddQueriedParentToBidirChild(new HasOneToManySetJDO(), new BidirectionalChildSetJDO());
  }

  public void testReplaceBidirColl() {
    Collection<BidirectionalChildJDO> childSet = Utils.<BidirectionalChildJDO>newHashSet(
        new BidirectionalChildSetJDO(), new BidirectionalChildSetJDO());
    testReplaceBidirColl(new HasOneToManySetJDO(), new BidirectionalChildSetJDO(), childSet);
  }

  @Override
  boolean isIndexed() {
    return false;
  }
}