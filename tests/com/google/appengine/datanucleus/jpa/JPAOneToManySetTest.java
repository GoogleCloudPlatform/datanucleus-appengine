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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.datanucleus.test.BidirectionalChildLongPkSetJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildSetJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildUnencodedStringPkSetJPA;
import com.google.appengine.datanucleus.test.HasOneToManyKeyPkSetJPA;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkSetJPA;
import com.google.appengine.datanucleus.test.HasOneToManySetJPA;
import com.google.appengine.datanucleus.test.HasOneToManyUnencodedStringPkSetJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAOneToManySetTest extends JPAOneToManyTestCase {

  public void testInsert_NewParentAndChild() throws Exception {
    testInsert_NewParentAndChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                 TXN_START_END);
  }

  public void testInsert_NewParentAndChild_NoTxn() throws Exception {
    testInsert_NewParentAndChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                 NEW_EM_START_END);
  }

  public void testInsert_ExistingParentNewChild() throws Exception {
    testInsert_ExistingParentNewChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                      TXN_START_END);
  }

  public void testInsert_ExistingParentNewChild_NoTxn() throws Exception {
    testInsert_ExistingParentNewChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                      NEW_EM_START_END);
  }

  public void testUpdate_UpdateChildWithMerge() throws Exception {
    testUpdate_UpdateChildWithMerge(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                    TXN_START_END);
  }
  public void testUpdate_UpdateChildWithMerge_NoTxn() throws Exception {
    testUpdate_UpdateChildWithMerge(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                    NEW_EM_START_END);
  }

  public void testUpdate_UpdateChild() throws Exception {
    testUpdate_UpdateChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                           TXN_START_END);
  }
  public void testUpdate_UpdateChild_NoTxn() throws Exception {
    testUpdate_UpdateChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                           NEW_EM_START_END);
  }

  public void testUpdate_NullOutChildren() throws Exception {
    testUpdate_NullOutChildren(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                               TXN_START_END);
  }
  public void testUpdate_NullOutChildren_NoTxn() throws Exception {
    testUpdate_NullOutChildren(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                               NEW_EM_START_END);
  }
  public void testUpdate_ClearOutChildren() throws Exception {
    testUpdate_ClearOutChildren(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                TXN_START_END);
  }
  public void testUpdate_ClearOutChildren_NoTxn() throws Exception {
    testUpdate_ClearOutChildren(new BidirectionalChildSetJPA(), new HasOneToManySetJPA(),
                                NEW_EM_START_END);
  }
  public void testFind() throws Exception {
    testFind(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class, TXN_START_END);
  }
  public void testFind_NoTxn() throws Exception {
    testFind(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class, NEW_EM_START_END);
  }
  public void testQuery() throws Exception {
    testQuery(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class, TXN_START_END);
  }
  public void testQuery_NoTxn() throws Exception {
    testQuery(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class, NEW_EM_START_END);
  }
  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class);
  }
  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class,
                                 TXN_START_END);
  }
  public void testDeleteParentDeletesChild_NoTxn() throws Exception {
    testDeleteParentDeletesChild(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class,
                                 NEW_EM_START_END);
  }
  public void testRemoveObject() throws Exception {
    testRemoveObject(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(nextNamedKey()),
        new BidirectionalChildSetJPA(nextNamedKey()), TXN_START_END);
  }
  public void testRemoveObject_NoTxn() throws Exception {
    testRemoveObject(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(nextNamedKey()),
        new BidirectionalChildSetJPA(nextNamedKey()), NEW_EM_START_END);
  }
  public void testChangeParent() throws Exception {
    testChangeParent(new HasOneToManySetJPA(), new HasOneToManySetJPA(), TXN_START_END);
  }
  public void testChangeParent_NoTxn() throws Exception {
    testChangeParent(new HasOneToManySetJPA(), new HasOneToManySetJPA(), NEW_EM_START_END);
  }
  public void testNewParentNewChild_NamedKeyOnChild() throws Exception {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManySetJPA(), TXN_START_END);
  }
  public void testNewParentNewChild_NamedKeyOnChild_NoTxn() throws Exception {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManySetJPA(), NEW_EM_START_END);
  }
  public void testAddAlreadyPersistedChildToParent_NoTxnSameEm() {
    testAddAlreadyPersistedChildToParent_NoTxnSameEm(new HasOneToManySetJPA());
  }
  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentEm() {
    testAddAlreadyPersistedChildToParent_NoTxnDifferentEm(new HasOneToManySetJPA());
  }
  public void testFetchOfOneToManyParentWithKeyPk() throws Exception {
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkSetJPA(), TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithKeyPk_NoTxn() throws Exception {
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkSetJPA(), NEW_EM_START_END);
  }
  public void testFetchOfOneToManyParentWithLongPk() throws Exception {
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkSetJPA(), TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithLongPk_NoTxn() throws Exception {
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkSetJPA(), NEW_EM_START_END);
  }
  public void testFetchOfOneToManyParentWithUnencodedStringPk() throws Exception {
    testFetchOfOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSetJPA(),
                                                    TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithUnencodedStringPk_NoTxn() throws Exception {
    testFetchOfOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSetJPA(),
                                                    NEW_EM_START_END);
  }
  public void testAddChildToOneToManyParentWithLongPk() throws Exception {
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkSetJPA(), new BidirectionalChildLongPkSetJPA(),
                                            TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithLongPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkSetJPA(), new BidirectionalChildLongPkSetJPA(),
                                            NEW_EM_START_END);
  }
  public void testAddChildToOneToManyParentWithUnencodedStringPk() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkSetJPA(), new BidirectionalChildUnencodedStringPkSetJPA(),
        TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithUnencodedStringPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkSetJPA(), new BidirectionalChildUnencodedStringPkSetJPA(),
        NEW_EM_START_END);
  }
  public void testAddQueriedParentToBidirChild() throws Exception {
    testAddQueriedParentToBidirChild(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                     TXN_START_END);
  }
  public void testAddQueriedParentToBidirChild_NoTxn() throws Exception {
    testAddQueriedParentToBidirChild(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                     NEW_EM_START_END);
  }
  public void testAddFetchedParentToBidirChild() throws Exception {
    testAddFetchedParentToBidirChild(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                     TXN_START_END);
  }
  public void testAddFetchedParentToBidirChild_NoTxn() throws Exception {
    testAddFetchedParentToBidirChild(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                     NEW_EM_START_END);
  }

  public void testOnlyOneParentPutOnParentAndChildUpdate() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                               TXN_START_END);
  }
  public void testOnlyOneParentPutOnParentAndChildUpdate_NoTxn() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                     NEW_EM_START_END);
  }

  public void testOnlyOnePutOnChildUpdate() throws Throwable {
    testOnlyOnePutOnChildUpdate(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                TXN_START_END);
  }
  public void testOnlyOnePutOnChildUpdate_NoTxn() throws Throwable {
    testOnlyOnePutOnChildUpdate(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                     NEW_EM_START_END);
  }

  public void testOnlyOneParentPutOnChildDelete() throws Throwable {
    // 1 put to remove the keys
    int expectedUpdatePuts = 1;
    testOnlyOneParentPutOnChildDelete(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                      TXN_START_END, expectedUpdatePuts);
  }

  public void testOnlyOneParentPutOnChildDelete_NoTxn() throws Throwable {
    // updates aren't necessarily atomic when non-tx, so get 1 after each collection clear.
    int expectedUpdatePuts = 4;
    testOnlyOneParentPutOnChildDelete(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(),
                                     NEW_EM_START_END, expectedUpdatePuts);
  }
}