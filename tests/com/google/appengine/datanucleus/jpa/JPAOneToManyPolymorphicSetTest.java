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
package com.google.appengine.datanucleus.jpa;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirBottomLongPkSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirBottomSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirBottomUnencodedStringPkSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirMiddleLongPkSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirMiddleSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirMiddleUnencodedStringPkSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirTopLongPkSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirTopSet;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirTopUnencodedStringPkSet;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsSetJPA.HasOneToManyKeyPkSetJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsSetJPA.HasOneToManyLongPkSetJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsSetJPA.HasOneToManySetJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsSetJPA.HasOneToManyUnencodedStringPkSetJPA;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirBottom;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirMiddle;

import static com.google.appengine.datanucleus.PolymorphicTestUtils.getEntityKind;

public class JPAOneToManyPolymorphicSetTest extends JPAOneToManyPolymorphicTestCase {
  
  public void testInsert_NewParentAndChild() throws Exception {
    testInsert_NewParentAndChild(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_NoTxn() throws Exception {
    testInsert_NewParentAndChild(NEW_EM_START_END);
  }
  private void testInsert_NewParentAndChild(StartEnd startEnd) throws Exception {
    testInsert_NewParentAndChild(new BidirTopSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Middle, 1, 1);
    testInsert_NewParentAndChild(new BidirBottomSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Top, 2, 2);
    testInsert_NewParentAndChild(new BidirMiddleSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Bottom, 3, 3);
  }

  public void testInsert_ExistingParentNewChild() throws Exception {
    NucleusLogger.GENERAL.info(">> testInsert_ExistingParentNewChild START");
    testInsert_ExistingParentNewChild(TXN_START_END);
    NucleusLogger.GENERAL.info(">> testInsert_ExistingParentNewChild END");
  }
  public void testInsert_ExistingParentNewChild_NoTxn() throws Exception {
    testInsert_ExistingParentNewChild(NEW_EM_START_END);
  }
  private void testInsert_ExistingParentNewChild(StartEnd startEnd) throws Exception {
    testInsert_ExistingParentNewChild(new BidirMiddleSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Bottom, 1, 1);
    testInsert_ExistingParentNewChild(new BidirBottomSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Middle, 2, 2);
    testInsert_ExistingParentNewChild(new BidirTopSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Top, 3, 3);
  }

  public void testUpdate_UpdateChildWithMerge() throws Exception {
    testUpdate_UpdateChildWithMerge(TXN_START_END);
  }
  public void testUpdate_UpdateChildWithMerge_NoTxn() throws Exception {
    testUpdate_UpdateChildWithMerge(NEW_EM_START_END);
  }
  private void testUpdate_UpdateChildWithMerge(StartEnd startEnd) throws Exception {
    testUpdate_UpdateChildWithMerge(new BidirBottomSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Top, 1, 1);
    testUpdate_UpdateChildWithMerge(new BidirMiddleSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Middle, 2, 2);
    testUpdate_UpdateChildWithMerge(new BidirTopSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Bottom, 3, 3);
  }

  public void testUpdate_UpdateChild() throws Exception {
    testUpdate_UpdateChild(TXN_START_END);
  }
  public void testUpdate_UpdateChild_NoTxn() throws Exception {
    testUpdate_UpdateChild(NEW_EM_START_END);
  }
  private void testUpdate_UpdateChild(StartEnd startEnd) throws Exception {
    testUpdate_UpdateChild(new BidirBottomSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Middle, 1, 1);
    testUpdate_UpdateChild(new BidirMiddleSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Bottom, 2, 2);
    testUpdate_UpdateChild(new BidirTopSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Top, 3, 3);
  }

  public void testUpdate_NullOutChildren() throws Exception {
    testUpdate_NullOutChildren(TXN_START_END);
  }
  public void testUpdate_NullOutChildren_NoTxn() throws Exception {
    testUpdate_NullOutChildren(NEW_EM_START_END);
  }
  private void testUpdate_NullOutChildren(StartEnd startEnd) throws Exception {
    testUpdate_NullOutChildren(new BidirBottomSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Middle, 1);
    testUpdate_NullOutChildren(new BidirMiddleSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Top, 2);
    testUpdate_NullOutChildren(new BidirTopSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Bottom, 3);
  }

  public void testUpdate_ClearOutChildren() throws Exception {
    testUpdate_ClearOutChildren(TXN_START_END);
  }
  public void testUpdate_ClearOutChildren_NoTxn() throws Exception {
    testUpdate_ClearOutChildren(NEW_EM_START_END);
  }
  private void testUpdate_ClearOutChildren(StartEnd startEnd) throws Exception {
    testUpdate_ClearOutChildren(new BidirMiddleSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Top, 1);
    testUpdate_ClearOutChildren(new BidirTopSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Middle, 2);
    testUpdate_ClearOutChildren(new BidirBottomSet(), new HasOneToManySetJPA(),
	startEnd, UnidirLevel.Bottom, 3);
  }

  public void testFind() throws Exception {
    testFind(TXN_START_END);
  }
  public void testFind_NoTxn() throws Exception {
    testFind(NEW_EM_START_END);
  }
  private void testFind(StartEnd startEnd) throws Exception {
    testFind(HasOneToManySetJPA.class, BidirTopSet.class, 
	startEnd, UnidirLevel.Top, getEntityKind(BidirTopSet.class));
    testFind(HasOneToManySetJPA.class, BidirMiddleSet.class, 
	startEnd, UnidirLevel.Bottom, getEntityKind(BidirTopSet.class));
    testFind(HasOneToManySetJPA.class, BidirBottomSet.class, 
	startEnd, UnidirLevel.Middle, getEntityKind(BidirTopSet.class));
  }

  public void testQuery() throws Exception {
    testQuery(TXN_START_END);
  }
  public void testQuery_NoTxn() throws Exception {
    testQuery(NEW_EM_START_END);
  }
  private void testQuery(StartEnd startEnd) throws Exception {
    testQuery(HasOneToManySetJPA.class, BidirMiddleSet.class, 
	startEnd, UnidirLevel.Top, getEntityKind(BidirTopSet.class));
    testQuery(HasOneToManySetJPA.class, BidirTopSet.class, 
	startEnd, UnidirLevel.Middle, getEntityKind(BidirTopSet.class));
    testQuery(HasOneToManySetJPA.class, BidirBottomSet.class, 
	startEnd, UnidirLevel.Bottom, getEntityKind(BidirTopSet.class));
  }
  
  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManySetJPA.class, BidirTopSet.class,
	UnidirLevel.Middle, getEntityKind(BidirTopSet.class));
    testChildFetchedLazily(HasOneToManySetJPA.class, BidirMiddleSet.class,
	UnidirLevel.Top, getEntityKind(BidirTopSet.class));
    testChildFetchedLazily(HasOneToManySetJPA.class, BidirBottomSet.class,
	UnidirLevel.Bottom, getEntityKind(BidirTopSet.class));
  }
  
  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(TXN_START_END);
  }  
  public void testDeleteParentDeletesChild_NoTxn() throws Exception {
    testDeleteParentDeletesChild(NEW_EM_START_END);
  }
  private void testDeleteParentDeletesChild(StartEnd startEnd) throws Exception {
    testDeleteParentDeletesChild(HasOneToManySetJPA.class, BidirBottomSet.class,
	startEnd, UnidirLevel.Top, getEntityKind(BidirTopSet.class));
    testDeleteParentDeletesChild(HasOneToManySetJPA.class, BidirMiddleSet.class,
	startEnd, UnidirLevel.Bottom, getEntityKind(BidirTopSet.class));
    testDeleteParentDeletesChild(HasOneToManySetJPA.class, BidirTopSet.class,
	startEnd, UnidirLevel.Middle, getEntityKind(BidirTopSet.class));
  }
  
  public void testRemoveObject() throws Exception {
    testRemoveObject(TXN_START_END);
  }
  public void testRemoveObject_NoTxn() throws Exception {
    testRemoveObject(NEW_EM_START_END);
  }
  private void testRemoveObject(StartEnd startEnd) throws Exception {
    testRemoveObject(new HasOneToManySetJPA(), new BidirTopSet(nextNamedKey()),
        new BidirTopSet(nextNamedKey()), startEnd, UnidirLevel.Top, UnidirLevel.Bottom, 1);
    testRemoveObject(new HasOneToManySetJPA(), new BidirBottomSet(nextNamedKey()),
        new BidirTopSet(nextNamedKey()), startEnd, UnidirLevel.Middle, UnidirLevel.Top, 2);
    testRemoveObject(new HasOneToManySetJPA(), new BidirMiddleSet(nextNamedKey()),
        new BidirTopSet(nextNamedKey()), startEnd, UnidirLevel.Bottom, UnidirLevel.Middle, 3);
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
    testAddChildToOneToManyParentWithLongPk(TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithLongPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithLongPk(NEW_EM_START_END);
  }      
  private void testAddChildToOneToManyParentWithLongPk(StartEnd startEnd) throws Exception {
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkSetJPA(), new BidirBottomLongPkSet(),
	startEnd, UnidirLevel.Top, "B");
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkSetJPA(), new BidirMiddleLongPkSet(),
	startEnd, UnidirLevel.Bottom, "M");
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkSetJPA(), new BidirTopLongPkSet(),
	startEnd, UnidirLevel.Middle, "T");
  }

  public void testAddChildToOneToManyParentWithUnencodedStringPk() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithUnencodedStringPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(NEW_EM_START_END);
  }
  private void testAddChildToOneToManyParentWithUnencodedStringPk(StartEnd startEnd) throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSetJPA(), 
	new BidirBottomUnencodedStringPkSet(), startEnd, UnidirLevel.Top, "C");
    testAddChildToOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSetJPA(), 
	new BidirTopUnencodedStringPkSet(), startEnd, UnidirLevel.Middle, "A");
    testAddChildToOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkSetJPA(), 
	new BidirMiddleUnencodedStringPkSet(), startEnd, UnidirLevel.Bottom, "B");
  }
  
  public void testAddQueriedParentToBidirChild() throws Exception {
    testAddQueriedParentToBidirChild(TXN_START_END);
  }
  public void testAddQueriedParentToBidirChild_NoTxn() throws Exception {
    testAddQueriedParentToBidirChild(NEW_EM_START_END);
  }
  private void testAddQueriedParentToBidirChild(StartEnd startEnd) throws Exception {
    testAddQueriedParentToBidirChild(new HasOneToManySetJPA(), new BidirMiddleSet(),
	startEnd);
    testAddQueriedParentToBidirChild(new HasOneToManySetJPA(), new BidirTopSet(),
	startEnd);
    testAddQueriedParentToBidirChild(new HasOneToManySetJPA(), new BidirBottomSet(),
	startEnd);
  }
  public void testAddFetchedParentToBidirChild() throws Exception {
    testAddFetchedParentToBidirChild(TXN_START_END);
  }  
  public void testAddFetchedParentToBidirChild_NoTxn() throws Exception {
    testAddFetchedParentToBidirChild(NEW_EM_START_END);
  }
  public void testAddFetchedParentToBidirChild(StartEnd startEnd) throws Exception {
    testAddFetchedParentToBidirChild(new HasOneToManySetJPA(), new BidirBottomSet(),
	startEnd);
    testAddFetchedParentToBidirChild(new HasOneToManySetJPA(), new BidirTopSet(),
	startEnd);
    testAddFetchedParentToBidirChild(new HasOneToManySetJPA(), new BidirMiddleSet(),
	startEnd);
  }

  public void testOnlyOneParentPutOnParentAndChildUpdate() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(TXN_START_END);
  }
  public void testOnlyOneParentPutOnParentAndChildUpdate_NoTxn() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(NEW_EM_START_END);
  }
  private void testOnlyOneParentPutOnParentAndChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManySetJPA(), new BidirTopSet(),
        startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManySetJPA(), new BidirMiddleSet(),
        startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManySetJPA(), new BidirBottomSet(),
        startEnd);
  }

  public void testOnlyOnePutOnChildUpdate() throws Throwable {
    testOnlyOnePutOnChildUpdate(TXN_START_END);
  }
  public void testOnlyOnePutOnChildUpdate_NoTxn() throws Throwable {
    testOnlyOnePutOnChildUpdate(NEW_EM_START_END);
  }
  private void testOnlyOnePutOnChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOnePutOnChildUpdate(new HasOneToManySetJPA(), new BidirTopSet(),
	startEnd);
    testOnlyOnePutOnChildUpdate(new HasOneToManySetJPA(), new BidirMiddleSet(),
	startEnd);
    testOnlyOnePutOnChildUpdate(new HasOneToManySetJPA(), new BidirBottomSet(),
	startEnd);
  }

  public void testOnlyOneParentPutOnChildDelete() throws Throwable {
    // 1 put to remove the keys
    int expectedUpdatePuts = 1;
    testOnlyOneParentPutOnChildDelete(new HasOneToManySetJPA(), new BidirTopSet(),
                                      TXN_START_END, expectedUpdatePuts);
  }

  public void testOnlyOneParentPutOnChildDelete_NoTxn() throws Throwable {
    // updates are atomic when non-tx, so get 1 after each collection clear, and 1 for the update.
    int expectedUpdatePuts = 4;
    testOnlyOneParentPutOnChildDelete(new HasOneToManySetJPA(), new BidirTopSet(),
                                     NEW_EM_START_END, expectedUpdatePuts);
  }
  
  @Override
  protected void registerSubclasses() {
    // Make sure all subclasses of UnidirTop, BidirTopSet are known. Only the meta data
    // of the top class in the inheritance tree (element type of the collections)
    // is known otherwise when getting the pojo. 
    getExecutionContext().getStoreManager().addClass(UnidirMiddle.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(UnidirBottom.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(BidirMiddleSet.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(BidirBottomSet.class.getName(),
	getExecutionContext().getClassLoaderResolver());
  }

}
