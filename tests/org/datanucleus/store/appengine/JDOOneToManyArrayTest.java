// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.EntityNotFoundException;

import org.datanucleus.test.BidirectionalChildArrayJDO;
import org.datanucleus.test.HasOneToManyArrayJDO;
import org.datanucleus.test.HasOneToManyArrayWithOrderByJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManyArrayTest extends JDOOneToManyTestCase {

  public void testInsertNewParentAndChild() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    testInsert_NewParentAndChild(parent, bidirChild);
  }

  // TODO(maxr) This doesn't work for rdbms either.  Find out why.
  public void xxxtestInsertExistingParentNewChild() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    testInsert_ExistingParentNewChild(parent, bidirChild);
  }

  // TODO(maxr): Doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestSwapAtPosition_Array() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild2 = new BidirectionalChildArrayJDO();
    testSwapAtPosition(parent, bidirChild, bidirChild2);
  }

  // TODO(maxr): Doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestRemoveAtPosition_Array() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild2 = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild3 = new BidirectionalChildArrayJDO();
    testRemoveAtPosition(parent, bidirChild, bidirChild2, bidirChild3);
  }

  // TODO(maxr): Doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestAddAtPosition_Array() throws EntityNotFoundException {
    HasOneToManyArrayJDO parent = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidirChild = new BidirectionalChildArrayJDO();
    BidirectionalChildArrayJDO bidirChild2 = new BidirectionalChildArrayJDO();
    testAddAtPosition(parent, bidirChild, bidirChild2);
  }

  public void testUpdateUpdateChildWithMerge() throws EntityNotFoundException {
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
    testUpdate_UpdateChildWithMerge(pojo, bidir);
  }

  public void testUpdateUpdateChild() throws EntityNotFoundException {
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
    testUpdate_UpdateChild(pojo, bidir);
  }

  // TODO(maxr) This doesn't work for RDBMS either.  Submit bug to Andy.
  public void xxxtestUpdateNullOutChildren() throws EntityNotFoundException {
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
    testUpdate_NullOutChildren(pojo, bidir);
  }

  // DataNucleus doesn't detect changes to array elements
  // so this won't work.
  public void xxxtestUpdateClearOutChildren() throws EntityNotFoundException {
    HasOneToManyArrayJDO pojo = new HasOneToManyArrayJDO();
    BidirectionalChildArrayJDO bidir = new BidirectionalChildArrayJDO();
    testUpdate_ClearOutChildren(pojo, bidir);
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
    testFindWithOrderBy(HasOneToManyArrayWithOrderByJDO.class);
  }

  public void testFind() throws EntityNotFoundException {
    testFind(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class);
  }

  public void testQuery() throws EntityNotFoundException {
    testQuery(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class);
  }

  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class);
  }

  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(HasOneToManyArrayJDO.class, BidirectionalChildArrayJDO.class);
  }

  public void testChangeParent() {
    testChangeParent(new HasOneToManyArrayJDO(), new HasOneToManyArrayJDO());
  }

  public void testNewParentNewChild_NamedKeyOnChild() throws EntityNotFoundException {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyArrayJDO());
  }

  @Override
  boolean isIndexed() {
    return true;
  }
}