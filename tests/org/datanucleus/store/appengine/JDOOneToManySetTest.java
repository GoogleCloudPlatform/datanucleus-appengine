// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.EntityNotFoundException;

import org.datanucleus.test.BidirectionalChildSetJDO;
import org.datanucleus.test.HasOneToManySetJDO;
import org.datanucleus.test.HasOneToManyWithOrderByJDO;
import org.datanucleus.test.HasOneToManyListJDO;
import org.datanucleus.test.BidirectionalChildListJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOOneToManySetTest extends JDOOneToManyTest {

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

  public void testFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(HasOneToManyWithOrderByJDO.class);
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

  boolean isIndexed() {
    return false;
  }
}