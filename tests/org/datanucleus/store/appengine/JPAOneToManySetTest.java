// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.EntityNotFoundException;

import org.datanucleus.test.BidirectionalChildSetJPA;
import org.datanucleus.test.HasOneToManySetJPA;
import org.datanucleus.test.HasOneToManyWithNonDeletingCascadeSetJPA;
import org.datanucleus.test.HasOneToManyWithOrderByJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAOneToManySetTest extends JPAOneToManyTest {

  public void testInsert_NewParentAndChild() throws EntityNotFoundException {
    testInsert_NewParentAndChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA());
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    testInsert_ExistingParentNewChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA());
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    testUpdate_UpdateChildWithMerge(new BidirectionalChildSetJPA(), new HasOneToManySetJPA());
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    testUpdate_UpdateChild(new BidirectionalChildSetJPA(), new HasOneToManySetJPA());
  }

  public void testUpdate_NullOutChildren() throws EntityNotFoundException {
    testUpdate_NullOutChildren(new BidirectionalChildSetJPA(), new HasOneToManySetJPA());
  }
  public void testUpdate_ClearOutChildren() throws EntityNotFoundException {
    testUpdate_ClearOutChildren(new BidirectionalChildSetJPA(), new HasOneToManySetJPA());
  }
  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
    testUpdate_NullOutChild_NoDelete(new HasOneToManyWithNonDeletingCascadeSetJPA());
  }
  public void testUpdate_ClearOutChild_NoDelete() throws EntityNotFoundException {
    testUpdate_ClearOutChild_NoDelete(new HasOneToManyWithNonDeletingCascadeSetJPA());
  }
  public void testFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(HasOneToManyWithOrderByJPA.class);
  }
  public void testFind() throws EntityNotFoundException {
    testFind(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class);
  }
  public void testQuery() throws EntityNotFoundException {
    testQuery(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class);
  }
  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class);
  }
  public void testDeleteParentDeletesChild() throws EntityNotFoundException {
    testDeleteParentDeletesChild(HasOneToManySetJPA.class, BidirectionalChildSetJPA.class);
  }
  public void testRemoveObject() throws EntityNotFoundException {
    testRemoveObject(new HasOneToManySetJPA(), new BidirectionalChildSetJPA(nextNamedKey()),
        new BidirectionalChildSetJPA(nextNamedKey()));
  }
}