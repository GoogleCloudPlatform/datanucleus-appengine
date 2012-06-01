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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.jpa.HasChildWithSeparateNameFieldJPA;
import com.google.appengine.datanucleus.test.jpa.HasEncodedStringPkSeparateNameFieldJPA;
import com.google.appengine.datanucleus.test.jpa.HasKeyPkJPA;
import com.google.appengine.datanucleus.test.jpa.HasLongPkOneToManyBidirChildrenJPA;
import com.google.appengine.datanucleus.test.jpa.HasPolymorphicRelationsListJPA;
import com.google.appengine.datanucleus.test.jpa.HasUnencodedStringPkOneToManyBidirChildrenJPA;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirBottomList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirBottomLongPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirBottomStringPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirBottomUnencodedStringPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirMiddleList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirMiddleLongPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirMiddleStringPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirMiddleUnencodedStringPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirTopList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirTopLongPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirTopStringPkList;
import com.google.appengine.datanucleus.test.jpa.BidirectionalSingleTableChildListJPA.BidirTopUnencodedStringPkList;
import com.google.appengine.datanucleus.test.jpa.HasPolymorphicRelationsListJPA.HasOneToManyKeyPkListJPA;
import com.google.appengine.datanucleus.test.jpa.HasPolymorphicRelationsListJPA.HasOneToManyListJPA;
import com.google.appengine.datanucleus.test.jpa.HasPolymorphicRelationsListJPA.HasOneToManyLongPkListJPA;
import com.google.appengine.datanucleus.test.jpa.HasPolymorphicRelationsListJPA.HasOneToManyStringPkListJPA;
import com.google.appengine.datanucleus.test.jpa.HasPolymorphicRelationsListJPA.HasOneToManyUnencodedStringPkListJPA;
import com.google.appengine.datanucleus.test.jpa.HasPolymorphicRelationsListJPA.HasOneToManyWithUnsupportedInheritanceList;
import com.google.appengine.datanucleus.test.jpa.SubclassesJPA.MappedSuperclassChild;
import com.google.appengine.datanucleus.test.jpa.SubclassesJPA.TablePerClassChild;
import com.google.appengine.datanucleus.test.jpa.UnidirectionalSingeTableChildJPA.UnidirBottom;
import com.google.appengine.datanucleus.test.jpa.UnidirectionalSingeTableChildJPA.UnidirMiddle;
import com.google.appengine.datanucleus.test.jpa.UnidirectionalSingeTableChildJPA.UnidirTop;

import java.util.Collections;

import static com.google.appengine.datanucleus.PolymorphicTestUtils.assertKeyParentEquals;
import static com.google.appengine.datanucleus.PolymorphicTestUtils.getEntityKind;

public class JPAOneToManyPolymorphicListTest extends JPAOneToManyPolymorphicTestCase {

  public void testInsert_NewParentAndChild() throws Exception {
    testInsert_NewParentAndChild(new BidirTopList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Bottom, 1, 1);
    testInsert_NewParentAndChild(new BidirMiddleList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Middle, 2, 2);
    testInsert_NewParentAndChild(new BidirBottomList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Top, 3, 3);
  }

  public void testInsert_NewParentAndChild_NoTxn() throws Exception {
    testInsert_NewParentAndChild(new BidirBottomList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Middle, 1, 1);
    testInsert_NewParentAndChild(new BidirTopList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Bottom, 2, 2);
    testInsert_NewParentAndChild(new BidirMiddleList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Top, 3, 3);
  }

  public void testInsert_ExistingParentNewChild() throws Exception {
    testInsert_ExistingParentNewChild(new BidirTopList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Top, 1, 1);
    testInsert_ExistingParentNewChild(new BidirBottomList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Bottom, 2, 2);
    testInsert_ExistingParentNewChild(new BidirMiddleList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Middle, 3, 3);
  }

  public void testInsert_ExistingParentNewChild_NoTxn() throws Exception {
    testInsert_ExistingParentNewChild(new BidirMiddleList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Middle, 1, 1);
    testInsert_ExistingParentNewChild(new BidirBottomList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Top, 2, 2);
    testInsert_ExistingParentNewChild(new BidirTopList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Bottom, 3, 3);
  }

  public void testUpdate_UpdateChildWithMerge() throws Exception {
    testUpdate_UpdateChildWithMerge(new BidirMiddleList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Top, 1, 1);
    testUpdate_UpdateChildWithMerge(new BidirTopList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Bottom, 2, 2);
    testUpdate_UpdateChildWithMerge(new BidirBottomList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Middle, 3, 3);
  }

  public void testUpdate_UpdateChildWithMerge_NoTxn() throws Exception {
    testUpdate_UpdateChildWithMerge(new BidirTopList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Top, 1, 1);
    testUpdate_UpdateChildWithMerge(new BidirMiddleList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Middle, 2, 2);
    testUpdate_UpdateChildWithMerge(new BidirBottomList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Bottom, 3, 3);
  }

  public void testUpdate_UpdateChild() throws Exception {
    testUpdate_UpdateChild(new BidirMiddleList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Middle, 1, 1);
    testUpdate_UpdateChild(new BidirBottomList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Top, 2, 2);
    testUpdate_UpdateChild(new BidirTopList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Bottom, 3, 3);
  }

  public void testUpdate_UpdateChild_NoTxn() throws Exception {
    testUpdate_UpdateChild(new BidirTopList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Middle, 1, 1);
    testUpdate_UpdateChild(new BidirMiddleList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Top, 2, 2);
    testUpdate_UpdateChild(new BidirBottomList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Bottom, 3, 3);
  }

  public void testUpdate_NullOutChildren() throws Exception {
    testUpdate_NullOutChildren(new BidirTopList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Bottom, 1);
    testUpdate_NullOutChildren(new BidirBottomList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Middle, 2);
    testUpdate_NullOutChildren(new BidirMiddleList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Top, 3);
  }

  public void testUpdate_NullOutChildren_NoTxn() throws Exception {
    testUpdate_NullOutChildren(new BidirBottomList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Middle, 1);
    testUpdate_NullOutChildren(new BidirTopList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Top, 2);
    testUpdate_NullOutChildren(new BidirMiddleList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Bottom, 3);
  }

  public void testUpdate_ClearOutChildren() throws Exception {
    testUpdate_ClearOutChildren(new BidirBottomList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Top, 1);
    testUpdate_ClearOutChildren(new BidirMiddleList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Middle, 2);
    testUpdate_ClearOutChildren(new BidirTopList(), new HasOneToManyListJPA(),
        TXN_START_END, UnidirLevel.Bottom, 3);
  }
  public void testUpdate_ClearOutChildren_NoTxn() throws Exception {
    testUpdate_ClearOutChildren(new BidirTopList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Bottom, 1);
    testUpdate_ClearOutChildren(new BidirMiddleList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Middle, 2);
    testUpdate_ClearOutChildren(new BidirBottomList(), new HasOneToManyListJPA(),
        NEW_EM_START_END, UnidirLevel.Top, 3);
  }
  
  public void testFindWithOrderBy() throws Exception {
    testFindWithOrderBy(HasPolymorphicRelationsListJPA.HasOneToManyWithOrderByJPA.class,
                                      TXN_START_END);
  }
  public void testFindWithOrderBy_NoTxn() throws Exception {
    testFindWithOrderBy(HasPolymorphicRelationsListJPA.HasOneToManyWithOrderByJPA.class,
                                      NEW_EM_START_END);
  }

  public void testFind() throws Exception {
    testFind(TXN_START_END);
  }
  public void testFind_NoTxn() throws Exception {
    testFind(NEW_EM_START_END);
  }
  private void testFind(StartEnd startEnd) throws Exception {
    testFind(HasOneToManyListJPA.class, BidirTopList.class,
	startEnd, UnidirLevel.Top, getEntityKind(BidirTopList.class));
    testFind(HasOneToManyListJPA.class, BidirMiddleList.class,
	startEnd, UnidirLevel.Middle, getEntityKind(BidirTopList.class));
    testFind(HasOneToManyListJPA.class, BidirBottomList.class,
	startEnd, UnidirLevel.Bottom, getEntityKind(BidirTopList.class));
  }

  public void testQuery() throws Exception {
    testQuery(TXN_START_END);
  }  
  public void testQuery_NoTxn() throws Exception {
    testQuery(NEW_EM_START_END);
  }
  private void testQuery(StartEnd startEnd) throws Exception {
    testQuery(HasOneToManyListJPA.class, BidirBottomList.class,
	startEnd, UnidirLevel.Middle, getEntityKind(BidirTopList.class));
    testQuery(HasOneToManyListJPA.class, BidirTopList.class,
	startEnd, UnidirLevel.Top, getEntityKind(BidirTopList.class));
    testQuery(HasOneToManyListJPA.class, BidirMiddleList.class,
	startEnd, UnidirLevel.Bottom, getEntityKind(BidirTopList.class));
  }
  
  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyListJPA.class, BidirTopList.class,
	UnidirLevel.Middle, getEntityKind(BidirTopList.class));
    testChildFetchedLazily(HasOneToManyListJPA.class, BidirMiddleList.class,
	UnidirLevel.Bottom, getEntityKind(BidirTopList.class));
    testChildFetchedLazily(HasOneToManyListJPA.class, BidirBottomList.class,
	UnidirLevel.Top, getEntityKind(BidirTopList.class));
  }

  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(TXN_START_END);
  }
  public void testDeleteParentDeletesChild_NoTxn() throws Exception {
    testDeleteParentDeletesChild(NEW_EM_START_END);
  }
  private void testDeleteParentDeletesChild(StartEnd startEnd) throws Exception {
    testDeleteParentDeletesChild(HasOneToManyListJPA.class, BidirMiddleList.class,
	startEnd, UnidirLevel.Top, getEntityKind(BidirTopList.class));
    testDeleteParentDeletesChild(HasOneToManyListJPA.class, BidirTopList.class,
	startEnd, UnidirLevel.Bottom, getEntityKind(BidirTopList.class));
    testDeleteParentDeletesChild(HasOneToManyListJPA.class, BidirBottomList.class,
	startEnd, UnidirLevel.Middle, getEntityKind(BidirTopList.class));
  }

  public void testSwapAtPosition() throws Exception {
    testSwapAtPosition(TXN_START_END);
  }

  public void testSwapAtPosition_NoTxn() throws Exception {
    testSwapAtPosition(NEW_EM_START_END);
  }

  private void testSwapAtPosition(StartEnd startEnd) throws Exception {
    HasOneToManyListJPA pojo = new HasOneToManyListJPA();
    pojo.setVal("yar");
    BidirTopList bidir1 = new BidirTopList();
    BidirTopList bidir2 = new BidirBottomList();
    bidir2.setChildVal("yam");
    UnidirTop unidir = newUnidir(UnidirLevel.Bottom);
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getUnidirChildren().add(unidir);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir1);
    bidir1.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirTopList.class, 1, 1);

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getUnidirChildren().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().set(0, bidir2);

    UnidirTop unidir2 = newUnidir(UnidirLevel.Middle);
    String expectedName = unidir2.getName();
    unidir2.setStr("another title");
    pojo.getUnidirChildren().set(0, unidir2);

    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA();
    hasKeyPk2.setStr("another str");
    pojo.getHasKeyPks().set(0, hasKeyPk2);
    startEnd.end();

    startEnd.start();
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(unidir2.getId());
    assertNotNull(hasKeyPk2.getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(unidir2.getId())), pojoEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPk2.getId()), pojoEntity.getProperty("hasKeyPks"));

    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bookId));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(bidir2.getClass().getName(), bidirChildEntity.getProperty("DTYPE"));
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir2.getId()));
    assertNotNull(unidirEntity);
    assertEquals(expectedName, unidirEntity.getProperty("name"));
    assertEquals(UnidirLevel.Middle.discriminator, unidirEntity.getProperty("DTYPE"));
    assertEquals("another title", unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir2.getId()), unidirEntity.getKey());
    assertKeyParentEquals(pojo.getId(), unidirEntity, unidir2.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk2.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(unidirEntity.getKey()), pojoEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirTopList.class, 1, 1);
  }

  public void testRemoveObject() throws Exception {
    testRemoveObject(new HasOneToManyListJPA(), new BidirTopList(),
        new BidirMiddleList(), TXN_START_END, UnidirLevel.Top, UnidirLevel.Middle, 1);
    testRemoveObject(new HasOneToManyListJPA(), new BidirMiddleList(),
        new BidirBottomList(), TXN_START_END, UnidirLevel.Middle, UnidirLevel.Bottom, 2);
    testRemoveObject(new HasOneToManyListJPA(), new BidirTopList(),
        new BidirBottomList(), TXN_START_END, UnidirLevel.Bottom, UnidirLevel.Top, 3);
  }

  public void testRemoveObject_NoTxn() throws Exception {
    testRemoveObject(new HasOneToManyListJPA(), new BidirMiddleList(),
        new BidirTopList(), NEW_EM_START_END, UnidirLevel.Middle, UnidirLevel.Top, 1);
    testRemoveObject(new HasOneToManyListJPA(), new BidirBottomList(),
        new BidirTopList(), NEW_EM_START_END, UnidirLevel.Bottom, UnidirLevel.Top, 2);
    testRemoveObject(new HasOneToManyListJPA(), new BidirMiddleList(),
        new BidirBottomList(), NEW_EM_START_END, UnidirLevel.Top, UnidirLevel.Bottom, 3);
  }

  public void testRemoveAtPosition() throws Exception {
    testRemoveAtPosition(TXN_START_END);
  }

  public void testRemoveAtPosition_NoTxn() throws Exception {
    testRemoveAtPosition(NEW_EM_START_END);
  }

  private void testRemoveAtPosition(StartEnd startEnd) throws Exception {
    HasOneToManyListJPA pojo = new HasOneToManyListJPA();
    pojo.setVal("yar");
    BidirTopList bidir1 = new BidirBottomList();
    BidirTopList bidir2 = new BidirTopList();
    bidir1.setChildVal("yam1");
    bidir2.setChildVal("yam2");
    UnidirTop unidir1 = newUnidir(UnidirLevel.Top);
    UnidirTop unidir2 = newUnidir(UnidirLevel.Bottom);
    unidir2.setName("max");
    unidir2.setStr("another str");
    HasKeyPkJPA hasKeyPk1 = new HasKeyPkJPA();
    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA();
    hasKeyPk2.setStr("yar 2");
    pojo.getUnidirChildren().add(unidir1);
    pojo.getUnidirChildren().add(unidir2);
    pojo.getHasKeyPks().add(hasKeyPk1);
    pojo.getHasKeyPks().add(hasKeyPk2);
    pojo.getBidirChildren().add(bidir1);
    pojo.getBidirChildren().add(bidir2);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirTopList.class, 1, 2);

    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getUnidirChildren().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().remove(0);
    pojo.getUnidirChildren().remove(0);
    pojo.getHasKeyPks().remove(0);

    startEnd.start();
    em.merge(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(unidir2.getId());
    assertNotNull(hasKeyPk2.getId());
    assertEquals(bidir2.getId(), pojo.getBidirChildren().get(0).getId());
    assertEquals(hasKeyPk2.getId(), pojo.getHasKeyPks().get(0).getId());
    assertEquals(unidir2.getId(), pojo.getUnidirChildren().get(0).getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(unidir2.getId())), pojoEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPk2.getId()), pojoEntity.getProperty("hasKeyPks"));

    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(KeyFactory.stringToKey(bookId));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(bidir2.getClass().getName(), bidirChildEntity.getProperty("DTYPE"));
    assertEquals(bidir2.getPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals("yam2", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir2.getId()));
    assertNotNull(unidirEntity);
    assertEquals(UnidirLevel.Bottom.discriminator, unidirEntity.getProperty("DTYPE"));
    assertEquals("max", unidirEntity.getProperty("name"));
    assertEquals("another str", unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir2.getId()), unidirEntity.getKey());
    assertKeyParentEquals(pojo.getId(), unidirEntity, unidir2.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk2.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar 2", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(unidirEntity.getKey()), pojoEntity.getProperty("unidirChildren"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirTopList.class, 1, 1);
  }

  public void testAddAtPosition() throws Exception {
    testAddAtPosition(TXN_START_END);
  }
  public void testAddAtPosition_NoTxn() throws Exception {
    testAddAtPosition(NEW_EM_START_END);
  }

  private void testAddAtPosition(StartEnd startEnd) throws Exception {
    HasOneToManyListJPA pojo = new HasOneToManyListJPA();
    pojo.setVal("yar");
    BidirTopList bidir1 = new BidirBottomList();
    BidirTopList bidir2 = new BidirMiddleList();
    bidir1.setChildVal("yam1");
    bidir2.setChildVal("yam2");
    UnidirTop unidir1 = newUnidir(UnidirLevel.Middle);
    UnidirTop unidir2 = newUnidir(UnidirLevel.Top);
    unidir2.setName("max");
    unidir2.setStr("another str");
    HasKeyPkJPA hasKeyPk1 = new HasKeyPkJPA();
    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA();
    hasKeyPk2.setStr("yar 2");
    pojo.getUnidirChildren().add(unidir1);
    pojo.getHasKeyPks().add(hasKeyPk1);
    pojo.getBidirChildren().add(bidir1);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirTopList.class, 1, 1);

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getUnidirChildren().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().add(0, bidir2);
    pojo.getUnidirChildren().add(0, unidir2);
    pojo.getHasKeyPks().add(0, hasKeyPk2);
    startEnd.end();

    startEnd.start();
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getUnidirChildren().size());
    assertEquals(2, pojo.getHasKeyPks().size());
    assertEquals(2, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(unidir2.getId());
    assertNotNull(hasKeyPk2.getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(bidir2.getId()), KeyFactory.stringToKey(bidir1Id)), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(unidir2.getId()), KeyFactory.stringToKey(bookId)), pojoEntity.getProperty("unidirChildren"));
    assertEquals(Utils.newArrayList(hasKeyPk2.getId(), hasKeyPk1Key), pojoEntity.getProperty("hasKeyPks"));

    startEnd.end();

    ds.get(KeyFactory.stringToKey(bidir1Id));
    ds.get(KeyFactory.stringToKey(bookId));
    ds.get(hasKeyPk1Key);

    Entity bidirChildEntity1 = ds.get(KeyFactory.stringToKey(bidir1Id));
    assertNotNull(bidirChildEntity1);
    
    Entity bidirChildEntity2 = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity2);
    assertEquals("yam2", bidirChildEntity2.getProperty("childVal"));
    assertEquals(bidir2.getPropertyCount(), bidirChildEntity2.getProperties().size());
    assertEquals(bidir2.getClass().getName(), bidirChildEntity2.getProperty("DTYPE"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity2.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity2, bidir2.getId());

    Entity unidirEntity1 = ds.get(KeyFactory.stringToKey(bookId));
    Entity unidirEntity2 = ds.get(KeyFactory.stringToKey(unidir2.getId()));
    assertNotNull(unidirEntity2);
    assertEquals("max", unidirEntity2.getProperty("name"));
    assertEquals("another str", unidirEntity2.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir2.getId()), unidirEntity2.getKey());
    assertKeyParentEquals(pojo.getId(), unidirEntity2, unidir2.getId());

    Entity hasKeyPkEntity1 = ds.get(hasKeyPk1Key);
    Entity hasKeyPkEntity2 = ds.get(hasKeyPk2.getId());
    assertNotNull(hasKeyPkEntity2);
    assertEquals("yar 2", hasKeyPkEntity2.getProperty("str"));
    assertEquals(hasKeyPk2.getId(), hasKeyPkEntity2.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity2, hasKeyPk2.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity2.getKey(), bidirChildEntity1.getKey()), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(unidirEntity2.getKey(), unidirEntity1.getKey()), pojoEntity.getProperty("unidirChildren"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity2.getKey(), hasKeyPkEntity1.getKey()), pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirTopList.class, 1, 2);
  }

  public void testChangeParent() throws Exception {
    testChangeParent(new HasOneToManyListJPA(), new HasOneToManyListJPA(), TXN_START_END);
  }
  public void testChangeParent_NoTxn() throws Exception {
    testChangeParent(new HasOneToManyListJPA(), new HasOneToManyListJPA(), NEW_EM_START_END);
  }

  public void testNewParentNewChild_NamedKeyOnChild() throws Exception {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyListJPA(), TXN_START_END);
  }
  public void testNewParentNewChild_NamedKeyOnChild_NoTxn() throws Exception {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyListJPA(), NEW_EM_START_END);
  }

  public void testInsert_NewParentAndChild_LongPk() throws Exception {
    testInsert_NewParentAndChild_LongPk(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_LongPk_NoTxn() throws Exception {
    testInsert_NewParentAndChild_LongPk(NEW_EM_START_END);
  }
  private void testInsert_NewParentAndChild_LongPk(StartEnd startEnd) throws Exception {
    testInsert_NewParentAndChild_LongPk(new BidirTopLongPkList(), startEnd, 
	UnidirLevel.Top, "T", 1);
    testInsert_NewParentAndChild_LongPk(new BidirBottomLongPkList(), startEnd, 
	UnidirLevel.Top, "B", 2);
    testInsert_NewParentAndChild_LongPk(new BidirMiddleLongPkList(), startEnd, 
	UnidirLevel.Top, "M", 3);
  }  
  private void testInsert_NewParentAndChild_LongPk(BidirTopLongPkList bidirChild, StartEnd startEnd,
      UnidirLevel unidirLevel, String discriminator, int count) throws Exception {
    bidirChild.setChildVal("yam");

    UnidirTop unidir = newUnidir(UnidirLevel.Bottom);
    String expectedName = unidir.getName();
    String expectedStr = unidir.getStr();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    HasOneToManyLongPkListJPA parent = new HasOneToManyLongPkListJPA();
    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getUnidirChildren().add(unidir);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    startEnd.start();
    em.persist(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(unidir.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(discriminator, bidirChildEntity.getProperty("DTYPE"));
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirEntity);
    assertEquals(expectedName, unidirEntity.getProperty("name"));
    assertEquals(expectedStr, unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir.getId()), unidirEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), unidirEntity, unidir.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(KeyFactory.createKey(getEntityKind(parent.getClass()), parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(unidirEntity.getKey()), parentEntity.getProperty("unidirChildren"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertEquals(HasOneToManyLongPkListJPA.class.getName(), count, countForClass(HasOneToManyLongPkListJPA.class));
    assertEquals(BidirTopLongPkList.class.getName(), count, countForClass(BidirTopLongPkList.class));
    assertEquals(UnidirTop.class.getName(), count, countForClass(UnidirTop.class));
    assertEquals(HasKeyPkJPA.class.getName(), count, countForClass(HasKeyPkJPA.class));
  }

  public void testInsert_NewParentAndChild_StringPk() throws Exception {
    testInsert_NewParentAndChild_StringPk(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_StringPk_NoTxn() throws Exception {
    testInsert_NewParentAndChild_StringPk(NEW_EM_START_END);
  }
  private void testInsert_NewParentAndChild_StringPk(StartEnd startEnd) throws Exception {
    testInsert_NewParentAndChild_StringPk("yar", new BidirTopStringPkList(), startEnd,
	UnidirLevel.Top, new Long(1), 1);
    testInsert_NewParentAndChild_StringPk("yas", new BidirBottomStringPkList(), startEnd,
	UnidirLevel.Middle, new Long(3), 2);
    testInsert_NewParentAndChild_StringPk("yat", new BidirMiddleStringPkList(), startEnd,
	UnidirLevel.Bottom, new Long(2), 3);
  }
  
  private void testInsert_NewParentAndChild_StringPk(String id,BidirTopStringPkList bidirChild, 
      StartEnd startEnd, UnidirLevel unidirLevel, Long discriminator, int count) throws Exception {
    bidirChild.setChildVal("yam");

    UnidirTop unidir = newUnidir(unidirLevel);
    String expectedName = unidir.getName();
    String expectedStr = unidir.getStr();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    HasOneToManyStringPkListJPA parent = new HasOneToManyStringPkListJPA();
    parent.setId(id);
    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getUnidirChildren().add(unidir);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    startEnd.start();
    em.persist(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(unidir.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals(discriminator, bidirChildEntity.getProperty("DISCRIMINATOR"));
    assertEquals(bidirChild.getPropertyCount(), bidirChildEntity.getProperties().size());
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity unidirEntity = ds.get(KeyFactory.stringToKey(unidir.getId()));
    assertNotNull(unidirEntity);
    assertEquals(expectedName, unidirEntity.getProperty("name"));
    assertEquals(expectedStr, unidirEntity.getProperty("str"));
    assertEquals(KeyFactory.stringToKey(unidir.getId()), unidirEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), unidirEntity, unidir.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(KeyFactory.createKey(getEntityKind(parent.getClass()), parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(unidirEntity.getKey()), parentEntity.getProperty("unidirChildren"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertEquals(HasOneToManyStringPkListJPA.class.getName(), count, countForClass(HasOneToManyStringPkListJPA.class));
    assertEquals(BidirTopStringPkList.class.getName(), count, countForClass(BidirTopStringPkList.class));
    assertEquals(UnidirTop.class.getName(), count, countForClass(UnidirTop.class));
    assertEquals(HasKeyPkJPA.class.getName(), count, countForClass(HasKeyPkJPA.class));
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnSameEm() {
    testAddAlreadyPersistedChildToParent_NoTxnSameEm(new HasOneToManyListJPA());
  }

  public void testAddAlreadyPersistedChildToParent_NoTxnDifferentEm() {
    testAddAlreadyPersistedChildToParent_NoTxnDifferentEm(new HasOneToManyListJPA());
  }

  public void testLongPkOneToManyBidirChildren() throws Exception {
    testLongPkOneToManyBidirChildren(TXN_START_END);
  }
  public void testLongPkOneToManyBidirChildren_NoTxn() throws Exception {
    testLongPkOneToManyBidirChildren(NEW_EM_START_END);
  }
  private void testLongPkOneToManyBidirChildren(StartEnd startEnd)
      throws Exception {
    HasLongPkOneToManyBidirChildrenJPA pojo = new HasLongPkOneToManyBidirChildrenJPA();
    HasLongPkOneToManyBidirChildrenJPA.ChildA
        a = new HasLongPkOneToManyBidirChildrenJPA.ChildA();
    pojo.setChildAList(Utils.newArrayList(a));
    startEnd.start();
    em.persist(pojo);
    startEnd.end();
    startEnd.start();
    pojo = em.find(HasLongPkOneToManyBidirChildrenJPA.class, pojo.getId());
    assertEquals(1, pojo.getChildAList().size());
    assertEquals(pojo, pojo.getChildAList().get(0).getParent());
    startEnd.end();
  }

  public void testUnencodedStringPkOneToManyBidirChildren() throws Exception {
    testUnencodedStringPkOneToManyBidirChildren(TXN_START_END);
  }
  public void testUnencodedStringPkOneToManyBidirChildren_NoTxn() throws Exception {
    testUnencodedStringPkOneToManyBidirChildren(NEW_EM_START_END);
  }
  private void testUnencodedStringPkOneToManyBidirChildren(StartEnd startEnd)
      throws Exception {
    HasUnencodedStringPkOneToManyBidirChildrenJPA
        pojo = new HasUnencodedStringPkOneToManyBidirChildrenJPA();
    pojo.setId("yar");
    HasUnencodedStringPkOneToManyBidirChildrenJPA.ChildA
        a = new HasUnencodedStringPkOneToManyBidirChildrenJPA.ChildA();
    pojo.setChildAList(Utils.newArrayList(a));
    startEnd.start();
    em.persist(pojo);
    startEnd.end();
    startEnd.start();
    pojo = em.find(HasUnencodedStringPkOneToManyBidirChildrenJPA.class, pojo.getId());
    assertEquals(1, pojo.getChildAList().size());
    assertEquals(pojo, pojo.getChildAList().get(0).getParent());
    startEnd.end();
  }

  public void testFetchOfOneToManyParentWithKeyPk() throws Exception {
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkListJPA(), TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithKeyPk_NoTxn() throws Exception {
    testFetchOfOneToManyParentWithKeyPk(new HasOneToManyKeyPkListJPA(), NEW_EM_START_END);
  }

  public void testFetchOfOneToManyParentWithLongPk() throws Exception {
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithLongPk_NoTxn() throws Exception {
    testFetchOfOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), NEW_EM_START_END);
  }

  public void testFetchOfOneToManyParentWithUnencodedStringPk() throws Exception {
    testFetchOfOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkListJPA(), TXN_START_END);
  }
  public void testFetchOfOneToManyParentWithUnencodedStringPk_NoTxn() throws Exception {
    testFetchOfOneToManyParentWithUnencodedStringPk(new HasOneToManyUnencodedStringPkListJPA(), NEW_EM_START_END);
  }

  public void testAddChildToOneToManyParentWithLongPk() throws Exception {
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), new BidirMiddleLongPkList(), 
	TXN_START_END, UnidirLevel.Top, "M");
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), new BidirTopLongPkList(), 
	TXN_START_END, UnidirLevel.Middle, "T");
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), new BidirBottomLongPkList(), 
	TXN_START_END, UnidirLevel.Bottom, "B");
  }
  public void testAddChildToOneToManyParentWithLongPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), new BidirTopLongPkList(), 
	NEW_EM_START_END, UnidirLevel.Bottom, "T");
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), new BidirMiddleLongPkList(), 
	NEW_EM_START_END, UnidirLevel.Middle, "M");
    testAddChildToOneToManyParentWithLongPk(new HasOneToManyLongPkListJPA(), new BidirBottomLongPkList(), 
	NEW_EM_START_END, UnidirLevel.Top, "B");
  }

  public void testAddChildToOneToManyParentWithUnencodedStringPk() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithUnencodedStringPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(NEW_EM_START_END);
  }
  private void testAddChildToOneToManyParentWithUnencodedStringPk(StartEnd startEnd) throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkListJPA(), new BidirTopUnencodedStringPkList(),
        TXN_START_END, UnidirLevel.Middle, "A");
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkListJPA(), new BidirMiddleUnencodedStringPkList(),
        TXN_START_END, UnidirLevel.Bottom, "B");
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkListJPA(), new BidirBottomUnencodedStringPkList(),
        TXN_START_END, UnidirLevel.Top, "C");
  }

  public void testOneToManyChildAtMultipleLevels() throws Exception {
    testOneToManyChildAtMultipleLevels(TXN_START_END);
  }
  public void testOneToManyChildAtMultipleLevels_NoTxn() throws Exception {
    testOneToManyChildAtMultipleLevels(NEW_EM_START_END);
  }
  private void testOneToManyChildAtMultipleLevels(StartEnd startEnd)
      throws Exception {
    HasPolymorphicRelationsListJPA.HasOneToManyChildAtMultipleLevelsJPA pojo = new HasPolymorphicRelationsListJPA.HasOneToManyChildAtMultipleLevelsJPA();
    UnidirTop unidir1 = new UnidirTop();
    pojo.setUnidirChildren(Utils.newArrayList(unidir1));
    HasPolymorphicRelationsListJPA.HasOneToManyChildAtMultipleLevelsJPA child = new HasPolymorphicRelationsListJPA.HasOneToManyChildAtMultipleLevelsJPA();
    UnidirTop unidir2 = new UnidirMiddle();
    child.setUnidirChildren(Utils.newArrayList(unidir2));
    pojo.setChild(child);
    startEnd.start();
    em.persist(pojo);
    startEnd.end();
    startEnd.start();
    assertEquals(2, countForClass(UnidirTop.class));
    pojo = em.find(HasPolymorphicRelationsListJPA.HasOneToManyChildAtMultipleLevelsJPA.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(1, pojo.getUnidirChildren().size());
    assertEquals(pojo.getUnidirChildren().get(0), unidir1);
    assertEquals(child.getUnidirChildren().get(0), unidir2);
    assertEquals(1, child.getUnidirChildren().size());
    startEnd.end();
  }
  
  public void testAddQueriedParentToBidirChild() throws Exception {
    testAddQueriedParentToBidirChild(TXN_START_END);
  }
  public void testAddQueriedParentToBidirChild_NoTxn() throws Exception {
    testAddQueriedParentToBidirChild(NEW_EM_START_END);
  }
  private void testAddQueriedParentToBidirChild(StartEnd startEnd) throws Exception {
    testAddQueriedParentToBidirChild(new HasOneToManyListJPA(), new BidirTopList(),
	startEnd);
    testAddQueriedParentToBidirChild(new HasOneToManyListJPA(), new BidirMiddleList(),
	startEnd);
    testAddQueriedParentToBidirChild(new HasOneToManyListJPA(), new BidirBottomList(),
	startEnd);
  }

  public void testAddFetchedParentToBidirChild() throws Exception {
    testAddFetchedParentToBidirChild(TXN_START_END);
  }
  public void testAddFetchedParentToBidirChild_NoTxn() throws Exception {
    testAddFetchedParentToBidirChild(NEW_EM_START_END);
  }
  private void testAddFetchedParentToBidirChild(StartEnd startEnd) throws Exception {
    testAddFetchedParentToBidirChild(new HasOneToManyListJPA(), new BidirBottomList(),
        startEnd);
    testAddFetchedParentToBidirChild(new HasOneToManyListJPA(), new BidirMiddleList(),
        startEnd);
    testAddFetchedParentToBidirChild(new HasOneToManyListJPA(), new BidirTopList(),
        startEnd);
  }

  public void testDeleteChildWithSeparateNameField() throws Exception {
    testDeleteChildWithSeparateNameField(TXN_START_END);
  }
  public void testDeleteChildWithSeparateNameField_NoTxn() throws Exception {
    testDeleteChildWithSeparateNameField(TXN_START_END);
  }
  private void testDeleteChildWithSeparateNameField(StartEnd startEnd) throws Exception {
    HasChildWithSeparateNameFieldJPA parent = new HasChildWithSeparateNameFieldJPA();
    HasEncodedStringPkSeparateNameFieldJPA child = new HasEncodedStringPkSeparateNameFieldJPA();
    child.setName("the name");
    parent.getChildren().add(child);
    startEnd.start();
    em.persist(parent);
    startEnd.end();
    startEnd.start();
    parent = em.find(HasChildWithSeparateNameFieldJPA.class, parent.getId());
    em.remove(parent);
    startEnd.end();
  }

  public void testOnlyOneParentPutOnParentAndChildUpdate() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(TXN_START_END);
  }
  public void testOnlyOneParentPutOnParentAndChildUpdate_NoTxn() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(NEW_EM_START_END);
  }
  private void testOnlyOneParentPutOnParentAndChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManyListJPA(), new BidirTopList(),
	startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManyListJPA(), new BidirMiddleList(),
	startEnd);
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManyListJPA(), new BidirBottomList(),
	startEnd);
  }

  public void testOnlyOnePutOnChildUpdate() throws Throwable {
    testOnlyOnePutOnChildUpdate(TXN_START_END);
  }
  public void testOnlyOnePutOnChildUpdate_NoTxn() throws Throwable {
    testOnlyOnePutOnChildUpdate(NEW_EM_START_END);
  }
  private void testOnlyOnePutOnChildUpdate(StartEnd startEnd) throws Throwable {
    testOnlyOnePutOnChildUpdate(new HasOneToManyListJPA(), new BidirTopList(),
	startEnd);
    testOnlyOnePutOnChildUpdate(new HasOneToManyListJPA(), new BidirMiddleList(),
	startEnd);
    testOnlyOnePutOnChildUpdate(new HasOneToManyListJPA(), new BidirBottomList(),
	startEnd);
  }

  public void testOnlyOneParentPutOnChildDelete() throws Throwable {
    // 1 put to remove the keys
    int expectedUpdatePuts = 1;
    testOnlyOneParentPutOnChildDelete(new HasOneToManyListJPA(), new BidirTopList(),
                                      TXN_START_END, expectedUpdatePuts);
  }

  public void testOnlyOneParentPutOnChildDelete_NoTxn() throws Throwable {
    // updates are now atomic when non-tx, so get 1 after each collection clear and one for the update.
    int expectedUpdatePuts = 5;
    testOnlyOneParentPutOnChildDelete(new HasOneToManyListJPA(), new BidirTopList(),
                                      NEW_EM_START_END, expectedUpdatePuts);
  }
  
  public void testUnsupportedInheritanceMappings() {
    testUnsupportedInheritanceMappings(TXN_START_END);
  }
  public void testUnsupportedInheritanceMappings_NoTxn() {
    testUnsupportedInheritanceMappings(NEW_EM_START_END);
  }

  private void testUnsupportedInheritanceMappings(StartEnd startEnd) {
    // Make sure the child classes are known about otherwise the test is flawed
    getExecutionContext().getMetaDataManager().getMetaDataForClass(TablePerClassChild.class,
      getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getMetaDataManager().getMetaDataForClass(MappedSuperclassChild.class,
        getExecutionContext().getClassLoaderResolver());

    HasOneToManyWithUnsupportedInheritanceList parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren1().add(new TablePerClassChild());
    makePersistentWithExpectedException(startEnd, parent);

    parent = new HasOneToManyWithUnsupportedInheritanceList();
    parent.getChildren2().add(new MappedSuperclassChild());
    makePersistentWithExpectedException(startEnd, parent);
  }
  private void makePersistentWithExpectedException(StartEnd startEnd,
      HasOneToManyWithUnsupportedInheritanceList parent) {
    startEnd.start();
    try {
      em.persist(parent);
      startEnd.end();
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException uso) {
      assertTrue(uso.getMessage().contains("superclass-table"));
    }
  }
  
  @Override
  protected void registerSubclasses() {
    // Make sure all subclasses of UnidirTop and BidirTopList are known. 
    // Only the meta data of the top class in the inheritance tree 
    // (element type of the collections) is known otherwise
    // when getting the pojo. 
    getExecutionContext().getStoreManager().addClass(UnidirMiddle.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(UnidirBottom.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(BidirMiddleList.class.getName(),
	getExecutionContext().getClassLoaderResolver());
    getExecutionContext().getStoreManager().addClass(BidirBottomList.class.getName(),
	getExecutionContext().getClassLoaderResolver());
  }
}
