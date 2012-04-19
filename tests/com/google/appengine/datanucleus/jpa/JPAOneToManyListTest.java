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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.TestUtils;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.BidirectionalChildListJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildLongPkListJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildStringPkListJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildUnencodedStringPkListJPA;
import com.google.appengine.datanucleus.test.Book;
import com.google.appengine.datanucleus.test.HasChildWithSeparateNameFieldJPA;
import com.google.appengine.datanucleus.test.HasEncodedStringPkSeparateNameFieldJPA;
import com.google.appengine.datanucleus.test.HasKeyPkJPA;
import com.google.appengine.datanucleus.test.HasLongPkOneToManyBidirChildrenJPA;
import com.google.appengine.datanucleus.test.HasOneToManyChildAtMultipleLevelsJPA;
import com.google.appengine.datanucleus.test.HasOneToManyKeyPkListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyStringPkListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyUnencodedStringPkListJPA;
import com.google.appengine.datanucleus.test.HasOneToManyWithOrderByJPA;
import com.google.appengine.datanucleus.test.HasUnencodedStringPkOneToManyBidirChildrenJPA;


import java.util.Collections;

import static com.google.appengine.datanucleus.TestUtils.assertKeyParentEquals;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAOneToManyListTest extends JPAOneToManyTestCase {

  public void testInsert_NewParentAndChild() throws Exception {
    testInsert_NewParentAndChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                 TXN_START_END);
  }

  public void testInsert_NewParentAndChild_NoTxn() throws Exception {
    testInsert_NewParentAndChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                 NEW_EM_START_END);
  }

  public void testInsert_ExistingParentNewChild() throws Exception {
    testInsert_ExistingParentNewChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      TXN_START_END);
  }

  public void testInsert_ExistingParentNewChild_NoTxn() throws Exception {
    testInsert_ExistingParentNewChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      NEW_EM_START_END);
  }

  public void testUpdate_UpdateChildWithMerge() throws Exception {
    testUpdate_UpdateChildWithMerge(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      TXN_START_END);
  }

  public void testUpdate_UpdateChildWithMerge_NoTxn() throws Exception {
    testUpdate_UpdateChildWithMerge(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      NEW_EM_START_END);
  }

  public void testUpdate_UpdateChild() throws Exception {
    testUpdate_UpdateChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      TXN_START_END);
  }

  public void testUpdate_UpdateChild_NoTxn() throws Exception {
    testUpdate_UpdateChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      NEW_EM_START_END);
  }

  public void testUpdate_NullOutChildren() throws Exception {
    testUpdate_NullOutChildren(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      TXN_START_END);
  }

  public void testUpdate_NullOutChildren_NoTxn() throws Exception {
    testUpdate_NullOutChildren(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      NEW_EM_START_END);
  }

  public void testUpdate_ClearOutChildren() throws Exception {
    testUpdate_ClearOutChildren(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      TXN_START_END);
  }
  public void testUpdate_ClearOutChildren_NoTxn() throws Exception {
    testUpdate_ClearOutChildren(new BidirectionalChildListJPA(), new HasOneToManyListJPA(),
                                      NEW_EM_START_END);
  }
  public void testFindWithOrderBy() throws Exception {
    testFindWithOrderBy(HasOneToManyWithOrderByJPA.class,
                                      TXN_START_END);
  }
  public void testFindWithOrderBy_NoTxn() throws Exception {
    testFindWithOrderBy(HasOneToManyWithOrderByJPA.class,
                                      NEW_EM_START_END);
  }
  public void testFind() throws Exception {
    testFind(HasOneToManyListJPA.class, BidirectionalChildListJPA.class,
                                      TXN_START_END);
  }
  public void testFind_NoTxn() throws Exception {
    testFind(HasOneToManyListJPA.class, BidirectionalChildListJPA.class,
                                      NEW_EM_START_END);
  }
  public void testQuery() throws Exception {
    testQuery(HasOneToManyListJPA.class, BidirectionalChildListJPA.class,
                                      TXN_START_END);
  }
  public void testQuery_NoTxn() throws Exception {
    testQuery(HasOneToManyListJPA.class, BidirectionalChildListJPA.class,
                                      NEW_EM_START_END);
  }
  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyListJPA.class, BidirectionalChildListJPA.class);
  }
  public void testDeleteParentDeletesChild() throws Exception {
    testDeleteParentDeletesChild(HasOneToManyListJPA.class, BidirectionalChildListJPA.class,
                                      TXN_START_END);
  }
  public void testDeleteParentDeletesChild_NoTxn() throws Exception {
    testDeleteParentDeletesChild(HasOneToManyListJPA.class, BidirectionalChildListJPA.class,
                                      NEW_EM_START_END);
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
    BidirectionalChildListJPA bidir1 = new BidirectionalChildListJPA();
    BidirectionalChildListJPA bidir2 = new BidirectionalChildListJPA();
    bidir2.setChildVal("yam");
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir1);
    bidir1.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getBooks().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().set(0, bidir2);

    Book b2 = newBook();
    b2.setTitle("another title");
    pojo.getBooks().set(0, b2);

    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA();
    hasKeyPk2.setStr("another str");
    pojo.getHasKeyPks().set(0, hasKeyPk2);
    startEnd.end();

    startEnd.start();
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getBooks().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(b2.getId());
    assertNotNull(hasKeyPk2.getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(b2.getId())), pojoEntity.getProperty("books"));
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
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b2.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("another title", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b2.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b2.getId());

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
    assertEquals(Collections.singletonList(bookEntity.getKey()), pojoEntity.getProperty("books"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);
  }

  public void testRemoveObject() throws Exception {
    testRemoveObject(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
        new BidirectionalChildListJPA(), TXN_START_END);
  }

  public void testRemoveObject_NoTxn() throws Exception {
    testRemoveObject(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
        new BidirectionalChildListJPA(), NEW_EM_START_END);
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
    BidirectionalChildListJPA bidir1 = new BidirectionalChildListJPA();
    BidirectionalChildListJPA bidir2 = new BidirectionalChildListJPA();
    bidir1.setChildVal("yam1");
    bidir2.setChildVal("yam2");
    Book b1 = newBook();
    Book b2 = newBook();
    b2.setTitle("another title");
    HasKeyPkJPA hasKeyPk1 = new HasKeyPkJPA();
    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA();
    hasKeyPk2.setStr("yar 2");
    pojo.getBooks().add(b1);
    pojo.getBooks().add(b2);
    pojo.getHasKeyPks().add(hasKeyPk1);
    pojo.getHasKeyPks().add(hasKeyPk2);
    pojo.getBidirChildren().add(bidir1);
    pojo.getBidirChildren().add(bidir2);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 2);

    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getBooks().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().remove(0);
    pojo.getBooks().remove(0);
    pojo.getHasKeyPks().remove(0);

    startEnd.start();
    em.merge(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getBooks().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(b2.getId());
    assertNotNull(hasKeyPk2.getId());
    assertEquals(bidir2.getId(), pojo.getBidirChildren().get(0).getId());
    assertEquals(hasKeyPk2.getId(), pojo.getHasKeyPks().get(0).getId());
    assertEquals(b2.getId(), pojo.getBooks().get(0).getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(bidir2.getId())), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(KeyFactory.stringToKey(b2.getId())), pojoEntity.getProperty("books"));
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
    assertEquals("yam2", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b2.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("another title", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b2.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b2.getId());

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
    assertEquals(Collections.singletonList(bookEntity.getKey()), pojoEntity.getProperty("books"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);
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
    BidirectionalChildListJPA bidir1 = new BidirectionalChildListJPA();
    BidirectionalChildListJPA bidir2 = new BidirectionalChildListJPA();
    bidir1.setChildVal("yam1");
    bidir2.setChildVal("yam2");
    Book b1 = newBook();
    Book b2 = newBook();
    b2.setTitle("another title");
    HasKeyPkJPA hasKeyPk1 = new HasKeyPkJPA();
    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA();
    hasKeyPk2.setStr("yar 2");
    pojo.getBooks().add(b1);
    pojo.getHasKeyPks().add(hasKeyPk1);
    pojo.getBidirChildren().add(bidir1);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getBooks().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().add(0, bidir2);
    pojo.getBooks().add(0, b2);
    pojo.getHasKeyPks().add(0, hasKeyPk2);
    startEnd.end();

    startEnd.start();
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getBooks().size());
    assertEquals(2, pojo.getHasKeyPks().size());
    assertEquals(2, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(b2.getId());
    assertNotNull(hasKeyPk2.getId());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(bidir2.getId()), KeyFactory.stringToKey(bidir1Id)), pojoEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(KeyFactory.stringToKey(b2.getId()), KeyFactory.stringToKey(bookId)), pojoEntity.getProperty("books"));
    assertEquals(Utils.newArrayList(hasKeyPk2.getId(), hasKeyPk1Key), pojoEntity.getProperty("hasKeyPks"));

    startEnd.end();

    ds.get(KeyFactory.stringToKey(bidir1Id));
    ds.get(KeyFactory.stringToKey(bookId));
    ds.get(hasKeyPk1Key);

    Entity bidirChildEntity1 = ds.get(KeyFactory.stringToKey(bidir1Id));
    Entity bidirChildEntity2 = ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity2);
    assertEquals("yam2", bidirChildEntity2.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity2.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity2, bidir2.getId());

    Entity bookEntity1 = ds.get(KeyFactory.stringToKey(bookId));
    Entity bookEntity2 = ds.get(KeyFactory.stringToKey(b2.getId()));
    assertNotNull(bookEntity2);
    assertEquals("max", bookEntity2.getProperty("author"));
    assertEquals("22333", bookEntity2.getProperty("isbn"));
    assertEquals("another title", bookEntity2.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b2.getId()), bookEntity2.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity2, b2.getId());

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
    assertEquals(Utils.newArrayList(bookEntity2.getKey(), bookEntity1.getKey()), pojoEntity.getProperty("books"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity2.getKey(), hasKeyPkEntity1.getKey()), pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 2);
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
    BidirectionalChildLongPkListJPA bidirChild = new BidirectionalChildLongPkListJPA();
    bidirChild.setChildVal("yam");

    Book b = newBook();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    HasOneToManyLongPkListJPA parent = new HasOneToManyLongPkListJPA();
    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getBooks().add(b);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    startEnd.start();
    em.persist(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(TestUtils.createKey(parent, parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(bookEntity.getKey()), parentEntity.getProperty("books"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertEquals(HasOneToManyLongPkListJPA.class.getName(), 1, countForClass(HasOneToManyLongPkListJPA.class));
    assertEquals(BidirectionalChildLongPkListJPA.class.getName(), 1, countForClass(
        BidirectionalChildLongPkListJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), 1, countForClass(HasKeyPkJPA.class));
  }

  public void testInsert_NewParentAndChild_StringPk() throws Exception {
    testInsert_NewParentAndChild_StringPk(TXN_START_END);
  }
  public void testInsert_NewParentAndChild_StringPk_NoTxn() throws Exception {
    testInsert_NewParentAndChild_StringPk(NEW_EM_START_END);
  }
  private void testInsert_NewParentAndChild_StringPk(StartEnd startEnd) throws Exception {
    BidirectionalChildStringPkListJPA bidirChild = new BidirectionalChildStringPkListJPA();
    bidirChild.setChildVal("yam");

    Book b = newBook();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    HasOneToManyStringPkListJPA parent = new HasOneToManyStringPkListJPA();
    parent.setId("yar");
    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getBooks().add(b);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    startEnd.start();
    em.persist(parent);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(TestUtils.createKey(parent, parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Utils.newArrayList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Utils.newArrayList(bookEntity.getKey()), parentEntity.getProperty("books"));
    assertEquals(Utils.newArrayList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertEquals(HasOneToManyStringPkListJPA.class.getName(), 1, countForClass(
        HasOneToManyStringPkListJPA.class));
    assertEquals(BidirectionalChildStringPkListJPA.class.getName(), 1, countForClass(
        BidirectionalChildStringPkListJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), 1, countForClass(HasKeyPkJPA.class));
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
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyLongPkListJPA(), new BidirectionalChildLongPkListJPA(), TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithLongPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithLongPk(
        new HasOneToManyLongPkListJPA(), new BidirectionalChildLongPkListJPA(), NEW_EM_START_END);
  }

  public void testAddChildToOneToManyParentWithUnencodedStringPk() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkListJPA(), new BidirectionalChildUnencodedStringPkListJPA(),
        TXN_START_END);
  }
  public void testAddChildToOneToManyParentWithUnencodedStringPk_NoTxn() throws Exception {
    testAddChildToOneToManyParentWithUnencodedStringPk(
        new HasOneToManyUnencodedStringPkListJPA(), new BidirectionalChildUnencodedStringPkListJPA(),
    NEW_EM_START_END);
  }

  public void testOneToManyChildAtMultipleLevels() throws Exception {
    testOneToManyChildAtMultipleLevels(TXN_START_END);
  }
  public void testOneToManyChildAtMultipleLevels_NoTxn() throws Exception {
    testOneToManyChildAtMultipleLevels(NEW_EM_START_END);
  }
  private void testOneToManyChildAtMultipleLevels(StartEnd startEnd)
      throws Exception {
    HasOneToManyChildAtMultipleLevelsJPA pojo = new HasOneToManyChildAtMultipleLevelsJPA();
    Book b1 = new Book();
    pojo.setBooks(Utils.newArrayList(b1));
    HasOneToManyChildAtMultipleLevelsJPA child = new HasOneToManyChildAtMultipleLevelsJPA();
    Book b2 = new Book();
    child.setBooks(Utils.newArrayList(b2));
    pojo.setChild(child);
    startEnd.start();
    em.persist(pojo);
    startEnd.end();
    startEnd.start();
    assertEquals(2, countForClass(Book.class));
    pojo = em.find(HasOneToManyChildAtMultipleLevelsJPA.class, pojo.getId());
    assertEquals(child.getId(), pojo.getChild().getId());
    assertEquals(1, pojo.getBooks().size());
    assertEquals(pojo.getBooks().get(0), b1);
    assertEquals(child.getBooks().get(0), b2);
    assertEquals(1, child.getBooks().size());
    startEnd.end();
  }

  public void testAddQueriedParentToBidirChild() throws Exception {
    testAddQueriedParentToBidirChild(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                     TXN_START_END);
  }
  public void testAddQueriedParentToBidirChild_NoTxn() throws Exception {
    testAddQueriedParentToBidirChild(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                     NEW_EM_START_END);
  }

  public void testAddFetchedParentToBidirChild() throws Exception {
    testAddFetchedParentToBidirChild(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                     TXN_START_END);
  }
  public void testAddFetchedParentToBidirChild_NoTxn() throws Exception {
    testAddFetchedParentToBidirChild(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                     NEW_EM_START_END);
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
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                               TXN_START_END);
  }
  public void testOnlyOneParentPutOnParentAndChildUpdate_NoTxn() throws Throwable {
    testOnlyOneParentPutOnParentAndChildUpdate(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                               NEW_EM_START_END);
  }

  public void testOnlyOnePutOnChildUpdate() throws Throwable {
    testOnlyOnePutOnChildUpdate(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                TXN_START_END);
  }
  public void testOnlyOnePutOnChildUpdate_NoTxn() throws Throwable {
    testOnlyOnePutOnChildUpdate(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                NEW_EM_START_END);
  }

  public void testOnlyOneParentPutOnChildDelete() throws Throwable {
    // 1 put to remove the keys
    int expectedUpdatePuts = 1;
    testOnlyOneParentPutOnChildDelete(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                      TXN_START_END, expectedUpdatePuts);
  }

  public void testOnlyOneParentPutOnChildDelete_NoTxn() throws Throwable {
    // updates are atomic when non-tx, so get 1 after each collection clear and 1 for the update.
    int expectedUpdatePuts = 5;
    testOnlyOneParentPutOnChildDelete(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
                                      NEW_EM_START_END, expectedUpdatePuts);
  }
}
