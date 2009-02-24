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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.BidirectionalChildListJPA;
import org.datanucleus.test.BidirectionalChildListLongPkJPA;
import org.datanucleus.test.BidirectionalChildListStringPkJPA;
import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasOneToManyListJPA;
import org.datanucleus.test.HasOneToManyListLongPkJPA;
import org.datanucleus.test.HasOneToManyListStringPkJPA;
import org.datanucleus.test.HasOneToManyWithOrderByJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAOneToManyListTest extends JPAOneToManyTestCase {

  public void testInsert_NewParentAndChild() throws EntityNotFoundException {
    testInsert_NewParentAndChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA());
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    testInsert_ExistingParentNewChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA());
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    testUpdate_UpdateChildWithMerge(new BidirectionalChildListJPA(), new HasOneToManyListJPA());
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    testUpdate_UpdateChild(new BidirectionalChildListJPA(), new HasOneToManyListJPA());
  }

  public void testUpdate_NullOutChildren() throws EntityNotFoundException {
    testUpdate_NullOutChildren(new BidirectionalChildListJPA(), new HasOneToManyListJPA());
  }
  public void testUpdate_ClearOutChildren() throws EntityNotFoundException {
    testUpdate_ClearOutChildren(new BidirectionalChildListJPA(), new HasOneToManyListJPA());
  }
  public void testFindWithOrderBy() throws EntityNotFoundException {
    testFindWithOrderBy(HasOneToManyWithOrderByJPA.class);
  }
  public void testFind() throws EntityNotFoundException {
    testFind(HasOneToManyListJPA.class, BidirectionalChildListJPA.class);
  }
  public void testQuery() throws EntityNotFoundException {
    testQuery(HasOneToManyListJPA.class, BidirectionalChildListJPA.class);
  }
  public void testChildFetchedLazily() throws Exception {
    testChildFetchedLazily(HasOneToManyListJPA.class, BidirectionalChildListJPA.class);
  }
  public void testDeleteParentDeletesChild() throws EntityNotFoundException {
    testDeleteParentDeletesChild(HasOneToManyListJPA.class, BidirectionalChildListJPA.class);
  }

  public void testSwapAtPosition() throws EntityNotFoundException {
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

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);

    beginTxn();
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
    commitTxn();

    beginTxn();
    assertNotNull(pojo.getId());
    assertEquals(1, pojo.getBooks().size());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals(1, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(b2.getId());
    assertNotNull(hasKeyPk2.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(bookId));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b2.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("another title", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b2.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b2.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk2.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("another str", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getId());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);
  }

  public void testRemoveObject() throws EntityNotFoundException {
    testRemoveObject(new HasOneToManyListJPA(), new BidirectionalChildListJPA(),
        new BidirectionalChildListJPA());
  }

  public void testRemoveAtPosition() throws EntityNotFoundException {
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

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 2);

    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getBooks().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().remove(0);
    pojo.getBooks().remove(0);
    pojo.getHasKeyPks().remove(0);

    beginTxn();
    em.merge(pojo);
    commitTxn();

    beginTxn();
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

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir1Id));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(KeyFactory.stringToKey(bookId));
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    try {
      ldth.ds.get(hasKeyPk1Key);
      fail("expected EntityNotFoundException");
    } catch (EntityNotFoundException enfe) {
      // good
    }
    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam2", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b2.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("another title", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b2.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b2.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk2.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar 2", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getId());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);
  }

  public void testAddAtPosition() throws EntityNotFoundException {
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

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 1);

    beginTxn();
    pojo = em.find(pojo.getClass(), pojo.getId());
    String bidir1Id = pojo.getBidirChildren().get(0).getId();
    String bookId = pojo.getBooks().get(0).getId();
    Key hasKeyPk1Key = pojo.getHasKeyPks().get(0).getId();
    pojo.getBidirChildren().add(0, bidir2);
    pojo.getBooks().add(0, b2);
    pojo.getHasKeyPks().add(0, hasKeyPk2);
    commitTxn();

    beginTxn();
    assertNotNull(pojo.getId());
    assertEquals(2, pojo.getBooks().size());
    assertEquals(2, pojo.getHasKeyPks().size());
    assertEquals(2, pojo.getBidirChildren().size());
    assertNotNull(bidir2.getId());
    assertNotNull(bidir2.getParent());
    assertNotNull(b2.getId());
    assertNotNull(hasKeyPk2.getId());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    commitTxn();

    ldth.ds.get(KeyFactory.stringToKey(bidir1Id));
    ldth.ds.get(KeyFactory.stringToKey(bookId));
    ldth.ds.get(hasKeyPk1Key);

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidir2.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam2", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidir2.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidir2.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b2.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("another title", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b2.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b2.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk2.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar 2", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk2.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk2.getId());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(HasOneToManyListJPA.class, BidirectionalChildListJPA.class, 1, 2);
  }

  public void testChangeParent() {
    testChangeParent(new HasOneToManyListJPA(), new HasOneToManyListJPA());
  }
  public void testNewParentNewChild_NamedKeyOnChild() throws EntityNotFoundException {
    testNewParentNewChild_NamedKeyOnChild(new HasOneToManyListJPA());
  }

  public void testInsert_NewParentAndChild_LongPk() throws EntityNotFoundException {
    BidirectionalChildListLongPkJPA bidirChild = new BidirectionalChildListLongPkJPA();
    bidirChild.setChildVal("yam");

    Book b = newBook();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    HasOneToManyListLongPkJPA parent = new HasOneToManyListLongPkJPA();
    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getBooks().add(b);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    beginTxn();
    em.persist(parent);
    commitTxn();

    assertNotNull(bidirChild.getId());
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ldth.ds.get(TestUtils.createKey(parent, parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertEquals(HasOneToManyListLongPkJPA.class.getName(), 1, countForClass(HasOneToManyListLongPkJPA.class));
    assertEquals(BidirectionalChildListLongPkJPA.class.getName(), 1, countForClass(BidirectionalChildListLongPkJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), 1, countForClass(HasKeyPkJPA.class));
  }

  public void testInsert_NewParentAndChild_StringPk() throws EntityNotFoundException {
    BidirectionalChildListStringPkJPA bidirChild = new BidirectionalChildListStringPkJPA();
    bidirChild.setChildVal("yam");

    Book b = newBook();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    HasOneToManyListStringPkJPA parent = new HasOneToManyListStringPkJPA();
    parent.setId("yar");
    parent.getBidirChildren().add(bidirChild);
    bidirChild.setParent(parent);
    parent.getBooks().add(b);
    parent.getHasKeyPks().add(hasKeyPk);
    parent.setVal("yar");

    beginTxn();
    em.persist(parent);
    commitTxn();

    assertNotNull(bidirChild.getId());
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getClass(), parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ldth.ds.get(TestUtils.createKey(parent, parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertEquals(HasOneToManyListStringPkJPA.class.getName(), 1, countForClass(HasOneToManyListStringPkJPA.class));
    assertEquals(BidirectionalChildListStringPkJPA.class.getName(), 1, countForClass(BidirectionalChildListStringPkJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), 1, countForClass(HasKeyPkJPA.class));
  }
}
