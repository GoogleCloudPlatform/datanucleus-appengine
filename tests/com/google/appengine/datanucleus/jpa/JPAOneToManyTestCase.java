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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.DatastoreServiceInterceptor;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.BidirectionalChildJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildLongPkJPA;
import com.google.appengine.datanucleus.test.BidirectionalChildUnencodedStringPkJPA;
import com.google.appengine.datanucleus.test.Book;
import com.google.appengine.datanucleus.test.HasKeyPkJPA;
import com.google.appengine.datanucleus.test.HasOneToManyJPA;
import com.google.appengine.datanucleus.test.HasOneToManyKeyPkJPA;
import com.google.appengine.datanucleus.test.HasOneToManyLongPkJPA;
import com.google.appengine.datanucleus.test.HasOneToManyUnencodedStringPkJPA;
import com.google.appengine.datanucleus.test.HasOneToManyWithOrderByJPA;

import org.easymock.EasyMock;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import javax.persistence.PersistenceException;

import static com.google.appengine.datanucleus.TestUtils.assertKeyParentEquals;

/**
 * @author Max Ross <maxr@google.com>
 */
abstract class JPAOneToManyTestCase extends JPATestCase {

  void testInsert_NewParentAndChild(BidirectionalChildJPA bidirChild, HasOneToManyJPA parent,
                                    StartEnd startEnd)
      throws Exception {
    bidirChild.setChildVal("yam");

    Book b = newBook();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

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
    assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(parent.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(bookEntity.getKey()), parentEntity.getProperty("books"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(parent.getClass(), bidirChild.getClass(), 1, 1);
  }

  void testInsert_ExistingParentNewChild(BidirectionalChildJPA bidirChild, HasOneToManyJPA pojo,
                                         StartEnd startEnd) throws Exception {
    pojo.setVal("yar");

    startEnd.start();
    em.persist(pojo);
    startEnd.end();
    assertNotNull(pojo.getId());
    assertTrue(pojo.getBooks().isEmpty());
    assertTrue(pojo.getHasKeyPks().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("books"));
    assertNull(pojoEntity.getProperty("books"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    startEnd.start();
    Book b = newBook();
    pojo.getBooks().add(b);

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");
    pojo.getHasKeyPks().add(hasKeyPk);

    bidirChild.setChildVal("yam");
    pojo.getBidirChildren().add(bidirChild);
    bidirChild.setParent(pojo);

    em.merge(pojo);
    startEnd.end();

    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(4, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(bookEntity.getKey()), parentEntity.getProperty("books"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidirChild.getClass(), 1, 1);
  }

  void testUpdate_UpdateChildWithMerge(BidirectionalChildJPA bidir, HasOneToManyJPA pojo,
      StartEnd startEnd) throws Exception {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    b.setIsbn("yam");
    hasKeyPk.setStr("yar");
    bidir.setChildVal("yap");
    em.merge(pojo);
    startEnd.end();

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_UpdateChild(BidirectionalChildJPA bidir, HasOneToManyJPA pojo,
                              StartEnd startEnd) throws Exception {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    pojo.getBooks().iterator().next().setIsbn("yam");
    pojo.getHasKeyPks().iterator().next().setStr("yar");
    pojo.getBidirChildren().iterator().next().setChildVal("yap");
    startEnd.end();

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_NullOutChildren(BidirectionalChildJPA bidir, HasOneToManyJPA pojo,
                                  StartEnd startEnd) throws Exception {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    startEnd.start();
    pojo.nullBooks();
    pojo.nullHasKeyPks();
    pojo.nullBidirChildren();
    em.merge(pojo);
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(b.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(hasKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("books"));
    assertNull(pojoEntity.getProperty("books"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testUpdate_ClearOutChildren(BidirectionalChildJPA bidir, HasOneToManyJPA pojo,
                                   StartEnd startEnd) throws Exception {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    startEnd.start();
    pojo.getBooks().clear();
    pojo.getHasKeyPks().clear();
    pojo.getBidirChildren().clear();
    em.merge(pojo);
    startEnd.end();

    try {
      ds.get(KeyFactory.stringToKey(b.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(hasKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ds.get(KeyFactory.stringToKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(4, pojoEntity.getProperties().size());
    assertTrue(pojoEntity.hasProperty("bidirChildren"));
    assertNull(pojoEntity.getProperty("bidirChildren"));
    assertTrue(pojoEntity.hasProperty("books"));
    assertNull(pojoEntity.getProperty("books"));
    assertTrue(pojoEntity.hasProperty("hasKeyPks"));
    assertNull(pojoEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testFindWithOrderBy(Class<? extends HasOneToManyWithOrderByJPA> pojoClass,
                           StartEnd startEnd) throws Exception {
    getExecutionContext().getNucleusContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.allowMultipleRelationsOfSameType", true);

    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity bookEntity1 = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22222", "title 1");
    ds.put(bookEntity1);

    Entity bookEntity2 = Book.newBookEntity(pojoEntity.getKey(), "auth2", "22222", "title 2");
    ds.put(bookEntity2);

    Entity bookEntity3 = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22221", "title 0");
    ds.put(bookEntity3);

    startEnd.start();
    HasOneToManyWithOrderByJPA pojo =
        em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getBooksByAuthorAndTitle());
    assertEquals(3, pojo.getBooksByAuthorAndTitle().size());
    assertEquals("title 2", pojo.getBooksByAuthorAndTitle().get(0).getTitle());
    assertEquals("title 0", pojo.getBooksByAuthorAndTitle().get(1).getTitle());
    assertEquals("title 1", pojo.getBooksByAuthorAndTitle().get(2).getTitle());

    assertNotNull(pojo.getBooksByIdAndAuthor());
    assertEquals(3, pojo.getBooksByIdAndAuthor().size());
    assertEquals("title 0", pojo.getBooksByIdAndAuthor().get(0).getTitle());
    assertEquals("title 2", pojo.getBooksByIdAndAuthor().get(1).getTitle());
    assertEquals("title 1", pojo.getBooksByIdAndAuthor().get(2).getTitle());

    assertNotNull(pojo.getBooksByAuthorAndId());
    assertEquals(3, pojo.getBooksByAuthorAndId().size());
    assertEquals("title 2", pojo.getBooksByAuthorAndId().get(0).getTitle());
    assertEquals("title 1", pojo.getBooksByAuthorAndId().get(1).getTitle());
    assertEquals("title 0", pojo.getBooksByAuthorAndId().get(2).getTitle());

    startEnd.end();
  }

  void testFind(Class<? extends HasOneToManyJPA> pojoClass,
                Class<? extends BidirectionalChildJPA> bidirClass,
                StartEnd startEnd) throws Exception {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22222", "title 1");
    ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ds.put(bidirEntity);
    // TODO This test doesn't create the child keys in parent!

    startEnd.start();
    HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getBooks());
    assertEquals(1, pojo.getBooks().size());
    assertEquals("auth1", pojo.getBooks().iterator().next().getAuthor());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().iterator().next().getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    startEnd.end();
  }

  void testQuery(Class<? extends HasOneToManyJPA> pojoClass,
                 Class<? extends BidirectionalChildJPA> bidirClass,
                 StartEnd startEnd) throws Exception {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ds.put(bidirEntity);
    // TODO This test doesn't create the child keys in parent!

    javax.persistence.Query q = em.createQuery(
        "select from " + pojoClass.getName() + " b where id = :key");
    q.setParameter("key", KeyFactory.keyToString(pojoEntity.getKey()));
    startEnd.start();
    @SuppressWarnings("unchecked")
    List<HasOneToManyJPA> result = (List<HasOneToManyJPA>) q.getResultList();
    assertEquals(1, result.size());
    HasOneToManyJPA pojo = result.get(0);
    assertNotNull(pojo.getBooks());
    assertEquals(1, pojo.getBooks().size());
    assertEquals("auth", pojo.getBooks().iterator().next().getAuthor());
    assertEquals(1, pojo.getBooks().size());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().iterator().next().getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().iterator().next().getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().iterator().next().getParent());
    startEnd.end();
  }

  void testChildFetchedLazily(Class<? extends HasOneToManyJPA> pojoClass,
                              Class<? extends BidirectionalChildJPA> bidirClass) throws Exception {
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    // force a new emf to get created after we've installed our own
    // DatastoreService mock
    emf.close();
    tearDown();
    DatastoreService mockDatastore = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService(config);
    DatastoreServiceFactoryInternal.setDatastoreService(mockDatastore);
    try {
      setUp();

      Entity pojoEntity = new Entity(pojoClass.getSimpleName());
      ds.put(pojoEntity);

      Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
      ds.put(bookEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ds.put(hasKeyPkEntity);

      Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
      bidirEntity.setProperty("childVal", "yap");
      ds.put(bidirEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(txn.getId()).andReturn("1").times(2);
      txn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(txn);
      EasyMock.expect(mockDatastore.beginTransaction(EasyMock.isA(TransactionOptions.class))).andReturn(txn);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(mockDatastore.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(mockDatastore);

      beginTxn();
      HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      // need to close the pmf before we restore the original datastore service
      emf.close();
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(mockDatastore);
  }

  void testDeleteParentDeletesChild(Class<? extends HasOneToManyJPA> pojoClass,
                                    Class<? extends BidirectionalChildJPA> bidirClass,
                                    StartEnd startEnd) throws Exception {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ds.put(bidirEntity);

    startEnd.start();
    HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    em.remove(pojo);
    startEnd.end();
    assertCountsInDatastore(pojoClass, bidirClass, 0, 0);
  }

  private static int nextId = 0;

  static String nextNamedKey() {
    return "a" + nextId++;
  }

  public void testRemoveObject(HasOneToManyJPA pojo, BidirectionalChildJPA bidir1,
                               BidirectionalChildJPA bidir2,
                               StartEnd startEnd) throws Exception {
    pojo.setVal("yar");
    bidir1.setChildVal("yam1");
    bidir2.setChildVal("yam2");
    Book b1 = newBook();
    Book b2 = newBook();
    b2.setTitle("another title");
    HasKeyPkJPA hasKeyPk1 = new HasKeyPkJPA(nextNamedKey());
    HasKeyPkJPA hasKeyPk2 = new HasKeyPkJPA(nextNamedKey());
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

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 2);

    String bidir1Id = bidir1.getId();
    String bookId = b1.getId();
    Key hasKeyPk1Key = hasKeyPk1.getId();
    pojo.getBidirChildren().remove(bidir1);
    pojo.getBooks().remove(b1);
    pojo.getHasKeyPks().remove(hasKeyPk1);

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
    assertEquals(bidir2.getId(), pojo.getBidirChildren().iterator().next().getId());
    assertEquals(hasKeyPk2.getId(), pojo.getHasKeyPks().iterator().next().getId());
    assertEquals(b2.getId(), pojo.getBooks().iterator().next().getId());

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
    assertEquals(Collections.singletonList(bidirChildEntity.getKey()), parentEntity.getProperty("bidirChildren"));
    assertEquals(Collections.singletonList(bookEntity.getKey()), parentEntity.getProperty("books"));
    assertEquals(Collections.singletonList(hasKeyPkEntity.getKey()), parentEntity.getProperty("hasKeyPks"));

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 1);
  }

  void testChangeParent(HasOneToManyJPA pojo, HasOneToManyJPA pojo2,
                        StartEnd startEnd) throws Exception {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Book b1 = new Book();
    pojo.getBooks().add(b1);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo2.getBooks().add(b1);
    try {
      em.persist(pojo2);
      startEnd.end();
      fail("expected exception");
    } catch (PersistenceException e) {
      rollbackTxn();
    }
  }

  void testNewParentNewChild_NamedKeyOnChild(HasOneToManyJPA pojo,
                                             StartEnd startEnd) throws Exception {
    Book b1 = new Book("named key");
    pojo.getBooks().add(b1);

    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b1.getId()));
    assertEquals("named key", bookEntity.getKey().getName());
  }

  void testAddAlreadyPersistedChildToParent_NoTxnDifferentEm(HasOneToManyJPA pojo) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Book book = new Book();
    em.persist(book);
    em.close();
    em = emf.createEntityManager();
    pojo.getBooks().add(book);
    try {
      em.persist(pojo);
      em.close();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }

    assertEquals(0, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Book.class));
  }

  void testAddAlreadyPersistedChildToParent_NoTxnSameEm(HasOneToManyJPA pojo) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Book book = new Book();
    em.persist(book);
    em.close();
    em = emf.createEntityManager();
    pojo.getBooks().add(book);
    try {
      em.persist(pojo);
      em.close();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }

    assertEquals(0, countForClass(pojo.getClass()));
    assertEquals(1, countForClass(Book.class));
  }

  void testFetchOfOneToManyParentWithKeyPk(HasOneToManyKeyPkJPA pojo,
                                           StartEnd startEnd) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getBooks().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithLongPk(HasOneToManyLongPkJPA pojo,
                                            StartEnd startEnd) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getBooks().size());
    startEnd.end();
  }

  void testFetchOfOneToManyParentWithUnencodedStringPk(HasOneToManyUnencodedStringPkJPA pojo,
                                                       StartEnd startEnd) throws Exception {
    pojo.setId("yar");
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    assertEquals(0, pojo.getBooks().size());
    startEnd.end();
  }

  void testAddChildToOneToManyParentWithLongPk(
      HasOneToManyLongPkJPA pojo, BidirectionalChildLongPkJPA bidirChild,
      StartEnd startEnd) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    Book b = new Book();
    pojo.getBooks().add(b);
    pojo.getBidirChildren().add(bidirChild);
    bidirChild.setParent(pojo);
    startEnd.end();

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals(pojoEntity.getKey(), bookEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddChildToOneToManyParentWithUnencodedStringPk(
      HasOneToManyUnencodedStringPkJPA pojo, BidirectionalChildUnencodedStringPkJPA bidirChild,
      StartEnd startEnd) throws Exception {
    pojo.setId("yar");
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    Book b = new Book();
    pojo.getBooks().add(b);
    pojo.getBidirChildren().add(bidirChild);
    startEnd.end();

    Entity bookEntity = ds.get(KeyFactory.stringToKey(b.getId()));
    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    Entity pojoEntity = ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertEquals(pojoEntity.getKey(), bookEntity.getParent());
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddQueriedParentToBidirChild(HasOneToManyJPA pojo, BidirectionalChildJPA bidir,
                                        StartEnd startEnd) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = (HasOneToManyJPA) em.createQuery("select from " + pojo.getClass().getName() + " b").getSingleResult();
    bidir.setParent(pojo);
    em.persist(bidir);
    startEnd.end();

    startEnd.start();
    pojo = (HasOneToManyJPA) em.createQuery("select from " + pojo.getClass().getName() + " b").getSingleResult();
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  void testAddFetchedParentToBidirChild(HasOneToManyJPA pojo, BidirectionalChildJPA bidir,
                                        StartEnd startEnd) throws Exception {
    startEnd.start();
    em.persist(pojo);
    startEnd.end();

    startEnd.start();
    pojo = em.find(pojo.getClass(), pojo.getId());
    bidir.setParent(pojo);
    em.persist(bidir);
    startEnd.end();

    startEnd.start();
    pojo = (HasOneToManyJPA) em.createQuery("select from " + pojo.getClass().getName() + " b").getSingleResult();
    assertEquals(1, pojo.getBidirChildren().size());
    startEnd.end();

    Entity bidirEntity = ds.get(KeyFactory.stringToKey(bidir.getId()));
    Entity pojoEntity = ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(pojoEntity.getKey(), bidirEntity.getParent());
  }

  private static final class PutPolicy implements DatastoreServiceInterceptor.Policy {
    private final List<Object[]> putParamList = Utils.newArrayList();
    public void intercept(Object o, Method method, Object[] params) {
      if (method.getName().equals("put")) {
        putParamList.add(params);
      }
    }
  }

  PutPolicy setupPutPolicy(HasOneToManyJPA pojo, BidirectionalChildJPA bidir,
                           StartEnd startEnd) throws Throwable {
    PutPolicy policy = new PutPolicy();
    DatastoreServiceInterceptor.install(getStoreManager(), policy);
    try {
      emf.close();
      switchDatasource(getEntityManagerFactoryName());
      Book book = new Book();
      pojo.getBooks().add(book);
      pojo.getBidirChildren().add(bidir);
      HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
      pojo.getHasKeyPks().add(hasKeyPk);

      startEnd.start();
      em.persist(pojo);
      startEnd.end();
      // 1 put for the parent, 3 puts for the children, 1 more put
      // to add the child keys back on the parent
      assertEquals(5, policy.putParamList.size());
      policy.putParamList.clear();
      return policy;
    } catch (Throwable t) {
      DatastoreServiceInterceptor.uninstall();
      throw t;
    }
  }

  void testOnlyOnePutOnChildUpdate(HasOneToManyJPA pojo, BidirectionalChildJPA bidir,
                                   StartEnd startEnd) throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.getBooks().iterator().next().setTitle("some author");
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      pojo.getHasKeyPks().iterator().next().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for each child update
    assertEquals(3, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnParentAndChildUpdate(HasOneToManyJPA pojo, BidirectionalChildJPA bidir,
                                                  StartEnd startEnd) throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.getBooks().iterator().next().setTitle("some author");
      pojo.getBidirChildren().iterator().next().setChildVal("blarg");
      pojo.getHasKeyPks().iterator().next().setStr("double blarg");
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    // 1 put for the parent update, 1 put for each child update
    assertEquals(4, policy.putParamList.size());
  }

  void testOnlyOneParentPutOnChildDelete(HasOneToManyJPA pojo, BidirectionalChildJPA bidir,
                                         StartEnd startEnd,
                                         int expectedUpdatePuts) throws Throwable {
    PutPolicy policy = setupPutPolicy(pojo, bidir, startEnd);
    try {
      startEnd.start();
      pojo = em.find(pojo.getClass(), pojo.getId());
      pojo.setVal("another val");
      pojo.getBooks().clear();
      pojo.getBidirChildren().clear();
      pojo.getHasKeyPks().clear();
      startEnd.end();
    } finally {
      DatastoreServiceInterceptor.uninstall();
    }
    assertEquals(expectedUpdatePuts, policy.putParamList.size());
  }

  void assertCountsInDatastore(Class<? extends HasOneToManyJPA> pojoClass,
                               Class<? extends BidirectionalChildJPA> bidirClass,
                               int expectedParent, int expectedChildren) {
    assertEquals(pojoClass.getName(), expectedParent, countForClass(pojoClass));
    assertEquals(bidirClass.getName(), expectedChildren, countForClass(bidirClass));
    assertEquals(Book.class.getName(), expectedChildren, countForClass(Book.class));
    assertEquals(HasKeyPkJPA.class.getName(), expectedChildren, countForClass(HasKeyPkJPA.class));
  }

  Book newBook() {
    Book b = new Book(nextNamedKey());
    b.setAuthor("max");
    b.setIsbn("22333");
    b.setTitle("yam");
    return b;
  }
}
