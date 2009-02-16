// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.BidirectionalChildJPA;
import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasOneToManyJPA;
import org.datanucleus.test.HasOneToManyWithOrderByJPA;
import org.easymock.EasyMock;

import java.util.List;

import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
abstract class JPAOneToManyTest extends JPATestCase {

  void testInsert_NewParentAndChild(BidirectionalChildJPA bidirChild, HasOneToManyJPA parent)
      throws EntityNotFoundException {
    bidirChild.setChildVal("yam");

    Book b = newBook();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

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
    assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(parent.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(parent.getClass(), bidirChild.getClass(), 1, 1);
  }

  void testInsert_ExistingParentNewChild(BidirectionalChildJPA bidirChild, HasOneToManyJPA pojo)
      throws EntityNotFoundException {
    pojo.setVal("yar");

    beginTxn();
    em.persist(pojo);
    commitTxn();
    assertNotNull(pojo.getId());
    assertTrue(pojo.getBooks().isEmpty());
    assertTrue(pojo.getHasKeyPks().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    beginTxn();
    Book b = newBook();
    pojo.getBooks().add(b);

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");
    pojo.getHasKeyPks().add(hasKeyPk);

    bidirChild.setChildVal("yam");
    pojo.getBidirChildren().add(bidirChild);

    em.merge(pojo);
    commitTxn();

    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.stringToKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.stringToKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.stringToKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(pojo.getClass(), bidirChild.getClass(), 1, 1);
  }

  void testUpdate_UpdateChildWithMerge(BidirectionalChildJPA bidir, HasOneToManyJPA pojo)
      throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    beginTxn();
    b.setIsbn("yam");
    hasKeyPk.setStr("yar");
    bidir.setChildVal("yap");
    em.merge(pojo);
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_UpdateChild(BidirectionalChildJPA bidir, HasOneToManyJPA pojo)
      throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());
    assertNotNull(bidir.getId());
    assertNotNull(pojo.getId());

    beginTxn();
    pojo = em.find(pojo.getClass(), pojo.getId());
    pojo.getBooks().iterator().next().setIsbn("yam");
    pojo.getHasKeyPks().iterator().next().setStr("yar");
    pojo.getBidirChildren().iterator().next().setChildVal("yap");
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);
  }

  void testUpdate_NullOutChildren(BidirectionalChildJPA bidir, HasOneToManyJPA pojo)
      throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    beginTxn();
    pojo.nullBooks();
    pojo.nullHasKeyPks();
    pojo.nullBidirChildren();
    em.merge(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(b.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testUpdate_ClearOutChildren(BidirectionalChildJPA bidir, HasOneToManyJPA pojo)
      throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();

    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 1);

    beginTxn();
    pojo.getBooks().clear();
    pojo.getHasKeyPks().clear();
    pojo.getBidirChildren().clear();
    em.merge(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.stringToKey(b.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(hasKeyPk.getId());
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    try {
      ldth.ds.get(KeyFactory.stringToKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(pojo.getClass(), bidir.getClass(), 1, 0);
  }

  void testFindWithOrderBy(Class<? extends HasOneToManyWithOrderByJPA> pojoClass)
      throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity1 = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22222", "title 1");
    ldth.ds.put(bookEntity1);

    Entity bookEntity2 = Book.newBookEntity(pojoEntity.getKey(), "auth2", "22222", "title 2");
    ldth.ds.put(bookEntity2);

    Entity bookEntity3 = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22221", "title 0");
    ldth.ds.put(bookEntity3);

    beginTxn();
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

    commitTxn();
  }

  void testFind(Class<? extends HasOneToManyJPA> pojoClass,
                Class<? extends BidirectionalChildJPA> bidirClass) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22222", "title 1");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ldth.ds.put(bidirEntity);

    beginTxn();
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
    commitTxn();
  }

  void testQuery(Class<? extends HasOneToManyJPA> pojoClass,
                 Class<? extends BidirectionalChildJPA> bidirClass) throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ldth.ds.put(bidirEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + pojoClass.getName() + " where id = :key");
    q.setParameter("key", pojoEntity.getKey());
    beginTxn();
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
    commitTxn();
  }

  void testChildFetchedLazily(Class<? extends HasOneToManyJPA> pojoClass,
                              Class<? extends BidirectionalChildJPA> bidirClass) throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    try {
      setUp();

      Entity pojoEntity = new Entity(pojoClass.getSimpleName());
      ldth.ds.put(pojoEntity);

      Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
      ldth.ds.put(bookEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
      bidirEntity.setProperty("childVal", "yap");
      ldth.ds.put(bidirEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(txn.getId()).andReturn("1").times(2);
      txn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(txn);
      EasyMock.expect(ds.beginTransaction()).andReturn(txn);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(ds.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      beginTxn();
      HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  void testDeleteParentDeletesChild(Class<? extends HasOneToManyJPA> pojoClass,
                                    Class<? extends BidirectionalChildJPA> bidirClass)
      throws EntityNotFoundException {
    Entity pojoEntity = new Entity(pojoClass.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(bidirClass.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ldth.ds.put(bidirEntity);

    beginTxn();
    HasOneToManyJPA pojo = em.find(pojoClass, KeyFactory.keyToString(pojoEntity.getKey()));
    em.remove(pojo);
    commitTxn();
    assertCountsInDatastore(pojoClass, bidirClass, 0, 0);
  }

  private static int nextId = 0;

  static String nextNamedKey() {
    return "a" + nextId++;
  }

  public void testRemoveObject(HasOneToManyJPA pojo, BidirectionalChildJPA bidir1,
                               BidirectionalChildJPA bidir2) throws EntityNotFoundException {
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

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 2);

    String bidir1Id = bidir1.getId();
    String bookId = b1.getId();
    Key hasKeyPk1Key = hasKeyPk1.getId();
    pojo.getBidirChildren().remove(bidir1);
    pojo.getBooks().remove(b1);
    pojo.getHasKeyPks().remove(hasKeyPk1);

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
    assertEquals(bidir2.getId(), pojo.getBidirChildren().iterator().next().getId());
    assertEquals(hasKeyPk2.getId(), pojo.getHasKeyPks().iterator().next().getId());
    assertEquals(b2.getId(), pojo.getBooks().iterator().next().getId());

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

    assertCountsInDatastore(pojo.getClass(), bidir1.getClass(), 1, 1);
  }

  void testChangeParent(HasOneToManyJPA pojo, HasOneToManyJPA pojo2) {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Book b1 = new Book();
    pojo.getBooks().add(b1);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    beginTxn();
    pojo2.getBooks().add(b1);
    em.persist(pojo2);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      rollbackTxn();
    }
  }

  void testNewParentNewChild_NamedKeyOnChild(HasOneToManyJPA pojo) throws EntityNotFoundException {
    Book b1 = new Book();
    b1.setId("named key");
    pojo.getBooks().add(b1);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.stringToKey(b1.getId()));
    assertEquals("named key", bookEntity.getKey().getName());
  }

  int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(new Query(clazz.getSimpleName())).countEntities();
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
