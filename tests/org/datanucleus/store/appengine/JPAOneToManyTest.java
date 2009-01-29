// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.BidirectionalChildJPA;
import org.datanucleus.test.Book;
import org.datanucleus.test.HasKeyPkJPA;
import org.datanucleus.test.HasOneToManyJPA;
import org.datanucleus.test.HasOneToManyWithNonDeletingCascadeJPA;
import org.datanucleus.test.HasOneToManyWithOrderByJPA;
import org.easymock.EasyMock;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAOneToManyTest extends JPATestCase {

  public void testInsert_NewParentAndChild() throws EntityNotFoundException {
    BidirectionalChildJPA bidirChild = new BidirectionalChildJPA();
    bidirChild.setChildVal("yam");

    Book b = newBook();

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");

    HasOneToManyJPA parent = new HasOneToManyJPA();
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

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.decodeKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.decodeKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(parent.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.decodeKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(parent.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(parent.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(parent.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(1, 1);
  }

  public void testInsert_ExistingParentNewChild() throws EntityNotFoundException {
    HasOneToManyJPA pojo = new HasOneToManyJPA();
    pojo.setVal("yar");

    beginTxn();
    em.persist(pojo);
    commitTxn();
    assertNotNull(pojo.getId());
    assertTrue(pojo.getBooks().isEmpty());
    assertTrue(pojo.getHasKeyPks().isEmpty());
    assertTrue(pojo.getBidirChildren().isEmpty());

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(pojoEntity);
    assertEquals(1, pojoEntity.getProperties().size());

    beginTxn();
    Book b = newBook();
    pojo.getBooks().add(b);

    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    hasKeyPk.setStr("yag");
    pojo.getHasKeyPks().add(hasKeyPk);

    BidirectionalChildJPA bidirChild = new BidirectionalChildJPA();
    bidirChild.setChildVal("yam");
    pojo.getBidirChildren().add(bidirChild);

    em.merge(pojo);
    commitTxn();

    assertNotNull(bidirChild.getId());
    assertNotNull(bidirChild.getParent());
    assertNotNull(b.getId());
    assertNotNull(hasKeyPk.getId());

    Entity bidirChildEntity = ldth.ds.get(KeyFactory.decodeKey(bidirChild.getId()));
    assertNotNull(bidirChildEntity);
    assertEquals("yam", bidirChildEntity.getProperty("childVal"));
    assertEquals(KeyFactory.decodeKey(bidirChild.getId()), bidirChildEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bidirChildEntity, bidirChild.getId());

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("max", bookEntity.getProperty("author"));
    assertEquals("22333", bookEntity.getProperty("isbn"));
    assertEquals("yam", bookEntity.getProperty("title"));
    assertEquals(KeyFactory.decodeKey(b.getId()), bookEntity.getKey());
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yag", hasKeyPkEntity.getProperty("str"));
    assertEquals(hasKeyPk.getId(), hasKeyPkEntity.getKey());
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertNotNull(parentEntity);
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("yar", parentEntity.getProperty("val"));

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChildWithMerge() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    BidirectionalChildJPA bidir = new BidirectionalChildJPA();

    HasOneToManyJPA pojo = new HasOneToManyJPA();
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

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ldth.ds.get(KeyFactory.decodeKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_UpdateChild() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    BidirectionalChildJPA bidir = new BidirectionalChildJPA();

    HasOneToManyJPA pojo = new HasOneToManyJPA();
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
    pojo = em.find(HasOneToManyJPA.class, pojo.getId());
    pojo.getBooks().get(0).setIsbn("yam");
    pojo.getHasKeyPks().get(0).setStr("yar");
    pojo.getBidirChildren().get(0).setChildVal("yap");
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals("yam", bookEntity.getProperty("isbn"));
    assertKeyParentEquals(pojo.getId(), bookEntity, b.getId());

    Entity hasKeyPkEntity = ldth.ds.get(hasKeyPk.getId());
    assertNotNull(hasKeyPkEntity);
    assertEquals("yar", hasKeyPkEntity.getProperty("str"));
    assertKeyParentEquals(pojo.getId(), hasKeyPkEntity, hasKeyPk.getId());

    Entity bidirEntity = ldth.ds.get(KeyFactory.decodeKey(bidir.getId()));
    assertNotNull(bidirEntity);
    assertEquals("yap", bidirEntity.getProperty("childVal"));
    assertKeyParentEquals(pojo.getId(), bidirEntity, bidir.getId());

    assertCountsInDatastore(1, 1);
  }

  public void testUpdate_NullOutChildren() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    BidirectionalChildJPA bidir = new BidirectionalChildJPA();

    HasOneToManyJPA pojo = new HasOneToManyJPA();
    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(1, 1);

    beginTxn();
    pojo.setBooks(null);
    pojo.setHasKeyPks(null);
    pojo.setBidirChildren(null);
    em.merge(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.decodeKey(b.getId()));
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
      ldth.ds.get(KeyFactory.decodeKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(1, 0);
  }

  public void testUpdate_ClearOutChildren() throws EntityNotFoundException {
    Book b = newBook();
    HasKeyPkJPA hasKeyPk = new HasKeyPkJPA();
    BidirectionalChildJPA bidir = new BidirectionalChildJPA();

    HasOneToManyJPA pojo = new HasOneToManyJPA();
    pojo.getBooks().add(b);
    pojo.getHasKeyPks().add(hasKeyPk);
    pojo.getBidirChildren().add(bidir);
    bidir.setParent(pojo);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertCountsInDatastore(1, 1);

    beginTxn();
    pojo.getBooks().clear();
    pojo.getHasKeyPks().clear();
    pojo.getBidirChildren().clear();
    em.merge(pojo);
    commitTxn();

    try {
      ldth.ds.get(KeyFactory.decodeKey(b.getId()));
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
      ldth.ds.get(KeyFactory.decodeKey(bidir.getId()));
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }

    Entity pojoEntity = ldth.ds.get(KeyFactory.decodeKey(pojo.getId()));
    assertEquals(1, pojoEntity.getProperties().size());

    assertCountsInDatastore(1, 0);
  }

  public void testUpdate_NullOutChild_NoDelete() throws EntityNotFoundException {
    Book b = newBook();
    beginTxn();
    em.persist(b);
    commitTxn();
    HasOneToManyWithNonDeletingCascadeJPA pojo = new HasOneToManyWithNonDeletingCascadeJPA();
    pojo.getBooks().add(b);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertEquals(HasOneToManyWithNonDeletingCascadeJPA.class.getName(), 1,
                 countForClass(HasOneToManyWithNonDeletingCascadeJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));

    beginTxn();
    pojo.setBooks(null);
    em.merge(pojo);
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);

    assertEquals(HasOneToManyWithNonDeletingCascadeJPA.class.getName(), 1,
                 countForClass(HasOneToManyWithNonDeletingCascadeJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
  }

  public void testUpdate_ClearOutChild_NoDelete() throws EntityNotFoundException {
    Book b = newBook();
    beginTxn();
    em.persist(b);
    commitTxn();
    HasOneToManyWithNonDeletingCascadeJPA pojo = new HasOneToManyWithNonDeletingCascadeJPA();
    pojo.getBooks().add(b);

    beginTxn();
    em.persist(pojo);
    commitTxn();

    assertEquals(HasOneToManyWithNonDeletingCascadeJPA.class.getName(), 1,
                 countForClass(HasOneToManyWithNonDeletingCascadeJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));

    beginTxn();
    pojo.getBooks().clear();
    em.merge(pojo);
    commitTxn();

    Entity bookEntity = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertNotNull(bookEntity);
    assertEquals(HasOneToManyWithNonDeletingCascadeJPA.class.getName(), 1,
                 countForClass(HasOneToManyWithNonDeletingCascadeJPA.class));
    assertEquals(Book.class.getName(), 1, countForClass(Book.class));
  }

  public void testFindWithOrderBy() throws EntityNotFoundException {
    Entity pojoEntity = new Entity(HasOneToManyWithOrderByJPA.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity1 = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22222", "title 1");
    ldth.ds.put(bookEntity1);

    Entity bookEntity2 = Book.newBookEntity(pojoEntity.getKey(), "auth2", "22222", "title 2");
    ldth.ds.put(bookEntity2);

    Entity bookEntity3 = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22221", "title 0");
    ldth.ds.put(bookEntity3);

    beginTxn();
    HasOneToManyWithOrderByJPA pojo = em.find(HasOneToManyWithOrderByJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
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

  public void testFind() throws EntityNotFoundException {
    Entity pojoEntity = new Entity(HasOneToManyJPA.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth1", "22222", "title 1");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(BidirectionalChildJPA.class.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ldth.ds.put(bidirEntity);

    beginTxn();
    HasOneToManyJPA pojo = em.find(HasOneToManyJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    assertNotNull(pojo);
    assertNotNull(pojo.getBooks());
    assertEquals(1, pojo.getBooks().size());
    assertEquals("auth1", pojo.getBooks().get(0).getAuthor());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().get(0).getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals("yap", pojo.getBidirChildren().get(0).getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().get(0).getParent());
    commitTxn();
  }

  public void testQuery() {
    Entity pojoEntity = new Entity(HasOneToManyJPA.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(BidirectionalChildJPA.class.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ldth.ds.put(bidirEntity);

    javax.persistence.Query q = em.createQuery(
        "select from " + HasOneToManyJPA.class.getName() + " where id = :key");
    q.setParameter("key", pojoEntity.getKey());
    beginTxn();
    @SuppressWarnings("unchecked")
    List<HasOneToManyJPA> result = (List<HasOneToManyJPA>) q.getResultList();
    assertEquals(1, result.size());
    HasOneToManyJPA pojo = result.get(0);
    assertNotNull(pojo.getBooks());
    assertEquals(1, pojo.getBooks().size());
    assertEquals("auth", pojo.getBooks().get(0).getAuthor());
    assertEquals(1, pojo.getBooks().size());
    assertNotNull(pojo.getHasKeyPks());
    assertEquals(1, pojo.getHasKeyPks().size());
    assertEquals("yar", pojo.getHasKeyPks().get(0).getStr());
    assertNotNull(pojo.getBidirChildren());
    assertEquals(1, pojo.getBidirChildren().size());
    assertEquals("yap", pojo.getBidirChildren().get(0).getChildVal());
    assertEquals(pojo, pojo.getBidirChildren().get(0).getParent());
    commitTxn();
  }

  public void testChildFetchedLazily() throws Exception {
    tearDown();
    DatastoreService ds = EasyMock.createMock(DatastoreService.class);
    DatastoreService original = DatastoreServiceFactoryInternal.getDatastoreService();
    DatastoreServiceFactoryInternal.setDatastoreService(ds);
    try {
      setUp();

      Entity pojoEntity = new Entity(HasOneToManyJPA.class.getSimpleName());
      ldth.ds.put(pojoEntity);

      Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
      ldth.ds.put(bookEntity);

      Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
      hasKeyPkEntity.setProperty("str", "yar");
      ldth.ds.put(hasKeyPkEntity);

      Entity bidirEntity = new Entity(BidirectionalChildJPA.class.getSimpleName(), pojoEntity.getKey());
      bidirEntity.setProperty("childVal", "yap");
      ldth.ds.put(bidirEntity);

      Transaction txn = EasyMock.createMock(Transaction.class);
      EasyMock.expect(ds.beginTransaction()).andReturn(txn);
      // the only get we're going to perform is for the pojo
      EasyMock.expect(ds.get(txn, pojoEntity.getKey())).andReturn(pojoEntity);
      EasyMock.replay(ds);

      beginTxn();
      HasOneToManyJPA pojo = em.find(HasOneToManyJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
      assertNotNull(pojo);
      pojo.getId();
      commitTxn();
    } finally {
      DatastoreServiceFactoryInternal.setDatastoreService(original);
    }
    EasyMock.verify(ds);
  }

  public void testDeleteParentDeletesChild() {
    Entity pojoEntity = new Entity(HasOneToManyJPA.class.getSimpleName());
    ldth.ds.put(pojoEntity);

    Entity bookEntity = Book.newBookEntity(pojoEntity.getKey(), "auth", "22222", "the title");
    ldth.ds.put(bookEntity);

    Entity hasKeyPkEntity = new Entity(HasKeyPkJPA.class.getSimpleName(), pojoEntity.getKey());
    hasKeyPkEntity.setProperty("str", "yar");
    ldth.ds.put(hasKeyPkEntity);

    Entity bidirEntity = new Entity(BidirectionalChildJPA.class.getSimpleName(), pojoEntity.getKey());
    bidirEntity.setProperty("childVal", "yap");
    ldth.ds.put(bidirEntity);

    beginTxn();
    HasOneToManyJPA pojo = em.find(HasOneToManyJPA.class, KeyFactory.encodeKey(pojoEntity.getKey()));
    em.remove(pojo);
    commitTxn();
    assertCountsInDatastore(0, 0);
  }

  private int countForClass(Class<?> clazz) {
    return ldth.ds.prepare(new Query(clazz.getSimpleName())).countEntities();
  }

  private void assertCountsInDatastore(int expectedParent, int expectedChildren) {
    assertEquals(
        HasOneToManyJPA.class.getName(), expectedParent, countForClass(HasOneToManyJPA.class));
    assertEquals(
        BidirectionalChildJPA.class.getName(), expectedChildren,
        countForClass(BidirectionalChildJPA.class));
    assertEquals(
        Book.class.getName(), expectedChildren, countForClass(Book.class));
    assertEquals(
        HasKeyPkJPA.class.getName(), expectedChildren, countForClass(HasKeyPkJPA.class));
  }

  private Book newBook() {
    Book b = new Book();
    b.setAuthor("max");
    b.setIsbn("22333");
    b.setTitle("yam");
    return b;
  }

}
