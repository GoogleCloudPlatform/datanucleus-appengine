// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasVersionJPA;

import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JPAUpdateTest extends JPATestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "VERSION";

  public void testUpdateAfterFetch() throws EntityNotFoundException {
    Key key = ldth.ds.put(Book.newBookEntity("jimmy", "12345", "the title"));

    String keyStr = KeyFactory.encodeKey(key);
    beginTxn();
    Book book = em.find(Book.class, keyStr);

    assertEquals(keyStr, book.getId());
    assertEquals("jimmy", book.getAuthor());
    assertEquals("12345", book.getIsbn());
    assertEquals("the title", book.getTitle());

    book.setIsbn("56789");
    commitTxn();

    Entity bookCheck = ldth.ds.get(key);
    assertEquals("jimmy", bookCheck.getProperty("author"));
    assertEquals("56789", bookCheck.getProperty("isbn"));
    assertEquals("the title", bookCheck.getProperty("title"));
  }

  public void testUpdateAfterSave() throws EntityNotFoundException {
    Book b = new Book();
    b.setAuthor("max");
    b.setIsbn("22333");
    b.setTitle("yam");

    beginTxn();
    em.persist(b);
    commitTxn();

    assertNotNull(b.getId());

    beginTxn();
    b.setTitle("not yam");
    em.merge(b);
    commitTxn();

    Entity bookCheck = ldth.ds.get(KeyFactory.decodeKey(b.getId()));
    assertEquals("max", bookCheck.getProperty("author"));
    assertEquals("22333", bookCheck.getProperty("isbn"));
    assertEquals("not yam", bookCheck.getProperty("title"));
  }

  public void testOptimisticLocking_Update() {
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.encodeKey(key);
    beginTxn();
    HasVersionJPA hv = em.find(HasVersionJPA.class, keyStr);
    hv.setValue("value");
    commitTxn();
    assertEquals(2L, hv.getVersion());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    beginTxn();
    hv = em.find(HasVersionJPA.class, keyStr);
    hv.setValue("another value");
    // we update the entity directly in the datastore right before commit
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    ldth.ds.put(entity);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (RollbackException re) {
      // good
      assertTrue(re.getCause() instanceof OptimisticLockException);
    }
    beginTxn();
    // make sure the version didn't change on the model object
    assertEquals(2L, hv.getVersion());
    commitTxn();
  }

  public void testOptimisticLocking_Delete() {
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.encodeKey(key);
    beginTxn();
    HasVersionJPA hv = em.find(HasVersionJPA.class, keyStr);

    // delete the entity in the datastore right before we commit
    ldth.ds.delete(key);
    hv.setValue("value");
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (RollbackException re) {
      // good
      assertTrue(re.getCause() instanceof OptimisticLockException);
    }
    beginTxn();
    // make sure the version didn't change on the model object
    assertEquals(1L, hv.getVersion());
    commitTxn();
  }

}
