/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * **********************************************************************/

package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.apphosting.api.ApiProxy;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.test.Book;

import java.util.ConcurrentModificationException;

import javax.jdo.JDOException;
import javax.persistence.PersistenceException;
import javax.persistence.RollbackException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAConcurrentModificationTest extends JPATestCase {

  public void testInsertCollides() {
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);
    Book book = new Book();
    book.setAuthor("harold");
    book.setIsbn("1234");
    book.setFirstPublished(1888);
    book.setTitle("the title");
    beginTxn();
    em.persist(book);
    try {
      commitTxn();
      fail("expected exception");
    } catch (RollbackException e) {
      // good
      assertTrue(e.getCause() instanceof PersistenceException);
      assertTrue(e.getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(em.getTransaction().isActive());
    assertEquals(book, "harold", "1234", 1888, "the title");
  }

  public void testInsertCollidesOnCommit() {
    CollidingUpdateDatastoreDelegate.CollisionPolicy policy =
        new CollidingUpdateDatastoreDelegate.BaseCollisionPolicy() {
          int count = 0;
          protected void doCollide(String methodName) {
            if (count != 0) {
              throw new ConcurrentModificationException();
            }
            count++;
          }
        };
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);
    Book book = new Book();
    book.setAuthor("harold");
    book.setIsbn("1234");
    book.setFirstPublished(1888);
    book.setTitle("the title");
    beginTxn();
    em.persist(book);
    try {
      commitTxn();
      fail("expected exception");
    } catch (RollbackException e) {
      // good
      assertTrue(e.getCause() instanceof PersistenceException);
      assertTrue(e.getCause().getCause() instanceof NucleusDataStoreException);
      assertTrue(e.getCause().getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(em.getTransaction().isActive());
    assertEquals(book, "harold", "1234", 1888, "the title");
  }

  public void testUpdateCollides() {
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    beginTxn();
    Book b = em.find(Book.class, e.getKey());
    b.setFirstPublished(1998);
    try {
      commitTxn();
      fail("expected exception");
    } catch (RollbackException ex) {
      // good
      assertTrue(ex.getCause() instanceof PersistenceException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(em.getTransaction().isActive());
    assertEquals(b, "harold", "1234", 2000, "the title");
  }

  public void testUpdateOfDetachedCollides() {
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    beginTxn();
    Book book = em.find(Book.class, e.getKey());
    commitTxn();

    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    // update detached object
    book.setFirstPublished(1988);
    beginTxn();

    // reattach
    em.merge(book);
    try {
      commitTxn();
      fail("expected exception");
    } catch (RollbackException ex) {
      // good
      assertTrue(ex.getCause() instanceof PersistenceException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(em.getTransaction().isActive());
    // now verify that the new value is still in the detached version.
    assertEquals(book, "harold", "1234", 1988, "the title");
  }

  public void testUpdateOfDetachedCollidesThenSucceeds() {

    CollidingUpdateDatastoreDelegate.CollisionPolicy policy =
        new CollidingUpdateDatastoreDelegate.BaseCollisionPolicy() {
          int count = 0;
          protected void doCollide(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    beginTxn();
    Book book = em.find(Book.class, e.getKey());
    commitTxn();

    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update detached object
    book.setFirstPublished(1988);
    beginTxn();

    // reattach
    em.merge(book);
    try {
      commitTxn();
      fail("expected exception");
    } catch (RollbackException ex) {
      // good
      assertTrue(ex.getCause() instanceof PersistenceException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertFalse(em.getTransaction().isActive());
    beginTxn();
    em.merge(book);
    commitTxn();
    beginTxn();
    book = em.find(Book.class, e.getKey());
    assertEquals(book, "harold", "1234", 1988, "the title");
    commitTxn();
  }

  public void testUpdateOfAttachedCollidesThenSucceeds() {

    CollidingUpdateDatastoreDelegate.CollisionPolicy policy =
        new CollidingUpdateDatastoreDelegate.BaseCollisionPolicy() {
          int count = 0;
          protected void doCollide(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    beginTxn();
    Book b = em.find(Book.class, e.getKey());
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update attached object
    b.setFirstPublished(1988);
    try {
      commitTxn();
      fail("expected exception");
    } catch (RollbackException ex) {
      // good
      assertTrue(ex.getCause() instanceof PersistenceException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    // rollback of txn causes state of pojo to rollback as well
    assertEquals(2000, b.getFirstPublished());
    assertFalse(em.getTransaction().isActive());
    beginTxn();
    // reapply the change
    b.setFirstPublished(1988);
    em.merge(b);
    commitTxn();
    beginTxn();
    b = em.find(Book.class, e.getKey());
    assertEquals(b, "harold", "1234", 1988, "the title");
    commitTxn();
  }

  public void testDeleteCollides() {
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    beginTxn();
    Book b = em.find(Book.class, e.getKey());
    em.remove(b);

    try {
      commitTxn();
      fail("expected exception");
    } catch (RollbackException ex) {
      // good
      assertTrue(ex.getCause() instanceof PersistenceException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
  }

  public void testInsertCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);
    Book book = new Book();
    book.setAuthor("harold");
    book.setIsbn("1234");
    book.setFirstPublished(1988);
    book.setTitle("the title");
    em.persist(book);
    try {
      em.close();
      fail("expected exception");
    } catch (JDOException e) {
      // ouch, datanuc bug.  test will fail when fixed
      assertTrue(e.getCause() instanceof NucleusDataStoreException);
      assertTrue(e.getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertEquals(book, "harold", "1234", 1988, "the title");
  }

  public void testUpdateCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    Book b = em.find(Book.class, e.getKey());
    b.setFirstPublished(1988);
    try {
      em.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // ouch, datanuc bug.  test will fail when fixed
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    assertEquals(b, "harold", "1234", 1988, "the title");
  }

  public void testUpdateOfDetachedCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    // DataNuc JPA impl won't detach the object unless you open and close a txn
    beginTxn();
    Book book = em.find(Book.class, e.getKey());
    commitTxn();
    em.close();
    em = emf.createEntityManager();

    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    // update detached object
    book.setFirstPublished(1988);

    // reattach
    em.merge(book);

    try {
      em.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // ouch, datanuc bug.  test will fail when fixed
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    // now verify that the new value is still in the detached version.
    assertEquals(book, "harold", "1234", 1988, "the title");
  }

  public void testUpdateOfDetachedCollidesThenSucceeds_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);

    CollidingUpdateDatastoreDelegate.CollisionPolicy policy =
        new CollidingUpdateDatastoreDelegate.BaseCollisionPolicy() {
          int count = 0;
          protected void doCollide(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    beginTxn();
    Book b = em.find(Book.class, e.getKey());
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update detached object
    b.setFirstPublished(1988);

    // reattach
    em.merge(b);
    try {
      em.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // ouch, datanuc bug.  test will fail when fixed
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    em = emf.createEntityManager();
    em.merge(b);
    em.close();
    em = emf.createEntityManager();
    b = em.find(Book.class, e.getKey());
    assertEquals(b, "harold", "1234", 1988, "the title");
    em.close();
  }

  public void testUpdateOfAttachedCollidesThenSucceeds_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);

    CollidingUpdateDatastoreDelegate.CollisionPolicy policy =
        new CollidingUpdateDatastoreDelegate.BaseCollisionPolicy() {
          int count = 0;
          protected void doCollide(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    Book b = em.find(Book.class, e.getKey());
    // make a copy right away, otherwise our change will get reverted
    // when the txn rolls back
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate(), policy);
    ApiProxy.setDelegate(dd);

    // update attached object
    b.setFirstPublished(1988);
    em.merge(b);
    try {
      em.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // ouch, datanuc bug.  test will fail when fixed
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
    em = emf.createEntityManager();
    b = em.find(Book.class, e.getKey());
    // update attached object
    b.setFirstPublished(1988);
    em.merge(b);
    em.close();
    em = emf.createEntityManager();
    b = em.find(Book.class, e.getKey());
    assertEquals(b, "harold", "1234", 1988, "the title");
    em.close();
  }

  public void testDeleteCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ldth.ds.put(e);
    CollidingUpdateDatastoreDelegate dd =
        new CollidingUpdateDatastoreDelegate(ApiProxy.getDelegate());
    ApiProxy.setDelegate(dd);

    Book b = em.find(Book.class, e.getKey());

    em.remove(b);
    try {
      em.close();
      fail("expected exception");
    } catch (JDOException ex) {
      // ouch, datanuc bug.  test will fail when fixed
      assertTrue(ex.getCause() instanceof NucleusDataStoreException);
      assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
    }
  }

  private void assertEquals(Book book, String author, String isbn, int firstPublished, String title) {
    assertEquals(author, book.getAuthor());
    assertEquals(isbn, book.getIsbn());
    assertEquals(firstPublished, book.getFirstPublished());
    assertEquals(title, book.getTitle());
  }
}