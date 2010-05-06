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

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.test.Book;

import java.sql.SQLException;
import java.util.ConcurrentModificationException;

import javax.persistence.PersistenceException;
import javax.persistence.RollbackException;
import javax.transaction.xa.XAException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAConcurrentModificationTest extends JPATestCase {

  public void testInsertCollides() {
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);
    try {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testInsertCollidesOnCommit() {
    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count != 0) {
              throw new ConcurrentModificationException();
            }
            count++;
          }
        };
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);
    try {
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
        assertTrue(e.getCause().getCause() instanceof XAException);
        assertTrue(e.getCause().getCause().getCause() instanceof SQLException);
        assertTrue(e.getCause().getCause().getCause().getCause() instanceof ConcurrentModificationException);
      }
      assertFalse(em.getTransaction().isActive());
      assertEquals(book, "harold", "1234", 1888, "the title");
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateCollides() {
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    CollisionDatastoreDelegate dd =
        new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);
    try {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateOfDetachedCollides() {
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    beginTxn();
    Book book = em.find(Book.class, e.getKey());
    commitTxn();

    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);

    try {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateOfDetachedCollidesThenSucceeds() {

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    beginTxn();
    Book book = em.find(Book.class, e.getKey());
    commitTxn();

    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);

    try {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateOfAttachedCollidesThenSucceeds() {

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    beginTxn();
    Book b = em.find(Book.class, e.getKey());
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);

    try {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testDeleteCollides() {
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);

    try {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testInsertCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);
    try {
      Book book = new Book();
      book.setAuthor("harold");
      book.setIsbn("1234");
      book.setFirstPublished(1988);
      book.setTitle("the title");
      em.persist(book);
      try {
        em.close();
        fail("expected exception");
      } catch (PersistenceException e) {
        assertTrue(e.getCause() instanceof NucleusDataStoreException);
        assertTrue(e.getCause().getCause() instanceof ConcurrentModificationException);
      }
      assertEquals(book, "harold", "1234", 1988, "the title");
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);

    try {
      Book b = em.find(Book.class, e.getKey());
      b.setFirstPublished(1988);
      try {
        em.close();
        fail("expected exception");
      } catch (PersistenceException ex) {
        assertTrue(ex.getCause() instanceof NucleusDataStoreException);
        assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
      }
      assertEquals(b, "harold", "1234", 1988, "the title");
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateOfDetachedCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);
    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    // DataNuc JPA impl won't detach the object unless you open and close a txn
    beginTxn();
    Book book = em.find(Book.class, e.getKey());
    commitTxn();
    em.close();
    em = emf.createEntityManager();

    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);

    try {
      // update detached object
      book.setFirstPublished(1988);

      // reattach
      em.merge(book);

      try {
        em.close();
        fail("expected exception");
      } catch (PersistenceException ex) {
        assertTrue(ex.getCause() instanceof NucleusDataStoreException);
        assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
      }
      // now verify that the new value is still in the detached version.
      assertEquals(book, "harold", "1234", 1988, "the title");
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateOfDetachedCollidesThenSucceeds_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    beginTxn();
    Book b = em.find(Book.class, e.getKey());
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);

    try {
      // update detached object
      b.setFirstPublished(1988);

      // reattach
      em.merge(b);
      try {
        em.close();
        fail("expected exception");
      } catch (PersistenceException ex) {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testUpdateOfAttachedCollidesThenSucceeds_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);

    ExceptionThrowingDatastoreDelegate.ExceptionPolicy policy =
        new ExceptionThrowingDatastoreDelegate.BaseExceptionPolicy() {
          int count = 0;
          protected void doIntercept(String methodName) {
            if (count == 0) {
              count++;
              throw new ConcurrentModificationException();
            }
          }
        };

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    Book b = em.find(Book.class, e.getKey());
    // make a copy right away, otherwise our change will get reverted
    // when the txn rolls back
    ExceptionThrowingDatastoreDelegate dd =
        new ExceptionThrowingDatastoreDelegate(getDelegateForThread(), policy);
    setDelegateForThread(dd);

    try {
      // update attached object
      b.setFirstPublished(1988);
      em.merge(b);
      try {
        em.close();
        fail("expected exception");
      } catch (PersistenceException ex) {
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
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  public void testDeleteCollides_NoTxn() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed);

    Entity e = Book.newBookEntity("harold", "1234", "the title");
    ds.put(e);
    CollisionDatastoreDelegate dd = new CollisionDatastoreDelegate(getDelegateForThread());
    setDelegateForThread(dd);

    try {
      Book b = em.find(Book.class, e.getKey());

      em.remove(b);
      try {
        em.close();
        fail("expected exception");
      } catch (PersistenceException ex) {
        assertTrue(ex.getCause() instanceof NucleusDataStoreException);
        assertTrue(ex.getCause().getCause() instanceof ConcurrentModificationException);
      }
    } finally {
      setDelegateForThread(dd.getInner());
    }
  }

  private void assertEquals(Book book, String author, String isbn, int firstPublished, String title) {
    assertEquals(author, book.getAuthor());
    assertEquals(isbn, book.getIsbn());
    assertEquals(firstPublished, book.getFirstPublished());
    assertEquals(title, book.getTitle());
  }
}