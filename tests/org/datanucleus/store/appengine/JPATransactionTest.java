// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Transaction;

import junit.framework.TestCase;

import org.datanucleus.test.Book;
import org.easymock.EasyMock;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JPATransactionTest extends TestCase {

  private LocalDatastoreTestHelper ldth;
  private DatastoreService mockDatastoreService = EasyMock.createMock(DatastoreService.class);
  private Transaction mockTxn = EasyMock.createMock(Transaction.class);
  private DatastoreServiceRecordingImpl recordingImpl;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth = new LocalDatastoreTestHelper();
    ldth.setUp();
    recordingImpl = new DatastoreServiceRecordingImpl(mockDatastoreService, ldth.ds, mockTxn);
    DatastoreServiceFactoryInternal.setDatastoreService(recordingImpl);
  }

  @Override
  protected void tearDown() throws Exception {
    EasyMock.reset(mockDatastoreService, mockTxn);
    ldth.tearDown(true);
    ldth = null;
    DatastoreServiceFactoryInternal.setDatastoreService(null);
    recordingImpl = null;
    super.tearDown();
  }

  /**
   * A new EntityManager should be fetched on a per-test basis.  The
   * DatastoreService within the DatastorePersistenceHandler is obtained via the
   * DatastoreServiceFactory, so this ensures that the "injected" factory impl
   * is returned.
   */
  private EntityManager getEntityManager(String unit) {
    EntityManagerFactory emf = Persistence.createEntityManagerFactory(unit);
    return emf.createEntityManager();
  }

  public void testTransactionalWrite() throws Exception {
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("0");
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Book b1 = new Book();
    b1.setTitle("Foo Bar");
    b1.setAuthor("Joe Blow");
    b1.setIsbn("12345");

    EntityManager em = getEntityManager("transactional");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    try {
      em.persist(b1);
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testNontransactionalWrite() throws Exception {
    EasyMock.expect(mockDatastoreService.put(EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService);

    Book b1 = new Book();
    b1.setTitle("Foo Bar");
    b1.setAuthor("Joe Blow");
    b1.setIsbn("12345");

    EntityManager em = getEntityManager("nontransactional");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    try {
      em.persist(b1);
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService);
  }

  public void testTransactionalRead() throws Exception {
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(
        EasyMock.isA(Transaction.class),
        EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("1");
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity b1 = Book.newBookEntity("Joe Blow", "12345", "Foo Bar");
    ldth.ds.put(b1);

    EntityManager em = getEntityManager("transactional");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    try {
      em.find(Book.class, KeyFactory.encodeKey(b1.getKey()));
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testNontransactionalRead_Txn() throws Exception {
    mockTxn.commit();
    EasyMock.expectLastCall();
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity b1 = Book.newBookEntity("Joe Blow", "12345", "Foo Bar");
    ldth.ds.put(b1);

    EntityManager em = getEntityManager("nontransactional");
    em.getTransaction().begin();
    try {
      em.find(Book.class, KeyFactory.encodeKey(b1.getKey()));
    } finally {
      em.getTransaction().commit();
      em.close();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testNontransactionalRead_NoTxn() throws Exception {
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService);

    Entity b1 = Book.newBookEntity("Joe Blow", "12345", "Foo Bar");
    ldth.ds.put(b1);

    EntityManager em = getEntityManager("nontransactional");
    em.find(Book.class, KeyFactory.encodeKey(b1.getKey()));
    EasyMock.verify(mockDatastoreService);
  }
}
