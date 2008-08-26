// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.datanucleus.test.Book;
import org.easymock.EasyMock;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Transaction;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JPATransactionTest extends TestCase {

  LocalDatastoreTestHelper ldth = new LocalDatastoreTestHelper();
  DatastoreService mockDatastoreService = EasyMock.createMock(DatastoreService.class);
  Transaction mockTxn = EasyMock.createMock(Transaction.class);
  DatastoreServiceRecordingImpl recordingImpl;

  @Override
  protected void setUp() throws Exception {
    ldth.setUp();
    recordingImpl = new DatastoreServiceRecordingImpl(mockDatastoreService, ldth.ds);
    DatastoreServiceFactoryInternal.setDatastoreService(recordingImpl);
  }

  @Override
  protected void tearDown() throws Exception {
    EasyMock.reset(mockDatastoreService, mockTxn);
    ldth.tearDown();
    DatastoreServiceFactoryInternal.setDatastoreService(null);
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

  /**
   * Sets the DatastoreService returned by the DatastoreServiceFactory to an
   * implementation that returns the value from the "recorder" instead of the
   * "delegate".  This can be used to return a mock transaction.
   */
  private void setMockTransactionRecordingImpl() {
    recordingImpl = new DatastoreServiceRecordingImpl(mockDatastoreService, ldth.ds) {
      @Override
      public com.google.apphosting.api.datastore.Transaction beginTransaction() {
        delegate.beginTransaction();
        return recorder.beginTransaction();
      }
    };
    DatastoreServiceFactoryInternal.setDatastoreService(recordingImpl);
  }

  public void testTransactionalWrite() throws Exception {
    setMockTransactionRecordingImpl();
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("1");
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Book b1 = new Book();
    b1.setTitle("Foo Bar");
    b1.setAuthor("Joe Blow");
    b1.setIsbn("12345");

    EntityManager em = getEntityManager("transactional");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.persist(b1);
    txn.commit();

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
    em.persist(b1);
    txn.commit();

    EasyMock.verify(mockDatastoreService);
  }

  public void testTransactionalRead() throws Exception {
    setMockTransactionRecordingImpl();
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("1");
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity b1 = Book.newBookEntity("Joe Blow", "12345", "Foo Bar");
    ldth.ds.put(b1);

    EntityManager em = getEntityManager("transactional");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.find(Book.class, KeyFactory.encodeKey(b1.getKey()));
    txn.commit();

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testNontransactionalRead() throws Exception {
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService);

    Entity b1 = Book.newBookEntity("Joe Blow", "12345", "Foo Bar");
    ldth.ds.put(b1);

    EntityManager em = getEntityManager("nontransactional");
    EntityTransaction txn = em.getTransaction();
    txn.begin();
    em.find(Book.class, KeyFactory.encodeKey(b1.getKey()));
    txn.commit();

    EasyMock.verify(mockDatastoreService);
  }
}
