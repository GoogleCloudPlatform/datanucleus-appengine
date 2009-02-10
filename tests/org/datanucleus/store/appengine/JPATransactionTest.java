// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;

import junit.framework.TestCase;

import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed;
import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed;
import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.transactional_ds_non_transactional_ops_allowed;
import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.transactional_ds_non_transactional_ops_not_allowed;
import org.datanucleus.test.Book;
import org.easymock.EasyMock;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

/**
 * Verifies jpa txn behavior across the following variables:
 * datasource type (txn | nontxn)
 * programmatic txn demarcation (yes | no)
 * operation (read | write)
 * support for that operation outside a txn (yes | no)
 *
 * See https://spreadsheets.google.com/a/google.com/pub?key=p8C3zgqqUfpstFKZ4ns1bQg
 * for all the gory details.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 * @author Max Ross <maxr@google.com>
 */
public class JPATransactionTest extends TestCase {

  private static int handleCounter = 0;

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
    handleCounter = 0;
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
   * A new EntityManagerFactory should be fetched on a per-test basis.  The
   * DatastoreService within the DatastorePersistenceHandler is obtained via the
   * DatastoreServiceFactory, so this ensures that the "injected" factory impl
   * is returned.
   */
  private EntityManagerFactory getEntityManagerFactory(String unit) {
    return Persistence.createEntityManagerFactory(unit);
  }

  private void testWritePermutationWithExpectedDatastoreTxn(
      EntityManagerFactory emf, boolean explicitDemarcation) {
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.appengine.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn(Integer.toString(handleCounter++)).anyTimes();
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Book b1 = new Book();
    b1.setTitle("Foo Bar");
    b1.setAuthor("Joe Blow");
    b1.setIsbn("12345");

    EntityManager em = emf.createEntityManager();
    EntityTransaction txn = em.getTransaction();
    if (explicitDemarcation) {
      txn.begin();
    }
    try {
      em.persist(b1);
    } finally {
      if (explicitDemarcation) {
        txn.commit();
      }
      em.close();
    }
    EasyMock.verify(mockDatastoreService, mockTxn);
    EasyMock.reset(mockDatastoreService, mockTxn);
  }

  private void testReadPermutationWithExpectedDatastoreTxn(
      EntityManagerFactory emf, boolean explicitDemarcation) throws EntityNotFoundException {

    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(
        EasyMock.isA(com.google.appengine.api.datastore.Transaction.class),
        EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn(Integer.toString(handleCounter++)).anyTimes();
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity entity = Book.newBookEntity("jimmy", "123456", "great american novel");
    ldth.ds.put(entity);
    EntityManager em = emf.createEntityManager();
    EntityTransaction txn = em.getTransaction();
    if (explicitDemarcation) {
      txn.begin();
    }
    try {
      em.find(Book.class, KeyFactory.keyToString(entity.getKey()));
    } finally {
      if (explicitDemarcation) {
        txn.commit();
      }
      em.close();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
    EasyMock.reset(mockDatastoreService, mockTxn);
  }

  private void testWritePermutationWithoutExpectedDatastoreTxn(
      EntityManagerFactory emf, boolean explicitDemarcation) {
    EasyMock.expect(mockDatastoreService.put(EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService, mockTxn);

    Book b1 = new Book();
    b1.setTitle("Foo Bar");
    b1.setAuthor("Joe Blow");
    b1.setIsbn("12345");

    EntityManager em = emf.createEntityManager();
    EntityTransaction txn = em.getTransaction();
    if (explicitDemarcation) {
      txn.begin();
    }
    try {
      em.persist(b1);
    } finally {
      if (explicitDemarcation) {
        txn.commit();
      }
      em.close();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
    EasyMock.reset(mockDatastoreService, mockTxn);
  }

  private void testReadPermutationWithoutExpectedDatastoreTxn(
      EntityManagerFactory emf, boolean explicitDemarcation) throws EntityNotFoundException {
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity entity = Book.newBookEntity("jimmy", "123456", "great american novel");
    ldth.ds.put(entity);
    EntityManager em = emf.createEntityManager();
    EntityTransaction txn = em.getTransaction();
    if (explicitDemarcation) {
      txn.begin();
    }
    try {
      em.find(Book.class, KeyFactory.keyToString(entity.getKey()));
    } finally {
      if (explicitDemarcation) {
        txn.commit();
      }
      em.close();
    }
    EasyMock.verify(mockDatastoreService, mockTxn);
    EasyMock.reset(mockDatastoreService, mockTxn);
  }

  private static final boolean EXPLICIT_DEMARCATION = true;
  private static final boolean NO_EXPLICIT_DEMARCATION = false;

  public void testWritesWithDatastoreTxn() throws Exception {
    EntityManagerFactory emf = getEntityManagerFactory(
        transactional_ds_non_transactional_ops_not_allowed.name());
    testWritePermutationWithExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    testWritePermutationWithExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);
    emf.close();

    emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_allowed.name());
    testWritePermutationWithExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    testWritePermutationWithExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);
    emf.close();
  }

  public void testReadsWithDatastoreTxn() throws Exception {
    EntityManagerFactory emf = getEntityManagerFactory(
        transactional_ds_non_transactional_ops_not_allowed.name());
    testReadPermutationWithExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    emf.close();
    emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_allowed.name());
    testReadPermutationWithExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    emf.close();
  }

  public void testWritesWithoutDatastoreTxn() throws Exception {
    EntityManagerFactory emf = getEntityManagerFactory(
        nontransactional_ds_non_transactional_ops_allowed.name());
    testWritePermutationWithoutExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    testWritePermutationWithoutExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);
    emf.close();

    emf = getEntityManagerFactory(nontransactional_ds_non_transactional_ops_not_allowed.name());
    testWritePermutationWithoutExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    testWritePermutationWithoutExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);

    emf.close();
  }

  public void testReadsWithoutDatastoreTxn() throws Exception {
    EntityManagerFactory emf = getEntityManagerFactory(
        transactional_ds_non_transactional_ops_allowed.name());
    testReadPermutationWithoutExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);
    emf.close();

    emf = getEntityManagerFactory(nontransactional_ds_non_transactional_ops_allowed.name());
    testReadPermutationWithoutExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    testReadPermutationWithoutExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);
    emf.close();

    emf = getEntityManagerFactory(nontransactional_ds_non_transactional_ops_not_allowed.name());
    testReadPermutationWithoutExpectedDatastoreTxn(emf, EXPLICIT_DEMARCATION);
    testReadPermutationWithoutExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);

    emf.close();

    emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_not_allowed.name());
    testReadPermutationWithoutExpectedDatastoreTxn(emf, NO_EXPLICIT_DEMARCATION);
    emf.close();
  }
}
