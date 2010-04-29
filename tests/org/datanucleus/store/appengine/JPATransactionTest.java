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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;

import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed;
import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed;
import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.transactional_ds_non_transactional_ops_allowed;
import static org.datanucleus.store.appengine.JPATestCase.EntityManagerFactoryName.transactional_ds_non_transactional_ops_not_allowed;
import org.datanucleus.test.Book;
import org.datanucleus.test.Flight;
import org.datanucleus.test.HasKeyAncestorKeyPkJDO;
import org.easymock.EasyMock;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.Query;

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
public class JPATransactionTest extends DatastoreTestCase {

  private DatastoreService ds;
  private DatastoreService mockDatastoreService = EasyMock.createMock(DatastoreService.class);
  private Transaction mockTxn = EasyMock.createMock(Transaction.class);
  private DatastoreServiceRecordingImpl recordingImpl;
  private TxnIdAnswer txnIdAnswer;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ds = DatastoreServiceFactory.getDatastoreService();
    txnIdAnswer = new TxnIdAnswer();
    recordingImpl =
        new DatastoreServiceRecordingImpl(mockDatastoreService, ds, mockTxn, txnIdAnswer);
    DatastoreServiceFactoryInternal.setDatastoreService(recordingImpl);
  }

  @Override
  protected void tearDown() throws Exception {
    EasyMock.reset(mockDatastoreService, mockTxn);
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
    if (explicitDemarcation) {
      EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(mockTxn);
    }
    EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(null);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.appengine.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andAnswer(txnIdAnswer).anyTimes();
    EasyMock.expect(mockTxn.isActive()).andReturn(true).anyTimes();
    EasyMock.expect(mockTxn.getApp()).andReturn("test").anyTimes();
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
    EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(mockTxn);
    if (explicitDemarcation) {
      EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(null);
    }
    EasyMock.expect(mockDatastoreService.get(
        EasyMock.isA(com.google.appengine.api.datastore.Transaction.class),
        EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andAnswer(txnIdAnswer).anyTimes();
    EasyMock.expect(mockTxn.isActive()).andReturn(true).anyTimes();
    EasyMock.expect(mockTxn.getApp()).andReturn("test").anyTimes();
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity entity = Book.newBookEntity("jimmy", "123456", "great american novel");
    ds.put(entity);
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
    ds.put(entity);
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

  private interface QueryRunner {
    void runQuery(EntityManager em);
    boolean isAncestor();
  }

  private void testQueryPermutationWithoutExpectedDatastoreTxn(
      EntityManager em, boolean explicitDemarcation,
      QueryRunner queryRunner) throws EntityNotFoundException {
    if (queryRunner.isAncestor()) {
      EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(null);
    }
    EasyMock.expect(mockDatastoreService.prepare(
        (com.google.appengine.api.datastore.Transaction) EasyMock.isNull(),
        EasyMock.isA(com.google.appengine.api.datastore.Query.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService, mockTxn);

    javax.persistence.EntityTransaction txn = em.getTransaction();
    if (explicitDemarcation) {
      txn.begin();
    }
    try {
      queryRunner.runQuery(em);
    } finally {
      if (explicitDemarcation) {
        txn.commit();
      }
    }
    EasyMock.verify(mockDatastoreService, mockTxn);
    EasyMock.reset(mockDatastoreService, mockTxn);
  }

  private void testQueryPermutationWithExpectedDatastoreTxn(
      EntityManager em, boolean explicitDemarcation,
      QueryRunner queryRunner) throws EntityNotFoundException {

    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(mockTxn);
    if (queryRunner.isAncestor()) {
      EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(mockTxn);
    }
    EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(null);
    if (queryRunner.isAncestor()) {
      EasyMock.expect(mockDatastoreService.prepare(
          EasyMock.isA(com.google.appengine.api.datastore.Transaction.class),
          EasyMock.isA(com.google.appengine.api.datastore.Query.class))).andReturn(null);
    } else {
      EasyMock.expect(mockDatastoreService.prepare(
          (com.google.appengine.api.datastore.Transaction) EasyMock.isNull(),
          EasyMock.isA(com.google.appengine.api.datastore.Query.class))).andReturn(null);
    }
    EasyMock.expect(mockTxn.getId()).andAnswer(txnIdAnswer).anyTimes();
    EasyMock.expect(mockTxn.isActive()).andReturn(true).anyTimes();
    EasyMock.expect(mockTxn.getApp()).andReturn("test").anyTimes();
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    javax.persistence.EntityTransaction txn = em.getTransaction();
    if (explicitDemarcation) {
      txn.begin();
    }
    try {
      queryRunner.runQuery(em);
    } finally {
      if (explicitDemarcation) {
        txn.commit();
      }
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
    EasyMock.reset(mockDatastoreService, mockTxn);
  }

  private static final QueryRunner ANCESTOR = new QueryRunner() {
    public void runQuery(EntityManager em) {
      Query q = em.createQuery("SELECT FROM " + HasKeyAncestorKeyPkJDO.class.getName() + " WHERE ancestorKey = :p");
      q.setParameter("p", KeyFactory.createKey("yar", 23));
      q.getResultList();
    }

    public boolean isAncestor() {
      return true;
    }
  };

  private static final QueryRunner NON_ANCESTOR = new QueryRunner() {
    public void runQuery(EntityManager em) {
      Query q = em.createQuery("SELECT FROM " + Flight.class.getName());
      q.getResultList();
    }

    public boolean isAncestor() {
      return false;
    }
  };

  public void testQueriesWithDatastoreTxn() throws Exception {
    EntityManagerFactory emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_allowed.name());
    EntityManager em = emf.createEntityManager();
    testQueryPermutationWithExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, ANCESTOR);
    testQueryPermutationWithExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, NON_ANCESTOR);
    em.close();
    emf.close();
    emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_not_allowed.name());
    em = emf.createEntityManager();
    testQueryPermutationWithExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, ANCESTOR);
    testQueryPermutationWithExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, NON_ANCESTOR);
    em.close();
    emf.close();
  }

  public void testQueriesWithoutDatastoreTxn() throws Exception {
    EntityManagerFactory emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_allowed.name());
    EntityManager em = emf.createEntityManager();
    testQueryPermutationWithoutExpectedDatastoreTxn(em, NO_EXPLICIT_DEMARCATION, ANCESTOR);
    em.close();
    emf.close();

    emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_not_allowed.name());
    em = emf.createEntityManager();
    testQueryPermutationWithoutExpectedDatastoreTxn(em, NO_EXPLICIT_DEMARCATION, ANCESTOR);
    em.close();
    emf.close();

    emf = getEntityManagerFactory(nontransactional_ds_non_transactional_ops_not_allowed.name());
    em = emf.createEntityManager();
    testQueryPermutationWithoutExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, ANCESTOR);
    testQueryPermutationWithoutExpectedDatastoreTxn(em, NO_EXPLICIT_DEMARCATION, ANCESTOR);
    testQueryPermutationWithoutExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, NON_ANCESTOR);
    testQueryPermutationWithoutExpectedDatastoreTxn(em, NO_EXPLICIT_DEMARCATION, NON_ANCESTOR);
    em.close();
    emf.close();

    emf = getEntityManagerFactory(nontransactional_ds_non_transactional_ops_allowed.name());
    em = emf.createEntityManager();
    testQueryPermutationWithoutExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, ANCESTOR);
    testQueryPermutationWithoutExpectedDatastoreTxn(em, NO_EXPLICIT_DEMARCATION, ANCESTOR);
    testQueryPermutationWithoutExpectedDatastoreTxn(em, EXPLICIT_DEMARCATION, NON_ANCESTOR);
    testQueryPermutationWithoutExpectedDatastoreTxn(em, NO_EXPLICIT_DEMARCATION, NON_ANCESTOR);
    em.close();
    emf.close();
  }

  public void testEmptyTxnBlock_Txn() {
    EntityManagerFactory emf = getEntityManagerFactory(transactional_ds_non_transactional_ops_allowed.name());
    EntityManager em = emf.createEntityManager();
    try {
      EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
      EasyMock.expect(mockDatastoreService.getCurrentTransaction(null)).andReturn(mockTxn);
      EasyMock.expect(mockTxn.getId()).andAnswer(txnIdAnswer);
      mockTxn.commit();
      EasyMock.expectLastCall();
      EasyMock.replay(mockDatastoreService, mockTxn);
      em.getTransaction().begin();
      em.getTransaction().commit();
      EasyMock.verify(mockDatastoreService, mockTxn);
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
      emf.close();
    }
  }

  public void testEmptyTxnBlock_NoTxn() {
    EntityManagerFactory emf = getEntityManagerFactory(nontransactional_ds_non_transactional_ops_allowed.name());
    EntityManager em = emf.createEntityManager();
    try {
      EasyMock.replay(mockDatastoreService, mockTxn);
      em.getTransaction().begin();
      em.getTransaction().commit();
      EasyMock.verify(mockDatastoreService, mockTxn);
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
      emf.close();
    }
  }
}
