// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import junit.framework.TestCase;

import org.datanucleus.test.Flight;
import org.easymock.EasyMock;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.DatastoreServiceFactory;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JDOTransactionTest extends TestCase {

  LocalDatastoreTestHelper ldth = new LocalDatastoreTestHelper();
  DatastoreService mockDatastoreService = EasyMock.createMock(DatastoreService.class);
  com.google.apphosting.api.datastore.Transaction mockTxn = EasyMock.createMock(
      com.google.apphosting.api.datastore.Transaction.class);
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
   * A new PersistenceManager should be fetched on a per-test basis.  The
   * DatastoreService within the DatastorePersistenceHandler is obtained via the
   * DatastoreServiceFactory, so this ensures that the "injected" factory impl
   * is returned.
   */
  private PersistenceManager getPersistenceManager() {
    Properties properties = new Properties();
    properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
                    "org.datanucleus.jdo.JDOPersistenceManagerFactory");
    properties.setProperty("javax.jdo.option.ConnectionURL","appengine");
    properties.setProperty("datanucleus.NontransactionalRead", Boolean.TRUE.toString());
    properties.setProperty("datanucleus.NontransactionalWrite", Boolean.TRUE.toString());
    PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(properties);
    return pmf.getPersistenceManager();
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

    Flight f1 = new Flight();
    f1.setName("Harold");
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setYou(1);
    f1.setMe(2);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.setNontransactionalWrite(false);
    txn.begin();
    pm.makePersistent(f1);
    txn.commit();

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testNontransactionalWrite() throws Exception {
    EasyMock.expect(mockDatastoreService.put(EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService);

    Flight f1 = new Flight();
    f1.setName("Harold");
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setYou(1);
    f1.setMe(2);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.begin();
    pm.makePersistent(f1);
    txn.commit();

    EasyMock.verify(mockDatastoreService);
  }

  public void testMixedTransactionalWrites() throws Exception {
    setMockTransactionRecordingImpl();
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockDatastoreService.put(EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("1");
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Flight f1 = new Flight();
    f1.setName("Harold");
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setYou(1);
    f1.setMe(2);

    Flight f2 = new Flight();
    f1.setName("Kumar");
    f1.setOrigin("LAX");
    f1.setDest("SFO");
    f1.setYou(3);
    f1.setMe(4);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.setNontransactionalWrite(false);
    txn.begin();
    pm.makePersistent(f1);
    txn.setNontransactionalWrite(true);
    pm.makePersistent(f2);
    txn.commit();

    EasyMock.verify(mockDatastoreService, mockTxn);
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

    Entity f1 = Flight.newFlightEntity("foo", "bar", "baz", 1, 2);
    ldth.ds.put(f1);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.setNontransactionalRead(false);
    txn.begin();
    pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
    txn.commit();

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testNontransactionalRead() throws Exception {
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService);

    Entity f1 = Flight.newFlightEntity("foo", "bar", "baz", 1, 2);
    ldth.ds.put(f1);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.begin();
    pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
    txn.commit();

    EasyMock.verify(mockDatastoreService);
  }

  public void testMixedTransactionalReads() throws Exception {
    setMockTransactionRecordingImpl();
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("1");
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity f1 = Flight.newFlightEntity("foo", "bar", "baz", 1, 2);
    Entity f2 = Flight.newFlightEntity("baz", "Foo", "bar", 3, 4);
    ldth.ds.put(f1);
    ldth.ds.put(f2);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.setNontransactionalRead(false);
    txn.begin();
    pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
    txn.setNontransactionalRead(true);
    pm.getObjectById(Flight.class, KeyFactory.encodeKey(f2.getKey()));
    txn.commit();

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testMixedTransactionalReadsAndWrites() throws Exception {
    setMockTransactionRecordingImpl();
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("1");
    mockTxn.commit();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity f1 = Flight.newFlightEntity("foo", "bar", "baz", 1, 2);
    ldth.ds.put(f1);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.setNontransactionalRead(true);
    txn.setNontransactionalWrite(false);
    txn.begin();
    Flight flight = pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
    flight.setDest("BOS");
    txn.commit();

    EasyMock.verify(mockDatastoreService, mockTxn);
  }
}
