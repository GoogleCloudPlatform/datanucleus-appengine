// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import junit.framework.TestCase;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasAncestorJDO;
import org.easymock.EasyMock;

import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JDOTransactionTest extends TestCase {

  private LocalDatastoreTestHelper ldth;
  private DatastoreService mockDatastoreService = EasyMock.createMock(DatastoreService.class);
  private com.google.apphosting.api.datastore.Transaction mockTxn = EasyMock.createMock(
      com.google.apphosting.api.datastore.Transaction.class);
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
    properties.setProperty("datanucleus.identifier.case", "PreserveCase");
    PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(properties);
    return pmf.getPersistenceManager();
  }

  public void testTransactionalWrite() throws Exception {
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("0");
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
    try {
      pm.makePersistent(f1);
    } finally {
      txn.commit();
    }
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
    try {
      pm.makePersistent(f1);
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService);
  }

  public void testMixedTransactionalWrites() throws Exception {
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockDatastoreService.put(EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("0");
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
    try {
      pm.makePersistent(f1);
      HasAncestorJDO hasAncestor = new HasAncestorJDO(f1.getId());
      txn.setNontransactionalWrite(true);
      pm.makePersistent(hasAncestor);
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testTransactionalRead() throws Exception {
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
    try {
      pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testNontransactionalRead() throws Exception {
    mockTxn.commit();
    EasyMock.expectLastCall();
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null);
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity f1 = Flight.newFlightEntity("foo", "bar", "baz", 1, 2);
    ldth.ds.put(f1);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.begin();
    try {
      pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testMixedTransactionalReads() throws Exception {
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
    try {
      pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
      txn.setNontransactionalRead(true);
      pm.getObjectById(Flight.class, KeyFactory.encodeKey(f2.getKey()));
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }

  public void testMixedTransactionalReadsAndWrites() throws Exception {
    EasyMock.expect(mockDatastoreService.beginTransaction()).andReturn(mockTxn);
    EasyMock.expect(mockDatastoreService.get(EasyMock.isA(Key.class))).andReturn(null).times(3);
    EasyMock.expect(mockDatastoreService.put(
        EasyMock.isA(com.google.apphosting.api.datastore.Transaction.class),
        EasyMock.isA(Entity.class))).andReturn(null);
    EasyMock.expect(mockTxn.getId()).andReturn("1");
    mockTxn.commit();
    EasyMock.expectLastCall();
    EasyMock.replay(mockDatastoreService, mockTxn);

    Entity f1 = Flight.newFlightEntity("foo", "bar", "baz", 1, 2);
    ldth.ds.put(f1);

    PersistenceManager pm = getPersistenceManager();
    Transaction txn = pm.currentTransaction();
    txn.setNontransactionalRead(true);
    txn.setNontransactionalWrite(false);
    txn.begin();
    try {
      Flight flight = pm.getObjectById(Flight.class, KeyFactory.encodeKey(f1.getKey()));
      flight.setDest("BOS");
    } finally {
      txn.commit();
    }

    EasyMock.verify(mockDatastoreService, mockTxn);
  }
}
