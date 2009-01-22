package org.datanucleus.store.appengine;

import junit.framework.TestCase;

import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;

import javax.jdo.JDOHelper;

public class JDODataSourceConfigTest extends TestCase {

  public void testTransactionalPMF() {
    JDOPersistenceManagerFactory pmf =
        (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory("transactional");
    DatastoreManager storeMgr = (DatastoreManager) pmf.getStoreManager();
    JDOPersistenceManager pm = (JDOPersistenceManager) pmf.getPersistenceManager();
    assertTrue(storeMgr.connectionFactoryIsTransactional());
    pm.close();
    pmf.close();
  }

  public void testNonTransactionalPMF() {
    JDOPersistenceManagerFactory pmf =
        (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory("nontransactional");
    DatastoreManager storeMgr = (DatastoreManager) pmf.getStoreManager();
    JDOPersistenceManager pm = (JDOPersistenceManager) pmf.getPersistenceManager();
    assertFalse(storeMgr.connectionFactoryIsTransactional());
    pm.close();
    pmf.close();
  }

  

}
