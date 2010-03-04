/*********************b*************************************************
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

import com.google.appengine.api.datastore.Query;

import junit.framework.TestCase;

import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.jpa.EntityManagerImpl;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.appengine.jpa.DatastoreEntityManagerFactory;
import org.datanucleus.store.mapped.MappedStoreManager;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPATestCase extends TestCase {

  private static
  Map<EntityManagerFactoryName, EntityManagerFactory> emfCache = Utils.newHashMap();

  protected EntityManagerFactory emf;
  protected EntityManager em;

  protected DatastoreTestHelper ldth;

  private boolean failed = false;
  @Override
  protected void runTest() throws Throwable {
    try {
      super.runTest();
    } catch (Throwable t) {
      failed = true;
      throw t;
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ldth = new DatastoreTestHelper();
    ldth.setUp();
    emf = emfCache.get(getEntityManagerFactoryName());
    boolean success = false;
    try {
      if (emf == null) {
        emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name());
        if (cacheManagers()) {
          emfCache.put(getEntityManagerFactoryName(), emf);
        }
      }
      em = emf.createEntityManager();
      success = true;
    } finally {
      if (!success) {
        ldth.tearDown(false);
      }
    }
  }

  public enum EntityManagerFactoryName {
    // nonTransactionalRead and nonTransactionalWrite are true
    transactional_ds_non_transactional_ops_allowed,
    // nonTransactionalRead and nonTransactionalWrite are false
    transactional_ds_non_transactional_ops_not_allowed,
    // nonTransactionalRead and nonTransactionalWrite are true
    nontransactional_ds_non_transactional_ops_allowed,
    // nonTransactionalRead and nonTransactionalWrite are false
    nontransactional_ds_non_transactional_ops_not_allowed
  }

  /**
   * By default we use a datasource that requires txns.
   * Override this if your test needs to use a different instance.
   */
  protected EntityManagerFactoryName getEntityManagerFactoryName() {
    return EntityManagerFactoryName.transactional_ds_non_transactional_ops_not_allowed;
  }

  @Override
  protected void tearDown() throws Exception {
    boolean throwIfActiveTxn = !failed;
    failed = false;
    try {
      if (em.isOpen()) {
        if (em.getTransaction().isActive()) {
          em.getTransaction().rollback();
        }
        em.close();
      }
      em = null;
      // see if anybody closed any of our pms just remove them from the cache -
      // we'll rebuild it the next time it's needed.
      for (Map.Entry<EntityManagerFactoryName, EntityManagerFactory> entry : emfCache.entrySet()) {
        if (!entry.getValue().isOpen()) {
          emfCache.remove(entry.getKey());
        }
      }
      if (!cacheManagers() && emf.isOpen()) {
        emf.close();
      }
      emf = null;
    } finally {
      ldth.tearDown(throwIfActiveTxn);
      ldth = null;
      super.tearDown();
    }
  }

  protected void beginTxn() {
    em.getTransaction().begin();
  }

  protected void commitTxn() {
    em.getTransaction().commit();
  }

  protected void rollbackTxn() {
    em.getTransaction().rollback();
  }

  protected void switchDatasource(EntityManagerFactoryName name) {
    em.close();
    if (!cacheManagers() && emf.isOpen()) {
      emf.close();
    }
    emf = Persistence.createEntityManagerFactory(name.name());
    em = emf.createEntityManager();
  }

  public int countForClass(Class<?> clazz) {
    String kind = kindForClass(clazz);
    return ldth.ds.prepare(new Query(kind)).countEntities();
  }

  protected String kindForClass(Class<?> clazz) {
    JDOPersistenceManagerFactory pmf = (JDOPersistenceManagerFactory) ((DatastoreEntityManagerFactory)emf)
        .getPersistenceManagerFactory();
    OMFContext omfContext = pmf.getOMFContext();
    MetaDataManager mdm = omfContext.getMetaDataManager();
    MappedStoreManager storeMgr = (MappedStoreManager) pmf.getStoreManager();
    return EntityUtils.determineKind(
        mdm.getMetaDataForClass(
            clazz,
            omfContext.getClassLoaderResolver(getClass().getClassLoader())),
        storeMgr.getIdentifierFactory());
  }

  protected ObjectManager getObjectManager() {
    return ((EntityManagerImpl)em).getObjectManager();
  }

  protected String kindForObject(Object obj) {
    return kindForClass(obj.getClass());
  }

  private boolean cacheManagers() {
    return !Boolean.valueOf(System.getProperty("do.not.cache.managers"));
  }

  interface StartEnd {
    void start();
    void end();
  }

  final StartEnd TXN_START_END = new StartEnd() {
    public void start() {
      beginTxn();
    }

    public void end() {
      commitTxn();
    }
  };

  final StartEnd NEW_EM_START_END = new StartEnd() {
    public void start() {
      if (!em.isOpen()) {
        em = emf.createEntityManager();
      }
    }

    public void end() {
      em.close();
    }
  };

  protected DatastoreManager getStoreManager() {
    return (DatastoreManager) getObjectManager().getStoreManager();
  }
}
