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
package com.google.appengine.datanucleus.bugs.jpa;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.bugs.DatastoreTestCase;
import com.google.appengine.datanucleus.EntityUtils;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jpa.JPAEntityManager;
import org.datanucleus.api.jpa.JPAEntityManagerFactory;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.mapped.MappedStoreManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPATestCase extends DatastoreTestCase {

  private static
  ThreadLocal<Map<EntityManagerFactoryName, EntityManagerFactory>> emfCache =
      new ThreadLocal<Map<EntityManagerFactoryName, EntityManagerFactory>>() {
        @Override
        protected Map<EntityManagerFactoryName, EntityManagerFactory> initialValue() {
          // this shouldn't be necessary but I get concurrent mod exceptions
          return new ConcurrentHashMap<EntityManagerFactoryName, EntityManagerFactory>();
        }
      };

  protected EntityManagerFactory emf;
  protected EntityManager em;

  protected DatastoreService ds;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ds = DatastoreServiceFactory.getDatastoreService();
    emf = emfCache.get().get(getEntityManagerFactoryName());
    if (emf == null || !emf.isOpen()) {
      emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name());
      if (cacheManagers()) {
        emfCache.get().put(getEntityManagerFactoryName(), emf);
      }
    }
    em = emf.createEntityManager();
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
    try {
      if (em.isOpen()) {
        if (em.getTransaction().isActive()) {
          em.getTransaction().rollback();
        }
        em.close();
      }
      em = null;
      // see if anybody closed any of our emfs and if so just remove them from the cache -
      // we'll rebuild it the next time it's needed.
      for (Map.Entry<EntityManagerFactoryName, EntityManagerFactory> entry : emfCache.get().entrySet()) {
        if (!entry.getValue().isOpen()) {
          emfCache.get().remove(entry.getKey());
        }
      }
      if (!cacheManagers() && emf.isOpen()) {
        emf.close();
      }
      emf = null;
    } finally {
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
    if (em.getTransaction().isActive()) {
      em.getTransaction().rollback();
    }
  }

  protected void switchDatasource(EntityManagerFactoryName name) {
    switchDatasource(name, null);
  }

  protected void switchDatasource(EntityManagerFactoryName name, Map<String, String> props) {
    em.close();
    if (!cacheManagers() && emf.isOpen()) {
      emf.close();
    }
    if (props == null) {
      emf = Persistence.createEntityManagerFactory(name.name());
    } else {
      emf = Persistence.createEntityManagerFactory(name.name(), props);
    }
    em = emf.createEntityManager();
  }

  public int countForClass(Class<?> clazz) {
    String kind = kindForClass(clazz);
    return ds.prepare(new Query(kind)).countEntities();
  }

  protected String kindForClass(Class<?> clazz) {
    NucleusContext nucContext = ((JPAEntityManagerFactory)emf).getNucleusContext();
    MetaDataManager mdm = nucContext.getMetaDataManager();
    MappedStoreManager storeMgr = (MappedStoreManager) nucContext.getStoreManager();
    ClassLoaderResolver clr = nucContext.getClassLoaderResolver(getClass().getClassLoader());
    return EntityUtils.determineKind(
        mdm.getMetaDataForClass(
            clazz,
            nucContext.getClassLoaderResolver(getClass().getClassLoader())),
            storeMgr,
            clr);
  }

  protected ExecutionContext getExecutionContext() {
    return ((JPAEntityManager)em).getExecutionContext();
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
    return (DatastoreManager) getExecutionContext().getStoreManager();
  }
}
