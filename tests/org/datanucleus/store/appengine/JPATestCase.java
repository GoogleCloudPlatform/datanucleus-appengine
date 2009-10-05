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

import com.google.appengine.api.datastore.Query;

import junit.framework.TestCase;

import org.datanucleus.ObjectManager;
import org.datanucleus.jpa.EntityManagerImpl;
import org.datanucleus.metadata.MetaDataManager;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPATestCase extends TestCase {

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
    boolean success = false;
    try {
      emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name());
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
      emf.close();
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
    emf.close();
    emf = Persistence.createEntityManagerFactory(name.name());
    em = emf.createEntityManager();
  }

  public int countForClass(Class<?> clazz) {
    String kind = kindForClass(clazz);
    return ldth.ds.prepare(new Query(kind)).countEntities();
  }

  protected String kindForClass(Class<?> clazz) {
    ObjectManager om = ((EntityManagerImpl)em).getObjectManager();
    MetaDataManager mdm = om.getMetaDataManager();
    return EntityUtils.determineKind(
        mdm.getMetaDataForClass(clazz, om.getClassLoaderResolver()), om);
  }

}
