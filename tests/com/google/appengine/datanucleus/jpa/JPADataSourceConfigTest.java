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
package com.google.appengine.datanucleus.jpa;

import junit.framework.TestCase;

import org.datanucleus.api.jpa.JPAEntityManager;

import com.google.appengine.datanucleus.DatastoreManager;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPADataSourceConfigTest extends TestCase {

  public void testTransactionalEMF() {
    EntityManagerFactory emf = Persistence.createEntityManagerFactory(
            JPATestCase.EntityManagerFactoryName.transactional_ds_non_transactional_ops_not_allowed.name());
    JPAEntityManager em = (JPAEntityManager) emf.createEntityManager();
    DatastoreManager storeMgr = (DatastoreManager) em.getExecutionContext().getStoreManager();
    assertTrue(storeMgr.connectionFactoryIsAutoCreateTransaction());
    em.close();
    emf.close();
  }

  public void testNonTransactionalEMF() {
    EntityManagerFactory emf = Persistence.createEntityManagerFactory(
            JPATestCase.EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed.name());
    JPAEntityManager em = (JPAEntityManager) emf.createEntityManager();
    DatastoreManager storeMgr = (DatastoreManager) em.getExecutionContext().getStoreManager();
    assertFalse(storeMgr.connectionFactoryIsAutoCreateTransaction());
    em.close();
    emf.close();
  }



}