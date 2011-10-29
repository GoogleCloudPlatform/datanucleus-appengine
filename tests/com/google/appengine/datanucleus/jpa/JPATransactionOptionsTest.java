/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.Utils;

import java.util.Map;

import javax.persistence.Persistence;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPATransactionOptionsTest extends JPATestCase {

  public void testDefault() {
    DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
    TransactionOptions txnOpts = storeMgr.getDefaultDatastoreTransactionOptions();
    assertFalse(txnOpts.isXG());
  }

  public void testIsXG_Props() {
    em.close();
    emf.close();
    Map<String, String> props = Utils.newHashMap();
    props.put("datanucleus.appengine.datastoreEnableXGTransactions", Boolean.TRUE.toString());
    emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name(), props);
    em = emf.createEntityManager();
    DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
    TransactionOptions txnOpts = storeMgr.getDefaultDatastoreTransactionOptions();
    assertTrue(txnOpts.isXG());
  }

  public void testIsNotXG_Props() {
    em.close();
    emf.close();
    Map<String, String> props = Utils.newHashMap();
    props.put("datanucleus.appengine.datastoreEnableXGTransactions", Boolean.FALSE.toString());
    emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name(), props);
    em = emf.createEntityManager();
    DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
    TransactionOptions txnOpts = storeMgr.getDefaultDatastoreTransactionOptions();
    assertFalse(txnOpts.isXG());
  }

  public void testIsXG_Config() {
    em.close();
    emf.close();
    emf = Persistence.createEntityManagerFactory("allowXGTxns");
    em = emf.createEntityManager();
    DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
    TransactionOptions txnOpts = storeMgr.getDefaultDatastoreTransactionOptions();
    assertTrue(txnOpts.isXG());
  }

  public void testIsNotXG_Config() {
    em.close();
    emf.close();
    emf = Persistence.createEntityManagerFactory("disallowXGTxns");
    em = emf.createEntityManager();
    DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
    TransactionOptions txnOpts = storeMgr.getDefaultDatastoreTransactionOptions();
    assertFalse(txnOpts.isXG());
  }
}
