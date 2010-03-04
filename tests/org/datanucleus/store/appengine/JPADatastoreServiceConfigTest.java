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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.ReadPolicy;

import java.util.Map;

import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPADatastoreServiceConfigTest extends JPATestCase {

  public void testDefaultStorageVersion() {
    DatastoreManager storeMgr = (DatastoreManager) getObjectManager().getStoreManager();
    DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfig();
    DatastoreServiceConfig defaultConfig = DatastoreServiceConfig.Builder.withDefaults();
    assertEquals(defaultConfig.getDeadline(), config.getDeadline());
    assertEquals(defaultConfig.getReadPolicy(), config.getReadPolicy());
  }

  public void testNonDefaultStorageVersion() {
    em.close();
    emf.close();
    Map<String, String> props = Utils.newHashMap();
    props.put(DatastoreManager.DEFAULT_DATASTORE_DEADLINE_PROPERTY, "3.34");
    props.put(DatastoreManager.DEFAULT_DATASTORE_READ_CONSISTENCY_PROPERTY, ReadPolicy.Consistency.EVENTUAL.name());
    emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name(), props);
    em = emf.createEntityManager();
    DatastoreManager storeMgr = (DatastoreManager) getObjectManager().getStoreManager();
    DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfig();
    assertEquals(3.34d, config.getDeadline());
    assertEquals(new ReadPolicy(ReadPolicy.Consistency.EVENTUAL), config.getReadPolicy());
  }

  public void testUnknownReadPolicy() {
    em.close();
    emf.close();
    Map<String, String> props = Utils.newHashMap();
    props.put(DatastoreManager.DEFAULT_DATASTORE_READ_CONSISTENCY_PROPERTY, "dne");
    try {
      emf = Persistence.createEntityManagerFactory(getEntityManagerFactoryName().name(), props);
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause().getMessage().startsWith("Illegal value for"));
    }
  }
}