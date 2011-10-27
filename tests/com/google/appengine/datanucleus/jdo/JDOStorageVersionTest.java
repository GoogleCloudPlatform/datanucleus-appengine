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
package com.google.appengine.datanucleus.jdo;

import java.util.Map;

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;

import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.StorageVersion;
import com.google.appengine.datanucleus.Utils;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JDOStorageVersionTest extends JDOTestCase {

  public void testDefaultStorageVersion() {
    DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
    assertEquals(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS, storeMgr.getStorageVersion());
  }

  public void testNonDefaultStorageVersion() {
    pm.close();
    pmf.close();
    Map<String, String> props = Utils.newHashMap();
    props.put(StorageVersion.STORAGE_VERSION_PROPERTY, StorageVersion.PARENTS_DO_NOT_REFER_TO_CHILDREN.name());
    pmf = JDOHelper.getPersistenceManagerFactory(props, getPersistenceManagerFactoryName().name());
    pm = pmf.getPersistenceManager();
    DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
    assertEquals(StorageVersion.PARENTS_DO_NOT_REFER_TO_CHILDREN, storeMgr.getStorageVersion());
  }

  public void testUnknownStorageVersion() {
    pm.close();
    pmf.close();
    Map<String, String> props = Utils.newHashMap();
    props.put(StorageVersion.STORAGE_VERSION_PROPERTY, "does not exist");
    try {
      pmf = JDOHelper.getPersistenceManagerFactory(props, getPersistenceManagerFactoryName().name());
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause().getMessage().startsWith("'does not exist'"));
    }
  }
}
