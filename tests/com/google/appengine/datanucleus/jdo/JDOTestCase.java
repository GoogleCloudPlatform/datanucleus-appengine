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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.*;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastoreTestCase;
import com.google.appengine.datanucleus.EntityUtils;
import com.google.appengine.datanucleus.StorageVersion;
import com.google.appengine.datanucleus.Utils;

import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.ExecutionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

/**
 * Base testcase for tests that need a {@link PersistenceManagerFactory}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JDOTestCase extends DatastoreTestCase {

  public static final String PROP_DETACH_ON_CLOSE = "datanucleus.DetachOnClose";

  private static
  ThreadLocal<Map<PersistenceManagerFactoryName, PersistenceManagerFactory>> pmfCache =
      new ThreadLocal<Map<PersistenceManagerFactoryName, PersistenceManagerFactory>>() {
        @Override
        protected Map<PersistenceManagerFactoryName, PersistenceManagerFactory> initialValue() {
          return Utils.newHashMap();
        }
      };

  protected PersistenceManagerFactory pmf;
  protected PersistenceManager pm;

  protected DatastoreService ds;

  @Override
  protected void setUp() throws Exception {
    Map<String, String> props = new HashMap<String, String>();
    props.put("datanucleus.appengine.BigDecimalsEncoding", "String");
    super.setUp();
    ds = DatastoreServiceFactory.getDatastoreService();
    pmf = pmfCache.get().get(getPersistenceManagerFactoryName());
    if (pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(props, getPersistenceManagerFactoryName().name());
      if (cacheManagers()) {
        pmfCache.get().put(getPersistenceManagerFactoryName(), pmf);
      }
    }
    pm = pmf.getPersistenceManager();
  }

  public enum PersistenceManagerFactoryName { originalStorageVersion, transactional, nontransactional }

  /**
   * By default we use a datasource that requires txns.
   * Override this if your test needs to use a different instance.
   */
  protected PersistenceManagerFactoryName getPersistenceManagerFactoryName() {
    return PersistenceManagerFactoryName.transactional;
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      if (!pm.isClosed()) {
        if (pm.currentTransaction().isActive()) {
          pm.currentTransaction().rollback();
        }
        pm.close();
      }
      pm = null;
      // see if anybody closed any of our pmfs and if so just remove them from the cache -
      // we'll rebuild it the next time it's needed.
      for (Map.Entry<PersistenceManagerFactoryName, PersistenceManagerFactory> entry : pmfCache.get().entrySet()) {
        if (entry.getValue().isClosed()) {
          pmfCache.get().remove(entry.getKey());
        }
      }
      if (!cacheManagers() && !pmf.isClosed()) {
        pmf.close();
      }
      pmf = null;
    } finally {
      super.tearDown();
    }
  }

  protected DatastoreManager getDatastoreManagerForPM(PersistenceManager pm) {
    return (DatastoreManager) ((JDOPersistenceManager)pm).getExecutionContext().getStoreManager();
  }

  protected StorageVersion getStorageVersion(PersistenceManager pm) {
    return getDatastoreManagerForPM(pm).getStorageVersion();
  }

  protected void beginTxn() {
    pm.currentTransaction().begin();
  }

  protected void commitTxn() {
    pm.currentTransaction().commit();
  }

  protected void rollbackTxn() {
    pm.currentTransaction().rollback();
  }

  protected <T> T makePersistentInTxn(T obj, StartEnd startEnd) {
    boolean success = false;
    startEnd.start();
    try {
      pm.makePersistent(obj);
      startEnd.end();
      success = true;
    } finally {
      if (!success && pm.currentTransaction().isActive()) {
        rollbackTxn();
      }
    }
    return obj;
  }

  protected void switchDatasource(PersistenceManagerFactoryName name) {
    switchDatasource(name, null);
  }

  protected void switchDatasource(PersistenceManagerFactoryName name, Map<String, String> props) {
    if (!pm.isClosed()) {
      pm.close();
    }
    if (!cacheManagers() && !pmf.isClosed()) {
      pmf.close();
    }
    if (props == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(name.name());
    } else {
      pmf = JDOHelper.getPersistenceManagerFactory(props, name.name());      
    }
    pm = pmf.getPersistenceManager();
  }

  @SuppressWarnings("deprecation")
  protected int countForClass(Class<?> clazz) {
    String kind = kindForClass(clazz);
    return ds.prepare(
        new com.google.appengine.api.datastore.Query(kind)).countEntities();
  }

  protected void deleteAll() {
    List<Entity> entities = ds.prepare(new Query().setKeysOnly()).asList(FetchOptions.Builder.withDefaults());
    for (Entity entity : entities) {
      ds.delete(entity.getKey());
    }
  }

  protected String kindForClass(Class<?> clazz) {
    ExecutionContext om = getExecutionContext();
    MetaDataManager mdm = om.getMetaDataManager();
    return EntityUtils.determineKind(
        mdm.getMetaDataForClass(clazz, om.getClassLoaderResolver()), om);
  }

  protected String kindForObject(Object obj) {
    return kindForClass(obj.getClass());
  }

  protected ExecutionContext getExecutionContext() {
    return ((JDOPersistenceManager)pm).getExecutionContext();
  }

  private boolean cacheManagers() {
    return !Boolean.valueOf(System.getProperty("do.not.cache.managers"));
  }

  interface StartEnd {
    void start();
    void end();
    PersistenceManagerFactoryName getPmfName();
  }

  public final StartEnd TXN_START_END = new StartEnd() {
    public void start() {
      beginTxn();
    }

    public void end() {
      commitTxn();
    }

    public PersistenceManagerFactoryName getPmfName() {
      return PersistenceManagerFactoryName.transactional;
    }
  };

  public final StartEnd NEW_PM_START_END = new StartEnd() {
    public void start() {
      if (pm.isClosed()) {
        pm = pmf.getPersistenceManager();
      }
    }

    public void end() {
      pm.close();
    }

    public PersistenceManagerFactoryName getPmfName() {
      return PersistenceManagerFactoryName.nontransactional;
    }
  };

  protected DatastoreManager getStoreManager() {
    return (DatastoreManager) getExecutionContext().getStoreManager();
  }
}
