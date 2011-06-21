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
package com.google.appengine.datanucleus;

//import com.google.appengine.testing.cloudcover.util.CloudCoverLocalServiceTestHelper;

import com.google.appengine.datanucleus.jdo.DatastoreJDOPersistenceManagerFactory;
import com.google.appengine.datanucleus.jpa.DatastoreEntityManagerFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.apphosting.api.ApiProxy;
import junit.framework.TestCase;

import javax.jdo.spi.JDOImplHelper;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Base class for all tests that access the datastore.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public class DatastoreTestCase extends TestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalDatastoreServiceTestConfig());
    // TODO(maxr): Put this back once we have a maven project for cloudcover
//  private final CloudCoverLocalServiceTestHelper helper = new CloudCoverLocalServiceTestHelper(
//      new LocalDatastoreServiceTestConfig());

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    synchronized (JDOImplHelper.class) {
      Field f = JDOImplHelper.class.getDeclaredField("registeredClasses");
      f.setAccessible(true);
      Map map = (Map) f.get(null);
      if (!(map instanceof ThreadLocalMap)) {
        f.set(null, new ThreadLocalMap((Map) f.get(null)));
      }
    }
    synchronized (DatastoreJDOPersistenceManagerFactory.class) {
      System.setProperty(
          DatastoreJDOPersistenceManagerFactory.DISABLE_DUPLICATE_PMF_EXCEPTION_PROPERTY,
          Boolean.TRUE.toString());
    }
    synchronized (DatastoreEntityManagerFactory.class) {
      System.setProperty(DatastoreEntityManagerFactory.DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY,
                         Boolean.TRUE.toString());
    }
    helper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    // Do NOT clear out these properties.  System properties are global and if
    // we're running multiple tests concurrently, clearing them will cause other
    // tests to fail.  We want these properties set at all times so this is ok.
//    System.clearProperty(
//        DatastoreJDOPersistenceManagerFactory.DISABLE_DUPLICATE_PMF_EXCEPTION_PROPERTY);
//    System.clearProperty(DatastoreEntityManagerFactory.DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY);
    super.tearDown();
  }

  protected void setDelegateForThread(ApiProxy.Delegate delegate) {
    ApiProxy.setDelegate(delegate);
//    CloudCoverLocalServiceTestHelper.setDelegate(delegate);
  }

  protected ApiProxy.Delegate getDelegateForThread() {
    return ApiProxy.getDelegate();
//    return CloudCoverLocalServiceTestHelper.getDelegate();
  }

  /**
   * A bizarro custom map implementation that we inject into the jdo
   * implementation to get around a concurrent modification bug.
   * Methods that are supposed to return views instead return copies.  This is
   * non-standard but it addresses the concurrency issues.  We're only doing
   * this for tests so it's not a big deal. 
   */
  private static final class ThreadLocalMap implements Map {

    private final Map delegate;

    private ThreadLocalMap(Map delegate) {
      this.delegate = delegate;
    }

    public int size() {
      return delegate.size();
    }

    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    public boolean containsKey(Object o) {
      return delegate.containsKey(o);
    }

    public boolean containsValue(Object o) {
      return delegate.containsValue(o);
    }

    public Object get(Object o) {
      return delegate.get(o);
    }

    public synchronized Object put(Object o, Object o1) {
      return delegate.put(o, o1);
    }

    public synchronized Object remove(Object o) {
      return delegate.remove(o);
    }

    public synchronized void putAll(Map map) {
      delegate.putAll(map);
    }

    public synchronized void clear() {
      delegate.clear();
    }

    public synchronized Set keySet() {
      Set set = delegate.keySet();
      return new HashSet(set);
    }

    public synchronized Collection values() {
      Collection values = delegate.values();
      return new ArrayList(values);
    }

    public Set entrySet() {
      Set entries = delegate.entrySet();
      return new HashSet(entries);
    }

    public boolean equals(Object o) {
      return delegate.equals(o);
    }

    public int hashCode() {
      return delegate.hashCode();
    }
  }
}
