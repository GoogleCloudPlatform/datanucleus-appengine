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

import junit.framework.TestCase;

import com.google.appengine.datanucleus.Utils;

import java.util.Map;

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

import static com.google.appengine.datanucleus.jdo.JDOTestCase.PersistenceManagerFactoryName.nontransactional;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJDOPersistenceManagerFactoryTest extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DatastoreJDOPersistenceManagerFactory.getNumInstancesPerPersistenceUnit().clear();
  }

  public void testExceptionOnDuplicatePMF() {
    // we're mucking with system properties.  in order to stay thread-safe
    // we need to prevent other tests that muck with this system property from
    // running at the same time
    synchronized (DatastoreJDOPersistenceManagerFactory.class) {
      boolean propertyWasSet = System.getProperties().containsKey(
          DatastoreJDOPersistenceManagerFactory.DISABLE_DUPLICATE_PMF_EXCEPTION_PROPERTY);
      System.clearProperty(
          DatastoreJDOPersistenceManagerFactory.DISABLE_DUPLICATE_PMF_EXCEPTION_PROPERTY);
      try {
        PersistenceManagerFactory pmf =
            JDOHelper.getPersistenceManagerFactory(nontransactional.name());
        // Close it so we can verify that closing doesn't affect our exception
        // throwing.
        pmf.close();

        // Now allocate another with the same name
        try {
          JDOHelper.getPersistenceManagerFactory(nontransactional.name());
          fail("expected exception");
        } catch (JDOFatalUserException e) {
          // good
        }
      } finally {
        if (propertyWasSet) {
          System.setProperty(
              DatastoreJDOPersistenceManagerFactory.DISABLE_DUPLICATE_PMF_EXCEPTION_PROPERTY,
              Boolean.TRUE.toString());
        }
      }
    }
  }

  public void testPMFWithoutName() {
    Map<String, String> propMap = Utils.newHashMap();
    propMap.put("javax.jdo.PersistenceManagerFactoryClass",
                DatastoreJDOPersistenceManagerFactory.class.getName());
    propMap.put("javax.jdo.option.ConnectionURL", "appengine");
    PersistenceManagerFactory pmf =
        JDOHelper.getPersistenceManagerFactory(propMap);
    pmf.close();
    // Make sure our duplicate detection is disabled for pmfs that don't have
    // names.
    pmf = JDOHelper.getPersistenceManagerFactory(propMap);
    pmf.close();
  }
}
