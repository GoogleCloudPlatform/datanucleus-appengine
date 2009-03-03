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
package org.datanucleus.store.appengine.jdo;

import junit.framework.TestCase;

import static org.datanucleus.store.appengine.JDOTestCase.PersistenceManagerFactoryName.nontransactional;

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJDOPersistenceManagerFactoryTest extends TestCase {

  public void testExceptionOnDuplicatePMF() {
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
