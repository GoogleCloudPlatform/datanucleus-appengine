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

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJDOPersistenceManagerFactoryTest extends TestCase {

  public void testAllocationOfDuplicateNamedPMFAsSingleton() {
    PersistenceManagerFactory pmf1 = null;
    PersistenceManagerFactory pmf2 = null;
    try {
      pmf1 = JDOHelper.getPersistenceManagerFactory("nontransactional-singleton");

      pmf2 = JDOHelper.getPersistenceManagerFactory("nontransactional-singleton");

      assertTrue(pmf1 == pmf2);
    }
    catch (Exception e) {
      fail("Exception thrown during allocation of two PMFs with same name requiring singleton");
    }
    finally {
      if (pmf1 != pmf2) {
        if (pmf1 != null) {
          pmf1.close();
        }
        if (pmf2 != null) {
          pmf2.close();
        }
      }
      else {
        if (pmf1 != null) {
          pmf1.close();
        }
      }
    }
  }

  public void testAllocationOfDuplicateNamedPMF() {
    PersistenceManagerFactory pmf1 = null;
    PersistenceManagerFactory pmf2 = null;
    try {
      pmf1 = JDOHelper.getPersistenceManagerFactory("nontransactional");

      pmf2 = JDOHelper.getPersistenceManagerFactory("nontransactional");

      assertTrue(pmf1 != pmf2);
    }
    catch (Exception e) {
      fail("Exception thrown during allocation of two PMFs with same name");
    }
    finally {
      if (pmf1 != pmf2) {
        if (pmf1 != null) {
          pmf1.close();
        }
        if (pmf2 != null) {
          pmf2.close();
        }
      }
      else {
        if (pmf1 != null) {
          pmf1.close();
        }
      }
    }
  }
}
