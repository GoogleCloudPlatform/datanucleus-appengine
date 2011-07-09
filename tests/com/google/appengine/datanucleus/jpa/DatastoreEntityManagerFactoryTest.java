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

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import static com.google.appengine.datanucleus.jpa.JPATestCase.EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_allowed;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreEntityManagerFactoryTest extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    DatastoreEntityManagerFactory.getNumInstancesPerPersistenceUnit().clear();
  }


  public void testExceptionOnDuplicateEMF() {
    // we're mucking with system properties.  in order to stay thread-safe
    // we need to prevent other tests that muck with this system property from
    // running at the same time
    synchronized (DatastoreEntityManagerFactory.class) {
      boolean propertyWasSet = System.getProperties().containsKey(
          DatastoreEntityManagerFactory.DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY);
      System.clearProperty(
          DatastoreEntityManagerFactory.DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY);
      try {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(
            nontransactional_ds_non_transactional_ops_allowed.name());
        // Close it so we can verify that closing doesn't affect our exception
        // throwing.
        emf.close();

        // Now allocate another with the same name
        try {
          Persistence.createEntityManagerFactory(
              nontransactional_ds_non_transactional_ops_allowed.name());
          fail("expected exception");
        } catch (PersistenceException e) {
          // good
          assertTrue(e.getCause() instanceof IllegalStateException);
        }
      } finally {
        if (propertyWasSet) {
          System.setProperty(
              DatastoreEntityManagerFactory.DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY,
              Boolean.TRUE.toString());
        }
      }
    }
  }
}