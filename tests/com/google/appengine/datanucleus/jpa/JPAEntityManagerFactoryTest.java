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

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAEntityManagerFactoryTest extends TestCase {

  public void testAllocationOfDuplicateNamedEMFAsSingleton() {
    EntityManagerFactory emf1 = null;
    EntityManagerFactory emf2 = null;
    try {
      emf1 = Persistence.createEntityManagerFactory("nontransactional_ds_non_transactional_ops_allowed_singleton");

      emf2 = Persistence.createEntityManagerFactory("nontransactional_ds_non_transactional_ops_allowed_singleton");

      assertTrue(emf1 == emf2);
    }
    catch (Exception e) {
      fail("Exception thrown during allocation of two EMFs with same name requiring singleton");
    }
    finally {
      if (emf1 != emf2) {
        if (emf1 != null) {
          emf1.close();
        }
        if (emf2 != null) {
          emf2.close();
        }
      }
      else {
        if (emf1 != null) {
          emf1.close();
        }
      }
    }
  }

  public void testAllocationOfDuplicateNamedEMF() {
    EntityManagerFactory emf1 = null;
    EntityManagerFactory emf2 = null;
    try {
      emf1 = Persistence.createEntityManagerFactory("nontransactional_ds_non_transactional_ops_allowed");

      emf2 = Persistence.createEntityManagerFactory("nontransactional_ds_non_transactional_ops_allowed");

      assertTrue(emf1 != emf2);
    }
    catch (Exception e) {
      fail("Exception thrown during allocation of two EMFs with same name requiring singleton");
    }
    finally {
      if (emf1 != emf2) {
        if (emf1 != null) {
          emf1.close();
        }
        if (emf2 != null) {
          emf2.close();
        }
      }
      else {
        if (emf1 != null) {
          emf1.close();
        }
      }
    }
  }
}