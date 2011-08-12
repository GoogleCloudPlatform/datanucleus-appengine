/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import com.google.appengine.datanucleus.test.Flight;

import javax.jdo.PersistenceManager;

import org.datanucleus.util.NucleusLogger;

/**
 * Tests that really would be nice to put in JDOTransactionTest but that don't want interception of 
 * transaction calls.
 */
public class JDOTransactionBehaviourTest extends JDOTestCase {

  /**
   * Test where we have 2 PMs in the same thread and we start a transaction on each, with a different
   * entity-group on each. Tests that the transactions are indeed isolated.
   * Note that if we did the second PM as non-transactional then this would fail due to GAE/J datastore nonsense
   * about implicit transactions, with the non-transactional call being dumped into the other transaction.
   */
  public void testInterlacedTransactions() {
    // Create base data of object1, object2
    Flight fl1 = new Flight("LHR", "CHI", "BA201", 1, 2);
    pm.makePersistent(fl1);
    Object id1 = pm.getObjectId(fl1);
    pm.close();
    pm = pmf.getPersistenceManager();
    Flight fl2 = new Flight("SYD", "HKG", "QA176", 7, 9);
    pm.makePersistent(fl2);
    Object id2 = pm.getObjectId(fl2);
    pm.close();

    // Start first PM and retrieve the first object in a transaction
    PersistenceManager pm1 = pmf.getPersistenceManager();
    try {
      pm1.currentTransaction().begin();
      pm1.getObjectById(id1);

      // Start second PM and retrieve the second object in a transaction
      PersistenceManager pm2 = pmf.getPersistenceManager();
      try {
        pm2.currentTransaction().begin();
        // GAE/J v1 threw an exception here
        pm2.getObjectById(id2);
      }
      catch (Exception e) {
        NucleusLogger.GENERAL.error("Exception caught", e);
        fail("Should have allowed simultaneous PM transactions but didnt : caught exception - " + e.getMessage());
      }
      finally {
        pm2.currentTransaction().commit();
        pm2.close();
      }
    }
    finally {
      pm1.currentTransaction().commit();
      pm1.close();
    }
  }
}
