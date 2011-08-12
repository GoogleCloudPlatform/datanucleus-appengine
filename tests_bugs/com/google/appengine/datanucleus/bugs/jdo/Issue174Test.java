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
package com.google.appengine.datanucleus.bugs.jdo;

import javax.jdo.PersistenceManager;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.test.Issue174Entity;

public class Issue174Test extends JDOBugTestCase {

  public void testSimultaneousTransactionsSameThread() {
    // Create base data of object1, object2
    Issue174Entity ent1 = new Issue174Entity();
    pm.makePersistent(ent1);
    Object id1 = pm.getObjectId(ent1);
    pm.close();
    pm = pmf.getPersistenceManager();
    Issue174Entity ent2 = new Issue174Entity();
    pm.makePersistent(ent2);
    Object id2 = pm.getObjectId(ent2);
    pm.close();

    // Start first PM and retrieve the first object in a transaction
    PersistenceManager pm1 = pmf.getPersistenceManager();
    try {
      NucleusLogger.GENERAL.info(">> PM1 : txn.begin");
      pm1.currentTransaction().begin();
      NucleusLogger.GENERAL.info(">> PM1(TXN) : getObjectById");
      pm1.getObjectById(id1);

      // Start second PM and retrieve the second object in a transaction
      PersistenceManager pm2 = pmf.getPersistenceManager();
      try {
        pm2.currentTransaction().begin();
        // GAE/J v1 threw an exception here
        NucleusLogger.GENERAL.info(">> PM2 : getObjectById");
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
