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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.util.NucleusLogger;

/**
 * Example of how the low-level datastore can operate with transactions.
 */
public class Issue174TransactionTest extends JDOBugTestCase {

  public void testTransactions() {
    Entity entity1 = new Entity("Issue174");
    ds.put(entity1);
    Key key1 = entity1.getKey();
    Entity entity2 = new Entity("Issue175");
    ds.put(entity2);
    Key key2 = entity2.getKey();

    DatastoreServiceConfig cfg1 = DatastoreServiceConfig.Builder.withDeadline(100.0);
    DatastoreService ds1 = DatastoreServiceFactory.getDatastoreService(cfg1);
    NucleusLogger.GENERAL.info(">> PM1 cfg1=" + cfg1 + " ds1=" + ds1);
    Transaction txn1 = ds1.beginTransaction();
    NucleusLogger.GENERAL.info(">> PM1 ds1.beginTransaction txn1=" + txn1);
    try {
      NucleusLogger.GENERAL.info(">> PM1 ds1.get(txn, key)");
      Entity ent1 = ds1.get(txn1, key1);
      NucleusLogger.GENERAL.info(">> PM1 ent1=" + ent1);
    } catch (EntityNotFoundException enfe) {
      NucleusLogger.GENERAL.info(">> ds1 EntityNotFound");
    }

    DatastoreServiceConfig cfg2 = DatastoreServiceConfig.Builder.withDeadline(100.0);
    DatastoreService ds2 = DatastoreServiceFactory.getDatastoreService(cfg2);
    NucleusLogger.GENERAL.info(">> PM2 cfg2=" + cfg2 + " ds2=" + ds2);
    Transaction txn2 = ds2.beginTransaction();
    NucleusLogger.GENERAL.info(">> PM1 ds1.beginTransaction txn2=" + txn2);
    try {
      NucleusLogger.GENERAL.info(">> PM2 ds2.get(key)");
      Entity ent2 = ds2.get(txn2, key2);
      NucleusLogger.GENERAL.info(">> PM2 ent2=" + ent2);
    } catch (EntityNotFoundException enfe) {
      NucleusLogger.GENERAL.info(">> ds2 EntityNotFound");
    }

    txn1.commit();
    txn2.commit();
  }
}
