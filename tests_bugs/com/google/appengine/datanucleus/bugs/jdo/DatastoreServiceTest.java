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
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.util.NucleusLogger;

/**
 * Test sample to try things out with DatastoreService since GAE docs for low-level API are incomplete
 * and we often have to guess at restrictions.
 */
public class DatastoreServiceTest extends JDOBugTestCase {

  /**
   * Test to simulate the insert of 1-1 bidir relation, owner-child, with both using IDENTITY strategy.
   */
  public void testOneToOneBidirWithIdentity() throws Exception {
    Transaction txn = ds.beginTransaction();

    // 1. Insert parent without relation to get parentKey
    Entity entity1 = new Entity("Issue174");
    entity1.setProperty("name", "Parent Object");
    ds.put(txn, entity1);
    Key parentKey = entity1.getKey();

    // 2. Insert child with parentKey
    Entity entity2 = new Entity("Issue175", parentKey);
    entity2.setProperty("name", "Child Object");
    ds.put(txn, entity2);
    Key childKey = entity2.getKey();

    // 3. Insert parent with childKey property
    entity1.setProperty("child_id_oid", childKey);
    ds.put(txn, entity1);
    txn.commit();

    // Check what was stored
    DatastoreServiceConfig cfg1 = DatastoreServiceConfig.Builder.withDeadline(100.0);
    DatastoreService ds1 = DatastoreServiceFactory.getDatastoreService(cfg1);

    NucleusLogger.GENERAL.info(">> ds.get(parentKey)");
    Entity ent1 = ds1.get(parentKey);
    NucleusLogger.GENERAL.info(">> ent1=" + ent1);

    NucleusLogger.GENERAL.info(">> ds.get(childKey)");
    Entity ent2 = ds1.get(childKey);
    NucleusLogger.GENERAL.info(">> ent2=" + ent2);
  }
}
