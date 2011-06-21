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
package com.google.appengine.datanucleus.query;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.JDOTestCase;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.HasKeyAncestorKeyPkJDO;
import com.google.appengine.datanucleus.test.HasOneToManyListJDO;


import javax.jdo.JDOUserException;
import javax.jdo.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOQLDeleteTest extends JDOTestCase {

  public void testDelete_Txn_MultipleEntityGroups() {

    ds.put(Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24, 25));
    ds.put(Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24, 25));

    Query q = pm.newQuery(Flight.class);
    beginTxn();
    try {
      q.deletePersistentAll();
      fail("expected exception");
    } catch (JDOUserException e) {  // datanuc bug, should be throwing a fatal exception
      // good - can't delete books from multiple entity groups in a txn
    }
    rollbackTxn();
    assertEquals(2, countForClass(Flight.class));
  }

  public void testDelete_Txn_OneEntityGroup() {

    Key parentKey = KeyFactory.createKey("yar", 23);
    ds.put(Flight.newFlightEntity(parentKey, null, "jimmy", "bos", "mia", 23, 24, 25));
    ds.put(Flight.newFlightEntity(parentKey, null, "jimmy", "bos", "mia", 23, 24, 25));

    Query q = pm.newQuery(Flight.class);
    beginTxn();
    assertEquals(2, q.deletePersistentAll());
    assertEquals(2, countForClass(Flight.class));
    commitTxn();
    assertEquals(0, countForClass(Flight.class));
  }

  public void testDelete_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    ds.put(Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24));
    ds.put(Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24));

    Query q = pm.newQuery(Flight.class);
    assertEquals(2, q.deletePersistentAll());
    assertEquals(0, countForClass(Flight.class));
  }

  public void testDeleteAncestorQuery_Txn() {
    Key parentKey = KeyFactory.createKey("yar", 23);
    Entity pojo1 = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey);
    Entity pojo2 = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey);

    ds.put(pojo1);
    ds.put(pojo2);

    Query q = pm.newQuery(HasKeyAncestorKeyPkJDO.class, "ancestorKey == :p1");
    beginTxn();
    assertEquals(2, q.deletePersistentAll(parentKey));
    commitTxn();
    assertEquals(0, countForClass(HasKeyAncestorKeyPkJDO.class));
  }

  public void testDeleteAncestorQuery_TxnRollback() throws EntityNotFoundException {
    Key parentKey = KeyFactory.createKey("yar", 23);
    Entity pojo1 = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey);
    Entity pojo2 = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey);

    ds.put(pojo1);
    ds.put(pojo2);

    Query q = pm.newQuery(HasKeyAncestorKeyPkJDO.class, "ancestorKey == :p1");
    beginTxn();
    assertEquals(2, q.deletePersistentAll(parentKey));
    rollbackTxn();
    assertEquals(2, countForClass(HasKeyAncestorKeyPkJDO.class));
  }

  public void testDeleteAncestorQuery_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Key parentKey = KeyFactory.createKey("yar", 23);
    Entity pojo1 = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey);
    Entity pojo2 = new Entity(HasKeyAncestorKeyPkJDO.class.getSimpleName(), parentKey);

    ds.put(pojo1);
    ds.put(pojo2);

    Query q = pm.newQuery(HasKeyAncestorKeyPkJDO.class, "ancestorKey == :p1");
    assertEquals(2, q.deletePersistentAll(parentKey));
    assertEquals(0, countForClass(HasKeyAncestorKeyPkJDO.class));
  }

  public void testBatchDelete_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity e1 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ds.put(e1);
    Entity e2 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ds.put(e2);
    Entity e3 = Flight.newFlightEntity("jimmy", "bos", "mia", 23, 24);
    ds.put(e3);

    Key key = KeyFactory.createKey("yar", "does not exist");
    Query q = pm.newQuery(Flight.class, "id == :ids");
    assertEquals(2, q.deletePersistentAll(Utils.newArrayList(key, e1.getKey(), e2.getKey())));
    assertEquals(1, countForClass(Flight.class));
  }

  public void testBatchDelete_Txn() {
    Key parent = KeyFactory.createKey("yar", 23);
    Entity e1 = Flight.newFlightEntity(parent, null, "jimmy", "bos", "mia", 23, 24, 25);
    ds.put(e1);
    Entity e2 = Flight.newFlightEntity(parent, null, "jimmy", "bos", "mia", 23, 24, 25);
    ds.put(e2);
    Entity e3 = Flight.newFlightEntity(parent, null, "jimmy", "bos", "mia", 23, 24, 25);
    ds.put(e3);

    beginTxn();
    Query q = pm.newQuery(Flight.class, "id == :ids");
    assertEquals(2, q.deletePersistentAll(Utils.newArrayList(parent, e1.getKey(), e2.getKey())));
    assertEquals(3, countForClass(Flight.class));
    commitTxn();
    assertEquals(1, countForClass(Flight.class));
  }

  public void testDeleteCascades() {
    HasOneToManyListJDO parent = new HasOneToManyListJDO();
    Flight f = new Flight();
    parent.getFlights().add(f);
    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    assertEquals(1, countForClass(Flight.class));
    assertEquals(1, countForClass(HasOneToManyListJDO.class));
    beginTxn();
    Query q = pm.newQuery(HasOneToManyListJDO.class);
    assertEquals(1, q.deletePersistentAll());
    assertEquals(1, countForClass(Flight.class));
    assertEquals(1, countForClass(HasOneToManyListJDO.class));
    commitTxn();
    assertEquals(0, countForClass(Flight.class));
    assertEquals(0, countForClass(HasOneToManyListJDO.class));
  }
}