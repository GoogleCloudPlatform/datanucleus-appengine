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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.HasTableAndColumnsInMappingJDO;


import java.util.List;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOTableAndColumnTest extends JDOTestCase {

  public void testInsert() throws EntityNotFoundException {
    HasTableAndColumnsInMappingJDO htacim = new HasTableAndColumnsInMappingJDO();
    htacim.setFoo("foo val");
    makePersistentInTxn(htacim, TXN_START_END);
    assertNotNull(htacim.getId());
    Entity entity = ds.get(KeyFactory.createKey(
        HasTableAndColumnsInMappingJDO.TABLE_NAME, htacim.getId()));
    assertNotNull(entity);
    assertEquals(HasTableAndColumnsInMappingJDO.TABLE_NAME, entity.getKind());
    assertEquals("foo val", entity.getProperty(HasTableAndColumnsInMappingJDO.FOO_COLUMN_NAME));
  }

  private void setupCustomIdentifierPolicy(
      String wordSeparator, String tablePrefix, String tableSuffix, String idCase) {
    pm.close();
    pmf.close();
    Properties properties = new Properties();
    properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
                    "org.datanucleus.jdo.JDOPersistenceManagerFactory");
    properties.setProperty("javax.jdo.option.ConnectionURL","appengine");
    properties.setProperty("datanucleus.NontransactionalRead", Boolean.TRUE.toString());
    properties.setProperty("datanucleus.NontransactionalWrite", Boolean.TRUE.toString());
    properties.setProperty("datanucleus.identifier.wordSeparator", wordSeparator);
    properties.setProperty("datanucleus.identifier.case", idCase);
    properties.setProperty("datanucleus.identifier.tablePrefix", tablePrefix);
    properties.setProperty("datanucleus.identifier.tableSuffix", tableSuffix);
    pmf = JDOHelper.getPersistenceManagerFactory(properties);
    pm = pmf.getPersistenceManager();
  }

  public void testInsertWithCustomIdPolicy() throws EntityNotFoundException {
    setupCustomIdentifierPolicy("___", "PREFIX", "SUFFIX", "UpperCase");

    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");
    f1.setFlightNumber(400);
    pm.makePersistent(f1);
    Entity entity = ds.get(KeyFactory.stringToKey(f1.getId()));
    assertNotNull(entity);
    assertEquals("PREFIX" + Flight.class.getSimpleName().toUpperCase() + "SUFFIX", entity.getKind());
    assertEquals("Harold", entity.getProperty("NAME"));
    // column name isn't generated if explicitly set
    assertEquals(400L, entity.getProperty("flight_number"));
  }

  public void testFetch() {
    Entity entity = new Entity(HasTableAndColumnsInMappingJDO.TABLE_NAME);
    entity.setProperty(HasTableAndColumnsInMappingJDO.FOO_COLUMN_NAME, "foo val");
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasTableAndColumnsInMappingJDO htacim =
        pm.getObjectById(HasTableAndColumnsInMappingJDO.class, keyStr);
    assertNotNull(htacim);
    assertEquals(Long.valueOf(key.getId()), htacim.getId());
    assertEquals("foo val", htacim.getFoo());
    commitTxn();
  }

  public void testQueryWithCustomIdPolicy() {
    setupCustomIdentifierPolicy("___", "PREFIX", "SUFFIX", "UpperCase");

    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");
    f1.setFlightNumber(400);
    pm.makePersistent(f1);

    Flight f2 = new Flight();
    f2.setOrigin("BOS");
    f2.setDest("MIA");
    f2.setMe(2);
    f2.setYou(4);
    f2.setName("Harold2");
    f2.setFlightNumber(401);
    pm.makePersistent(f2);

    Query q = pm.newQuery("select from " + Flight.class.getName()
        + " where flightNumber > 300 "
        + " order by flightNumber desc");
    @SuppressWarnings("unchecked")
    List<Flight> result = (List<Flight>) q.execute();
    assertEquals(2, result.size());
    assertEquals(f2, result.get(0));
    assertEquals(f1, result.get(1));

    q = pm.newQuery("select from " + Flight.class.getName()
        + " where name == \"Harold\" "
        + " order by name desc");
    @SuppressWarnings("unchecked")
    List<Flight> result2 = (List<Flight>) q.execute();
    assertEquals(1, result2.size());
    assertEquals(f1, result2.get(0));
  }
}
