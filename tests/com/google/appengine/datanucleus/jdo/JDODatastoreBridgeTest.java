/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterable;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.datanucleus.JDODatastoreBridge;
import com.google.appengine.datanucleus.test.jdo.Flight;


import java.util.List;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JDODatastoreBridgeTest extends JDOTestCase {

  public void testConvertQueryResultList_Empty() {
    QueryResultList<Entity> result =
        ds.prepare(new Query("blarg")).asQueryResultList(FetchOptions.Builder.withLimit(10));

    JDODatastoreBridge bridge = new JDODatastoreBridge();
    List<Flight> flights = bridge.toJDOResult(pm, Flight.class, result);
    assertEquals(0, flights.size());
  }

  public void testConvertQueryResultList() {
    for (int i = 0; i < 5; i++) {
      Entity e = Flight.newFlightEntity("harold" + i, "bos", "mia", 23, 24);
      ds.put(e);
    }
    QueryResultList<Entity> result =
        ds.prepare(new Query("Flight")).asQueryResultList(FetchOptions.Builder.withLimit(10));

    JDODatastoreBridge bridge = new JDODatastoreBridge();
    List<Flight> flights = bridge.toJDOResult(pm, Flight.class, result);
    assertEquals(5, flights.size());
    String id = flights.get(0).getId();
    // make sure these flights are connected
    beginTxn();
    flights.get(0).setOrigin("lax");
    commitTxn();
    beginTxn();
    Flight f = pm.getObjectById(Flight.class, id);
    assertEquals("lax", f.getOrigin());
    commitTxn();
    deleteAll();
  }

  public void testConvertQueryResultIterable() {
    for (int i = 0; i < 5; i++) {
      Entity e = Flight.newFlightEntity("harold" + i, "bos", "mia", 23, 24);
      ds.put(e);
    }
    QueryResultIterable<Entity> result =
        ds.prepare(new Query("Flight")).asQueryResultIterable();

    JDODatastoreBridge bridge = new JDODatastoreBridge();
    List<Flight> flights = bridge.toJDOResult(pm, Flight.class, result);
    assertEquals(5, flights.size());
    String id = flights.get(0).getId();
    // make sure these flights are connected
    beginTxn();
    flights.get(0).setOrigin("lax");
    commitTxn();
    beginTxn();
    Flight f = pm.getObjectById(Flight.class, id);
    assertEquals("lax", f.getOrigin());
    commitTxn();
    deleteAll();
  }

  public void testAccessResultsAfterClose() {
    for (int i = 0; i < 3; i++) {
      Entity e = Flight.newFlightEntity("this", "bos", "mia", 24, 25);
      ds.put(e);
    }
    QueryResultIterable<Entity> result =
        ds.prepare(new Query("Flight")).asQueryResultIterable();
    beginTxn();
    JDODatastoreBridge bridge = new JDODatastoreBridge();
    List<Flight> flights = bridge.toJDOResult(pm, Flight.class, result);
    commitTxn();
    pm.close();
    assertEquals(3, flights.size());
    deleteAll();
  }

}