/*
 * Copyright (C) 2009 Max Ross.
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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.store.appengine.JDOTestCase;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.test.Flight;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.jdo.Query;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JDOQLCursorTest extends JDOTestCase {

  public void testGetCursor_List() {
    Entity e1 = Flight.newFlightEntity("harold", "bos", "mia", 23, 24);
    Entity e2 = Flight.newFlightEntity("harold", "bos", "mia", 23, 24);
    Entity e3 = Flight.newFlightEntity("harold", "bos", "mia", 23, 24);
    ldth.ds.put(Arrays.asList(e1, e2, e3));

    Map<String, Object> extensionMap = Utils.newHashMap();

    beginTxn();
    Query q = pm.newQuery(Flight.class);
    q.setRange(0, 1);
    List<Flight> flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e1.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    Cursor c = JDOCursorHelper.getCursor(flights);
    assertNotNull(c);

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c);
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e2.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertNotNull(JDOCursorHelper.getCursor(flights));

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c.toWebSafeString());
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e2.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    c = JDOCursorHelper.getCursor(flights);
    assertNotNull(c);

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c);
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e3.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertNull(JDOCursorHelper.getCursor(flights));

    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, c.toWebSafeString());
    q.setExtensions(extensionMap);
    flights = (List<Flight>) q.execute();
    assertEquals(1, flights.size());
    assertEquals(e3.getKey(), KeyFactory.stringToKey(flights.get(0).getId()));
    assertNull(JDOCursorHelper.getCursor(flights));
    commitTxn();
  }
}
