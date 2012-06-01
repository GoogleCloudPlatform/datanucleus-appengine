/**********************************************************************
Copyright (c) 2012 Google Inc.

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

import java.util.Map;

import javax.jdo.JDOHelper;

import com.google.appengine.datanucleus.test.jdo.Flight;
import com.google.appengine.datanucleus.test.jdo.HasOneToManyMapJDO;

/**
 * Some simple tests where we have a Map field in the persistable object.
 */
public class JDOMapTest extends JDOTestCase {

  public void testInsert() {
    HasOneToManyMapJDO pojo = new HasOneToManyMapJDO();
    pojo.setVal("First");
    Flight fl = new Flight("LHR", "CHI", "BA201", 1, 15);
    pojo.addFlight(fl.getName(), fl);
    Flight fl2 = new Flight("BCN", "MAD", "IB3311", 2, 26);
    pojo.addFlight(fl2.getName(), fl2);
    pojo.addBasicEntry(1, "First Entry");
    pojo.addBasicEntry(2, "Second Entry");

    Object id = null;
    beginTxn();
    pm.makePersistent(pojo);
    pm.flush();
    id = JDOHelper.getObjectId(pojo);
    commitTxn();
    pm.evictAll(); // Make sure not cached

    beginTxn();
    HasOneToManyMapJDO pojoRead = (HasOneToManyMapJDO)pm.getObjectById(id);

    Map<String, Flight> map1 = pojoRead.getFlightsByName();
    assertNotNull("Map<String,Flight> is null!", map1);
    assertEquals(2, map1.size());
    assertTrue(map1.containsKey("BA201"));
    assertTrue(map1.containsKey("IB3311"));

    Map<Integer, String> map2 = pojoRead.getBasicMap();
    assertNotNull("Map<Integer,String> is null!", map2);
    assertEquals(2, map2.size());
    assertTrue(map2.containsKey(1));
    assertTrue(map2.containsKey(2));

    commitTxn();
  }
}
