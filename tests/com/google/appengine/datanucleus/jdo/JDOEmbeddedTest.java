/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
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
 * **********************************************************************/
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.HasEmbeddedJDO;
import com.google.appengine.datanucleus.test.HasEmbeddedPc;
import com.google.appengine.datanucleus.test.HasEmbeddedWithKeyPkJDO;
import com.google.appengine.datanucleus.test.HasKeyPkJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOEmbeddedTest extends JDOTestCase {

  public void testEmbeddedWithGeneratedId() throws EntityNotFoundException {
    HasEmbeddedJDO pojo = new HasEmbeddedJDO();
    Flight f = new Flight();
    f.setId("yarg");
    f.setFlightNumber(23);
    f.setName("harold");
    f.setOrigin("bos");
    f.setDest("mia");
    f.setYou(24);
    f.setMe(25);
    pojo.setFlight(f);

    Flight f2 = new Flight();
    f2.setId("blarg");
    f2.setFlightNumber(26);
    f2.setName("jimmy");
    f2.setOrigin("jfk");
    f2.setDest("sea");
    f2.setYou(28);
    f2.setMe(29);
    pojo.setAnotherFlight(f2);

    HasEmbeddedJDO.Embedded1 embedded1 = new HasEmbeddedJDO.Embedded1();
    pojo.setEmbedded1(embedded1);
    embedded1.setVal1("v1");
    embedded1.setMultiVal1(Utils.newArrayList("yar1", "yar2"));
    HasEmbeddedJDO.Embedded2 embedded2 = new HasEmbeddedJDO.Embedded2();
    embedded2.setVal2("v2");
    embedded2.setMultiVal2(Utils.newArrayList("bar1", "bar2"));
    embedded1.setEmbedded2(embedded2);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    Entity e = ds.get(KeyFactory.createKey(kindForClass(pojo.getClass()), pojo.getId()));
    assertTrue(e.hasProperty("flightId")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("origin")); // Uses column names from Flight class since not overridden
    assertTrue(e.hasProperty("dest")); // Uses column names from Flight class since not overridden
    assertTrue(e.hasProperty("name")); // Uses column names from Flight class since not overridden
    assertTrue(e.hasProperty("you")); // Uses column names from Flight class since not overridden
    assertTrue(e.hasProperty("me")); // Uses column names from Flight class since not overridden
    assertTrue(e.hasProperty("flight_number")); // Uses column names from Flight class since not overridden
    assertTrue(e.hasProperty("ID")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("ORIGIN")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("DEST")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("NAME")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("YOU")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("ME")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("FLIGHTNUMBER")); // Uses column names from embedded mapping
    assertTrue(e.hasProperty("val1"));
    assertTrue(e.hasProperty("multiVal1"));
    assertTrue(e.hasProperty("val2"));
    assertTrue(e.hasProperty("multiVal2"));
    assertEquals(18, e.getProperties().size());

    assertEquals(1, countForClass(HasEmbeddedJDO.class));
    assertEquals(0, countForClass(Flight.class));
    switchDatasource(PersistenceManagerFactoryName.transactional);
    beginTxn();
    pojo = pm.getObjectById(HasEmbeddedJDO.class, pojo.getId());
    assertNotNull(pojo.getFlight());
    // it's weird but flight doesn't have an equals() method
    assertTrue(f.customEquals(pojo.getFlight()));
    assertNotNull(pojo.getAnotherFlight());
    assertTrue(f2.customEquals(pojo.getAnotherFlight()));
    
    assertNotNull(pojo.getEmbedded1());
    assertEquals("v1", pojo.getEmbedded1().getVal1());
    assertEquals(Utils.newArrayList("yar1", "yar2"), pojo.getEmbedded1().getMultiVal1());
    assertNotNull(pojo.getEmbedded1().getEmbedded2());
    assertEquals("v2", pojo.getEmbedded1().getEmbedded2().getVal2());
    assertEquals(Utils.newArrayList("bar1", "bar2"), pojo.getEmbedded1().getEmbedded2().getMultiVal2());
    commitTxn();
  }

  public void testEmbeddedWithKeyPk_NullEmbedded() {
    HasEmbeddedWithKeyPkJDO pojo = new HasEmbeddedWithKeyPkJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(HasEmbeddedWithKeyPkJDO.class, pojo.getId());
    assertNotNull(pojo.getEmbedded());
    commitTxn();
  }

  public void testEmbeddedWithKeyPk_NotNullEmbedded() {
    HasEmbeddedWithKeyPkJDO pojo = new HasEmbeddedWithKeyPkJDO();
    HasKeyPkJDO embedded = new HasKeyPkJDO();
    embedded.setStr("yar");
    pojo.setEmbedded(embedded);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(HasEmbeddedWithKeyPkJDO.class, pojo.getId());
    assertNotNull(pojo.getEmbedded());
    assertEquals("yar", pojo.getEmbedded().getStr());
    commitTxn();
  }

  public void testEmbeddedWithKeyPk_AddEmbeddedToExistingParent() {
    HasEmbeddedWithKeyPkJDO pojo = new HasEmbeddedWithKeyPkJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    HasKeyPkJDO embedded = new HasKeyPkJDO();
    embedded.setStr("yar");
    beginTxn();
    pojo.setEmbedded(embedded);
    pojo = pm.getObjectById(HasEmbeddedWithKeyPkJDO.class, pojo.getId());
    pojo.setEmbedded(embedded);
    commitTxn();
  }

  public void testEmbeddingPC() throws EntityNotFoundException {
    HasEmbeddedPc parent = new HasEmbeddedPc();
    HasKeyPkJDO embedded = new HasKeyPkJDO();
    embedded.setKey(KeyFactory.createKey("blar", 43L));
    parent.setEmbedded(embedded);
    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity e = ds.get(parent.getKey());
    assertTrue(e.hasProperty("key"));
  }
}
