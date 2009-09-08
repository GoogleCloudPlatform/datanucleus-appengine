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

package org.datanucleus.store.appengine;

import org.datanucleus.test.Flight;
import org.datanucleus.test.HasEmbeddedJDO;
import org.datanucleus.test.HasEmbeddedWithKeyPkJDO;
import org.datanucleus.test.HasKeyPkJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOEmbeddedTest extends JDOTestCase {

  public void testEmbeddedWithGeneratedId() {
    HasEmbeddedJDO pojo = new HasEmbeddedJDO();
    Flight f = new Flight();
    f.setFlightNumber(23);
    f.setName("harold");
    f.setOrigin("bos");
    f.setDest("mia");
    f.setYou(24);
    f.setMe(25);
    pojo.setFlight(f);
    HasEmbeddedJDO.Embedded1 embedded1 = new HasEmbeddedJDO.Embedded1();
    pojo.setEmbedded1(embedded1);
    embedded1.setVal1("v1");
    HasEmbeddedJDO.Embedded2 embedded2 = new HasEmbeddedJDO.Embedded2();
    embedded2.setVal2("v2");
    embedded1.setEmbedded2(embedded2);
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    assertEquals(1, countForClass(HasEmbeddedJDO.class));
    assertEquals(0, countForClass(Flight.class));
    beginTxn();
    pojo = pm.getObjectById(HasEmbeddedJDO.class, pojo.getId());
    assertNotNull(pojo.getFlight());
    // wild
    assertNull(pojo.getFlight().getId());
    assertNotNull(pojo.getEmbedded1());
    assertEquals("v1", pojo.getEmbedded1().getVal1());
    assertNotNull(pojo.getEmbedded1().getEmbedded2());
    assertEquals("v2", pojo.getEmbedded1().getEmbedded2().getVal2());
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
}
