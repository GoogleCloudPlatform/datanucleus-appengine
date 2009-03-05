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

import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.HasEmbeddedJDO;
import org.datanucleus.test.Flight;

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
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    assertEquals(1, countForClass(HasEmbeddedJDO.class));
    assertEquals(0, countForClass(Flight.class));
    beginTxn();
    pojo = pm.getObjectById(HasEmbeddedJDO.class, pojo.getId());
    assertNotNull(pojo.getFlight());
    // wild
    assertEquals(
        TestUtils.createKey(pojo, pojo.getId()), KeyFactory.stringToKey(pojo.getFlight().getId()));
    commitTxn();
  }
}
