/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import javax.jdo.JDOObjectNotFoundException;

import com.google.appengine.datanucleus.test.HasDatastoreIdentityLongJDO;

/**
 * Tests for datastore-identity with JDO.
 */
public class JDODatastoreIdentityTest extends JDOTestCase {

  public void testInsertUpdate_IdGen() {
    HasDatastoreIdentityLongJDO pojo = new HasDatastoreIdentityLongJDO();
    pojo.setName("First Name");

    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    Object id = pm.getObjectId(pojo);
    pm.evictAll();

    beginTxn();
    HasDatastoreIdentityLongJDO pojo2 = (HasDatastoreIdentityLongJDO)pm.getObjectById(id);
    assertNotNull(pojo2);
    assertEquals("First Name", pojo2.getName());
    pojo2.setName("Second Name");
    commitTxn();
    pm.evictAll();

    beginTxn();
    pojo2 = (HasDatastoreIdentityLongJDO)pm.getObjectById(id);
    assertNotNull(pojo2);
    assertEquals("Second Name", pojo2.getName());

    pm.deletePersistent(pojo2);
    commitTxn();
    pm.evictAll();

    beginTxn();
    try {
      pojo2 = (HasDatastoreIdentityLongJDO)pm.getObjectById(id);
      fail("Returned object even though it was deleted");
    } catch (JDOObjectNotFoundException onfe) {
      // expected
    }
    commitTxn();
  }
}