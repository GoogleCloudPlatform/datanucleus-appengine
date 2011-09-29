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

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.datanucleus.test.UnownedJDOOneToOneBiSideA;
import com.google.appengine.datanucleus.test.UnownedJDOOneToOneBiSideB;
import com.google.appengine.datanucleus.test.UnownedJDOOneToOneUniSideA;
import com.google.appengine.datanucleus.test.UnownedJDOOneToOneUniSideB;

/**
 * Tests for 1-1 unowned JDO relations.
 */
public class JDOUnownedOneToOneTest extends JDOTestCase {

  public void testPersistUniNewBoth() throws EntityNotFoundException {
    // Persist A-B as unowned
    UnownedJDOOneToOneUniSideA a = new UnownedJDOOneToOneUniSideA();
    a.setName("Side A");
    UnownedJDOOneToOneUniSideB b = new UnownedJDOOneToOneUniSideB();
    b.setName("Side B");
    a.setOther(b);

    pm.makePersistent(a);
    // TODO Enable this when we default to latest storage version
    /*Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneUniSideA a2 = (UnownedJDOOneToOneUniSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneUniSideB b2 = a.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));*/
  }

  public void testPersistBiNewBothFromOwner() throws EntityNotFoundException {
    // Persist A-B as unowned
    UnownedJDOOneToOneBiSideA a = new UnownedJDOOneToOneBiSideA();
    a.setName("Side A");
    UnownedJDOOneToOneBiSideB b = new UnownedJDOOneToOneBiSideB();
    b.setName("Side B");
    a.setOther(b);
    b.setOther(a);

    pm.makePersistent(a);
    // TODO Enable this when we default to latest storage version
    /*Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneUniSideA a2 = (UnownedJDOOneToOneUniSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneUniSideB b2 = a.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));*/
  }

  public void testPersistBiNewBothFromNonowner() throws EntityNotFoundException {
    // Persist A-B as unowned
    UnownedJDOOneToOneBiSideA a = new UnownedJDOOneToOneBiSideA();
    a.setName("Side A");
    UnownedJDOOneToOneBiSideB b = new UnownedJDOOneToOneBiSideB();
    b.setName("Side B");
    a.setOther(b);
    b.setOther(a);

    pm.makePersistent(b);
    // TODO Enable this when we default to latest storage version
    /*Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneUniSideA a2 = (UnownedJDOOneToOneUniSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneUniSideB b2 = a.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));*/
  }
}
