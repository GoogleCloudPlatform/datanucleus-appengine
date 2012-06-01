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

import java.util.List;

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.datanucleus.test.jdo.UnownedJDOOneToOneBiSideA;
import com.google.appengine.datanucleus.test.jdo.UnownedJDOOneToOneBiSideB;
import com.google.appengine.datanucleus.test.jdo.UnownedJDOOneToOneUniSideA;
import com.google.appengine.datanucleus.test.jdo.UnownedJDOOneToOneUniSideB;

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

    Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneUniSideA a2 = (UnownedJDOOneToOneUniSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneUniSideB b2 = a.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testPersistUniNewAExistingB() throws EntityNotFoundException {
    // Persist B
    UnownedJDOOneToOneUniSideB b = new UnownedJDOOneToOneUniSideB();
    b.setName("Side B");
    pm.makePersistent(b);
    Object bId = pm.getObjectId(b);

    // Persist A linked to B
    UnownedJDOOneToOneUniSideA a = new UnownedJDOOneToOneUniSideA();
    a.setName("Side A");
    a.setOther(b);
    pm.makePersistent(a);
    Object aId = pm.getObjectId(a);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneUniSideA a2 = (UnownedJDOOneToOneUniSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneUniSideB b2 = a.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
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

    Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneBiSideA a2 = (UnownedJDOOneToOneBiSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneBiSideB b2 = a2.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testPersistBiNewAExistingB() throws EntityNotFoundException {
    // Persist B
    UnownedJDOOneToOneBiSideB b = new UnownedJDOOneToOneBiSideB();
    b.setName("Side B");

    pm.makePersistent(b);
    Object bId = pm.getObjectId(b);

    // Persist A-B as unowned
    UnownedJDOOneToOneBiSideA a = new UnownedJDOOneToOneBiSideA();
    a.setName("Side A");
    a.setOther(b);
    b.setOther(a);

    pm.makePersistent(a);
    Object aId = pm.getObjectId(a);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneBiSideA a2 = (UnownedJDOOneToOneBiSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneBiSideB b2 = a2.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testPersistBiExistingANewB() throws EntityNotFoundException {
    // Persist A
    UnownedJDOOneToOneBiSideA a = new UnownedJDOOneToOneBiSideA();
    a.setName("Side A");

    pm.makePersistent(a);
    Object aId = pm.getObjectId(a);

    // Persist A-B as unowned
    UnownedJDOOneToOneBiSideB b = new UnownedJDOOneToOneBiSideB();
    b.setName("Side B");
    b.setOther(a);
    a.setOther(b);

    pm.makePersistent(b);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneBiSideA a2 = (UnownedJDOOneToOneBiSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneBiSideB b2 = a2.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testPersistBiExistingAExistingB() throws EntityNotFoundException {
    // Persist B
    UnownedJDOOneToOneBiSideB b = new UnownedJDOOneToOneBiSideB();
    b.setName("Side B");
    pm.makePersistent(b);
    Object bId = pm.getObjectId(b);

    // Persist A
    UnownedJDOOneToOneBiSideA a = new UnownedJDOOneToOneBiSideA();
    a.setName("Side A");
    pm.makePersistent(a);
    Object aId = pm.getObjectId(a);

    // Link A-B as unowned
    a.setOther(b);
    b.setOther(a);

    // Force the commit of changes since DN updates not atomic
    pm.makePersistent(a);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneBiSideA a2 = (UnownedJDOOneToOneBiSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneBiSideB b2 = a2.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
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

    Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToOneBiSideA a2 = (UnownedJDOOneToOneBiSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    UnownedJDOOneToOneBiSideB b2 = a2.getOther();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testUnownedQuery() {

    // create parent without children, store, detach
    UnownedJDOOneToOneUniSideA a = new UnownedJDOOneToOneUniSideA();
    UnownedJDOOneToOneUniSideB b = new UnownedJDOOneToOneUniSideB();
    a.setOther(b);

    pm.makePersistent(a);
    UnownedJDOOneToOneUniSideB detachedB = pm.detachCopy(b);
    pm.close();

    pm = pmf.getPersistenceManager();

    javax.jdo.Query q = pm.newQuery("SELECT FROM " + UnownedJDOOneToOneUniSideA.class.getName() + 
        " where this.other == :uf");
    List<UnownedJDOOneToOneUniSideA> parents = (List<UnownedJDOOneToOneUniSideA>) q.execute(detachedB);
    assertEquals(1, parents.size());
  }
}
