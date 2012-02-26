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
import java.util.Set;

import javax.jdo.FetchPlan;
import javax.jdo.JDOHelper;

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.datanucleus.test.UnownedJDOOneToManyBiSideA;
import com.google.appengine.datanucleus.test.UnownedJDOOneToManyBiSideB;
import com.google.appengine.datanucleus.test.UnownedJDOOneToManyUniListSideA;
import com.google.appengine.datanucleus.test.UnownedJDOOneToManyUniSideA;
import com.google.appengine.datanucleus.test.UnownedJDOOneToManyUniSideB;

/**
 * Tests for 1-N unowned JDO relations.
 */
public class JDOUnownedOneToManyTest extends JDOTestCase {

  public void testSetPersistUniNewBoth() throws EntityNotFoundException {
    // Persist A-B as unowned
    UnownedJDOOneToManyUniSideA a = new UnownedJDOOneToManyUniSideA();
    a.setName("Side A");
    UnownedJDOOneToManyUniSideB b = new UnownedJDOOneToManyUniSideB();
    b.setName("Side B");
    a.addOther(b);

    pm.makePersistent(a);

    Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToManyUniSideA a2 = (UnownedJDOOneToManyUniSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    Set<UnownedJDOOneToManyUniSideB> others = a2.getOthers();
    assertNotNull(others);
    assertEquals(1, others.size());
    UnownedJDOOneToManyUniSideB b2 = others.iterator().next();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testListPersistUniNewBoth() throws EntityNotFoundException {
    // Persist A-B as unowned
    UnownedJDOOneToManyUniListSideA a = new UnownedJDOOneToManyUniListSideA();
    a.setName("Side A");
    UnownedJDOOneToManyUniSideB b = new UnownedJDOOneToManyUniSideB();
    b.setName("Side B");
    a.addOther(b);

    pm.makePersistent(a);

    Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToManyUniListSideA a2 = (UnownedJDOOneToManyUniListSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    List<UnownedJDOOneToManyUniSideB> others = a2.getOthers();
    assertNotNull(others);
    assertEquals(1, others.size());
    UnownedJDOOneToManyUniSideB b2 = others.iterator().next();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testSetPersistBiNewBoth() throws EntityNotFoundException {
    // Persist A-B as unowned
    UnownedJDOOneToManyBiSideA a = new UnownedJDOOneToManyBiSideA();
    a.setName("Side A");
    UnownedJDOOneToManyBiSideB b = new UnownedJDOOneToManyBiSideB();
    b.setName("Side B");
    a.addOther(b);
    b.setRelated(a);

    pm.makePersistent(a);

    Object aId = pm.getObjectId(a);
    Object bId = pm.getObjectId(b);

    pm.evictAll(); // Make sure we go to the datastore

    // Retrieve by id and check
    UnownedJDOOneToManyBiSideA a2 = (UnownedJDOOneToManyBiSideA)pm.getObjectById(aId);
    assertNotNull(a2);
    assertEquals("Side A", a2.getName());
    Set<UnownedJDOOneToManyBiSideB> others = a2.getOthers();
    assertNotNull(others);
    assertEquals(1, others.size());
    UnownedJDOOneToManyBiSideB b2 = others.iterator().next();
    assertNotNull(b2);
    assertNotNull("Side B", b2.getName());
    assertEquals(bId, pm.getObjectId(b2));
  }

  public void testSetDeleteOwnerNotDependent() {
    UnownedJDOOneToManyUniSideA a = new UnownedJDOOneToManyUniSideA();
    a.setName("Side A");
    UnownedJDOOneToManyUniSideB b1 = new UnownedJDOOneToManyUniSideB();
    b1.setName("First B");
    UnownedJDOOneToManyUniSideB b2 = new UnownedJDOOneToManyUniSideB();
    b2.setName("Second B");
    a.addOther(b1);
    a.addOther(b2);
    pm.makePersistent(a);
    pm.close();

    pm = pmf.getPersistenceManager();
    List<UnownedJDOOneToManyUniSideA> as = (List<UnownedJDOOneToManyUniSideA>) 
        pm.newQuery("select from " + UnownedJDOOneToManyUniSideA.class.getName()).execute();
    assertEquals(1, as.size());
    assertEquals(2, as.get(0).getOthers().size());

    List<UnownedJDOOneToManyUniSideB> bs = (List<UnownedJDOOneToManyUniSideB>) 
        pm.newQuery("select from " + UnownedJDOOneToManyUniSideB.class.getName()).execute();
    assertEquals(2, bs.size());

    pm.deletePersistent(as.get(0));

    as = (List<UnownedJDOOneToManyUniSideA>)pm.newQuery("select from " + UnownedJDOOneToManyUniSideA.class.getName()).execute();
    assertEquals(0, as.size());

    bs = (List<UnownedJDOOneToManyUniSideB>) pm.newQuery("select from " + UnownedJDOOneToManyUniSideB.class.getName()).execute();
    assertEquals(2, bs.size());
  }

  public void testListDeleteOwnerNotDependent() {
    UnownedJDOOneToManyUniListSideA a = new UnownedJDOOneToManyUniListSideA();
    a.setName("Side A");
    UnownedJDOOneToManyUniSideB b1 = new UnownedJDOOneToManyUniSideB();
    b1.setName("First B");
    UnownedJDOOneToManyUniSideB b2 = new UnownedJDOOneToManyUniSideB();
    b2.setName("Second B");
    a.addOther(b1);
    a.addOther(b2);
    pm.makePersistent(a);
    pm.close();

    pm = pmf.getPersistenceManager();
    List<UnownedJDOOneToManyUniListSideA> as = (List<UnownedJDOOneToManyUniListSideA>) 
        pm.newQuery("select from " + UnownedJDOOneToManyUniListSideA.class.getName()).execute();
    assertEquals(1, as.size());
    assertEquals(2, as.get(0).getOthers().size());

    List<UnownedJDOOneToManyUniSideB> bs = (List<UnownedJDOOneToManyUniSideB>) 
        pm.newQuery("select from " + UnownedJDOOneToManyUniSideB.class.getName()).execute();
    assertEquals(2, bs.size());

    pm.deletePersistent(as.get(0));

    as = (List<UnownedJDOOneToManyUniListSideA>)pm.newQuery("select from " + UnownedJDOOneToManyUniListSideA.class.getName()).execute();
    assertEquals(0, as.size());

    bs = (List<UnownedJDOOneToManyUniSideB>) pm.newQuery("select from " + UnownedJDOOneToManyUniSideB.class.getName()).execute();
    assertEquals(2, bs.size());
  }

  public void testUnownedDelete() {
    UnownedJDOOneToManyUniListSideA p = new UnownedJDOOneToManyUniListSideA();
    UnownedJDOOneToManyUniSideB c1 = new UnownedJDOOneToManyUniSideB();
    p.addOther(c1);
    UnownedJDOOneToManyUniSideB c2 = new UnownedJDOOneToManyUniSideB();
    p.addOther(c2);

    pm.makePersistent(p);
    pm.close();

    pm = pmf.getPersistenceManager();

    List<UnownedJDOOneToManyUniListSideA> parents = (List<UnownedJDOOneToManyUniListSideA>) 
      pm.newQuery( "select from " + UnownedJDOOneToManyUniListSideA.class.getName() ).execute();

    assertEquals(1, parents.size());
    assertEquals(2, parents.get(0).getOthers().size());

    List<UnownedJDOOneToManyUniSideB> children = (List<UnownedJDOOneToManyUniSideB>)
      pm.newQuery("select from " + UnownedJDOOneToManyUniSideB.class.getName()).execute();
    assertEquals(2, children.size() );

    pm.deletePersistent(parents.get(0));

    parents = (List<UnownedJDOOneToManyUniListSideA>) 
      pm.newQuery("select from " + UnownedJDOOneToManyUniListSideA.class.getName()).execute();
    assertEquals(0, parents.size());

    children = (List<UnownedJDOOneToManyUniSideB>)
      pm.newQuery("select from " + UnownedJDOOneToManyUniSideB.class.getName()).execute();
    assertEquals(2, children.size());
  }

  public void testUnownedList() {

    // creat children first, persist
    UnownedJDOOneToManyUniSideB c1 = new UnownedJDOOneToManyUniSideB();
    UnownedJDOOneToManyUniSideB c2 = new UnownedJDOOneToManyUniSideB();
    pm.makePersistent(c1);
    pm.makePersistent(c2);
    pm.evictAll();

    // verify children
    List<UnownedJDOOneToManyUniSideB> children = (List<UnownedJDOOneToManyUniSideB>)
      pm.newQuery( "select from " + UnownedJDOOneToManyUniSideB.class.getName() ).execute();
    assertEquals(2, children.size());

    // create parent without children, store, detach
    UnownedJDOOneToManyUniListSideA p = new UnownedJDOOneToManyUniListSideA();
    p = pm.makePersistent(p);
    pm.getFetchPlan().setGroup(FetchPlan.ALL);
    UnownedJDOOneToManyUniListSideA pDetached = pm.detachCopy(p);
    assertTrue(JDOHelper.isDetached(pDetached));

    // add already persistent children
    pDetached.getOthers().add(pm.detachCopy(c1));
    pDetached.getOthers().add(pm.detachCopy(c2));

    // update parent
    pm.makePersistent(pDetached);
    pm.close();

    // get new pm
    pm = pmf.getPersistenceManager();

    List<UnownedJDOOneToManyUniListSideA> parents = (List<UnownedJDOOneToManyUniListSideA>)
      pm.newQuery( "select from " + UnownedJDOOneToManyUniListSideA.class.getName() ).execute();

    assertEquals( 1, parents.size() );
    assertEquals( 2, parents.get( 0 ).getOthers().size() );
  }
}