/**********************************************************************
Copyright (c) 2009 Google Inc.

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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.jdo.exceptions.NoPersistenceInformationException;
import org.datanucleus.test.SubclassesJDO.Child;
import org.datanucleus.test.SubclassesJDO.CompleteTableParentNoChildStrategy;
import org.datanucleus.test.SubclassesJDO.CompleteTableParentWithCompleteTableChild;
import org.datanucleus.test.SubclassesJDO.CompleteTableParentWithNewTableChild;
import org.datanucleus.test.SubclassesJDO.CompleteTableParentWithSubclassTableChild;
import org.datanucleus.test.SubclassesJDO.Grandchild;
import org.datanucleus.test.SubclassesJDO.NewTableParentWithCompleteTableChild;
import org.datanucleus.test.SubclassesJDO.NewTableParentWithNewTableChild;
import org.datanucleus.test.SubclassesJDO.NewTableParentWithSubclassTableChild;
import org.datanucleus.test.SubclassesJDO.OverrideParent;
import org.datanucleus.test.SubclassesJDO.Parent;
import org.datanucleus.test.SubclassesJDO.SubclassTableParentWithCompleteTableChild;
import org.datanucleus.test.SubclassesJDO.SubclassTableParentWithNewTableChild;
import org.datanucleus.test.SubclassesJDO.SubclassTableParentWithSubclassTableChild;

import java.util.List;

import javax.jdo.JDOFatalUserException;

/**
 * There's something flaky here that will probably show up as a real bug at
 * some point.  If the Parent class gets used first, the subclass
 * tests fail.  To get around this I'm just running the subclass tests
 * first.  There's definitely something funny going on though.
 *
 * @author Max Ross <maxr@google.com>
 */
// TODO(maxr): Tests where there are relationships on the parent/child/grandchild
// TODO(maxr): Tests where there are embedded fields on the parent/child/grandchild
// TODO(maxr): Tests where there are overrides
public class JDOSubclassTest extends JDOTestCase {

  public void testGrandchildren() throws Exception {
    testGrandchild(new CompleteTableParentWithCompleteTableChild.Child.Grandchild());
    testGrandchild(new CompleteTableParentNoChildStrategy.Child.Grandchild());
    testGrandchild(new SubclassTableParentWithCompleteTableChild.Child.Grandchild());
  }

  public void testChildren() throws Exception {
    testChild(new CompleteTableParentWithCompleteTableChild.Child());
    testChild(new CompleteTableParentNoChildStrategy.Child());
    testChild(new SubclassTableParentWithCompleteTableChild.Child());
    testChild(new SubclassTableParentWithNewTableChild.Child());
  }

  public void testUnsupportedStrategies_GAE() {
    assertUnsupportedByGAE(new NewTableParentWithCompleteTableChild.Child());
    assertUnsupportedByGAE(new NewTableParentWithNewTableChild.Child());
    assertUnsupportedByGAE(new CompleteTableParentWithNewTableChild.Child());
    assertUnsupportedByGAE(new SubclassTableParentWithNewTableChild.Child.Grandchild());
  }

  public void testUnsupportedStrategies_DataNuc() throws Exception {
    assertUnsupportedByDataNuc(new SubclassTableParentWithSubclassTableChild.Child());
    assertUnsupportedByDataNuc(new SubclassTableParentWithCompleteTableChild());
  }

  public void testParents() throws Exception {
    testParent(new CompleteTableParentWithCompleteTableChild());
    testParent(new CompleteTableParentWithNewTableChild());
    testParent(new CompleteTableParentWithSubclassTableChild());
    testParent(new CompleteTableParentNoChildStrategy());
    testParent(new NewTableParentWithCompleteTableChild());
    testParent(new NewTableParentWithSubclassTableChild());
    testParent(new NewTableParentWithNewTableChild());
  }

  public void testOverride() throws Exception {
    OverrideParent.Child child = new OverrideParent.Child();
    child.setOverriddenString("blarg");
    beginTxn();
    pm.makePersistent(child);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(child.getClass()), child.getId()));
    assertEquals("blarg", e.getProperty("overridden_string"));
    assertFalse(e.hasProperty("overriddenProperty"));
  }

  private void assertUnsupportedByDataNuc(Object obj) {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    beginTxn();
    try {
      pm.makePersistent(obj);
      fail("expected exception");
    } catch (NoPersistenceInformationException e) {
      // good
    }
    rollbackTxn();
  }

  private void assertUnsupportedByGAE(Object obj) {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    beginTxn();
    try {
      pm.makePersistent(obj);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause().getClass().getName(),
                 DatastoreManager.UnsupportedInheritanceStrategyException.class.isAssignableFrom(e.getCause().getClass()));
    }
    rollbackTxn();
  }

  private void testInsertParent(Parent parent) throws Exception {
    parent.setAString("a");
    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  private void testInsertChild(org.datanucleus.test.SubclassesJDO.Child child) throws Exception {
    child.setAString("a");
    child.setBString("b");
    beginTxn();
    pm.makePersistent(child);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(child.getClass()), child.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }

  private void testInsertGrandchild(org.datanucleus.test.SubclassesJDO.Grandchild grandchild) throws Exception {
    grandchild.setAString("a");
    grandchild.setBString("b");
    grandchild.setCString("c");
    beginTxn();
    pm.makePersistent(grandchild);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(grandchild.getClass()), grandchild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
    assertEquals("c", e.getProperty("cString"));
  }

  private void testFetchParent(Class<? extends Parent> parentClass) {
    Entity e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    Parent parent = pm.getObjectById(parentClass, e.getKey());
    assertEquals(parentClass, parent.getClass());
    assertEquals("a", parent.getAString());
    commitTxn();
  }

  private void testFetchChild(Class<? extends org.datanucleus.test.SubclassesJDO.Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    org.datanucleus.test.SubclassesJDO.Child child = pm.getObjectById(childClass, e.getKey());
    assertEquals(childClass, child.getClass());
    assertEquals("a", child.getAString());
    assertEquals("b", child.getBString());
    commitTxn();
  }

  private void testFetchGrandchild(Class<? extends org.datanucleus.test.SubclassesJDO.Grandchild> grandchildClass) {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    org.datanucleus.test.SubclassesJDO.Grandchild
        grandchild = pm.getObjectById(grandchildClass, e.getKey());
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a", grandchild.getAString());
    assertEquals("b", grandchild.getBString());
    assertEquals("c", grandchild.getCString());
    commitTxn();
  }

  private void testQueryParent(Class<? extends Parent> parentClass) {
    Entity e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "a2");
    ldth.ds.put(e);

    beginTxn();
    Parent parent = ((List<Parent>) pm.newQuery("select from " + parentClass.getName() + " where aString == 'a2'").execute()).get(0);
    assertEquals(parentClass, parent.getClass());
    assertEquals("a2", parent.getAString());
    commitTxn();
  }

  private void testQueryChild(Class<? extends org.datanucleus.test.SubclassesJDO.Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a2");
    e.setProperty("bString", "b2");
    ldth.ds.put(e);

    beginTxn();

    Child child = ((List<Child>) pm.newQuery("select from " + childClass.getName() + " where aString == 'a2'").execute()).get(0);
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    child = ((List<Child>) pm.newQuery("select from " + childClass.getName() + " where bString == 'b2'").execute()).get(0);
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    commitTxn();
  }

  private void testQueryGrandchild(Class<? extends org.datanucleus.test.SubclassesJDO.Grandchild> grandchildClass) {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a2");
    e.setProperty("bString", "b2");
    e.setProperty("cString", "c2");
    ldth.ds.put(e);

    beginTxn();
    Grandchild grandchild = ((List<Grandchild>) pm.newQuery("select from " + grandchildClass.getName() + " where aString == 'a2'").execute()).get(0);
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b2", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    grandchild = ((List<Grandchild>) pm.newQuery("select from " + grandchildClass.getName() + " where bString == 'b2'").execute()).get(0);
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b2", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    grandchild = ((List<Grandchild>) pm.newQuery("select from " + grandchildClass.getName() + " where cString == 'c2'").execute()).get(0);
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b2", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    commitTxn();
  }

  private void testDeleteParent(Class<? extends Parent> parentClass) {
    Entity e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    Parent parent = pm.getObjectById(parentClass, e.getKey());
    pm.deletePersistent(parent);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  private void testDeleteChild(Class<? extends org.datanucleus.test.SubclassesJDO.Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    org.datanucleus.test.SubclassesJDO.Child child = pm.getObjectById(childClass, e.getKey());
    pm.deletePersistent(child);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  private void testDeleteGrandchild(Class<? extends org.datanucleus.test.SubclassesJDO.Grandchild> grandchildClass) {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    org.datanucleus.test.SubclassesJDO.Child child = pm.getObjectById(grandchildClass, e.getKey());
    pm.deletePersistent(child);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  private void testUpdateParent(Class<? extends Parent> parentClass) throws Exception {
    Entity e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    Parent parent = pm.getObjectById(parentClass, e.getKey());
    parent.setAString("not a");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
  }

  private void testUpdateChild(Class<? extends org.datanucleus.test.SubclassesJDO.Child> childClass) throws Exception {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    org.datanucleus.test.SubclassesJDO.Child child = pm.getObjectById(childClass, e.getKey());
    child.setAString("not a");
    child.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  private void testUpdateGrandchild(Class<? extends org.datanucleus.test.SubclassesJDO.Grandchild> grandchildClass) throws Exception {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    org.datanucleus.test.SubclassesJDO.Grandchild
        grandchild = pm.getObjectById(grandchildClass, e.getKey());
    grandchild.setAString("not a");
    grandchild.setBString("not b");
    grandchild.setCString("not c");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
    assertEquals("not c", e.getProperty("cString"));
  }

  private void testGrandchild(org.datanucleus.test.SubclassesJDO.Grandchild grandchild) throws Exception {
    testInsertGrandchild(grandchild);
    testUpdateGrandchild(grandchild.getClass());
    testDeleteGrandchild(grandchild.getClass());
    testFetchGrandchild(grandchild.getClass());
    testQueryGrandchild(grandchild.getClass());
  }

  private void testChild(org.datanucleus.test.SubclassesJDO.Child child) throws Exception {
    testInsertChild(child);
    testUpdateChild(child.getClass());
    testDeleteChild(child.getClass());
    testFetchChild(child.getClass());
    testQueryChild(child.getClass());
  }

  private void testParent(Parent parent) throws Exception {
    testInsertParent(parent);
    testUpdateParent(parent.getClass());
    testDeleteParent(parent.getClass());
    testFetchParent(parent.getClass());
    testQueryParent(parent.getClass());
  }

}
