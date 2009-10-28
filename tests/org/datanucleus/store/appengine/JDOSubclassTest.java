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
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.jdo.exceptions.NoPersistenceInformationException;
import org.datanucleus.test.SubclassesJDO;
import org.datanucleus.test.SubclassesJDO.CompleteTableParentNoChildStrategy;
import org.datanucleus.test.SubclassesJDO.CompleteTableParentWithCompleteTableChild;
import org.datanucleus.test.SubclassesJDO.CompleteTableParentWithEmbedded;
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
import javax.jdo.Query;

/**
 * There's something flaky here that will probably show up as a real bug at
 * some point.  If the Parent class gets used first, the subclass
 * tests fail.  To get around this I'm just running the subclass tests
 * first.  There's definitely something funny going on though.
 *
 * @author Max Ross <maxr@google.com>
 */
// TODO(maxr): Tests where there are relationships on the parent/child/grandchild
// TODO(maxr): Tests where there is inheritance in the embedded classes
// TODO(maxr): non-transactional tests
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

  public void testEmbedded_Child() throws Exception {
    CompleteTableParentWithEmbedded.Child child = new CompleteTableParentWithEmbedded.Child();
    child.setAString("aString");
    child.setBString("bString");
    SubclassesJDO.IsEmbeddedOnly embedded = new SubclassesJDO.IsEmbeddedOnly();
    embedded.setVal0("embedded val 0");
    embedded.setVal1("embedded val 1");
    child.setEmbedded(embedded);
    SubclassesJDO.IsEmbeddedOnlyBase embeddedBase = new SubclassesJDO.IsEmbeddedOnlyBase();
    embeddedBase.setVal0("embedded base val 0");
    child.setEmbeddedBase(embeddedBase);
    SubclassesJDO.IsEmbeddedOnly2 embedded2 = new SubclassesJDO.IsEmbeddedOnly2();
    embedded2.setVal2("embedded val 2");
    embedded2.setVal3("embedded val 3");
    child.setEmbedded2(embedded2);
    SubclassesJDO.IsEmbeddedOnlyBase2 embeddedBase2 = new SubclassesJDO.IsEmbeddedOnlyBase2();
    embeddedBase2.setVal2("embedded base val 2");
    child.setEmbeddedBase2(embeddedBase2);
    beginTxn();
    pm.makePersistent(child);
    commitTxn();
    Key key = KeyFactory.createKey(kindForClass(child.getClass()), child.getId());
    Entity e = ldth.ds.get(key);
    assertEquals("aString", e.getProperty("aString"));
    assertEquals("bString", e.getProperty("bString"));
    assertEquals("embedded val 0", e.getProperty("val0"));
    assertEquals("embedded val 1", e.getProperty("val1"));
    assertEquals("embedded base val 0", e.getProperty("VAL0"));
    assertEquals("embedded val 2", e.getProperty("val2"));
    assertEquals("embedded val 3", e.getProperty("val3"));
    assertEquals("embedded base val 2", e.getProperty("VAL2"));
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    child = pm.getObjectById(child.getClass(), child.getId());
    assertEmbeddedChildContents(child);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    Query q = pm.newQuery("select from " + child.getClass().getName() + " where embedded.val1 == :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setUnique(true);
    child = (CompleteTableParentWithEmbedded.Child) q.execute("embedded val 1");
    assertEmbeddedChildContents(child);

    q = pm.newQuery("select from " + child.getClass().getName() + " where embedded.val0 == :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setUnique(true);
    child = (CompleteTableParentWithEmbedded.Child) q.execute("embedded val 0");
    assertEmbeddedChildContents(child);

    q = pm.newQuery("select from " + child.getClass().getName() + " where embeddedBase.val0 == :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setUnique(true);
    child = (CompleteTableParentWithEmbedded.Child) q.execute("embedded base val 0");
    assertEmbeddedChildContents(child);

    q = pm.newQuery("select from " + child.getClass().getName() + " where embedded2.val2 == :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setUnique(true);
    child = (CompleteTableParentWithEmbedded.Child) q.execute("embedded val 2");
    assertEmbeddedChildContents(child);

    q = pm.newQuery("select from " + child.getClass().getName() + " where embedded2.val3 == :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setUnique(true);
    child = (CompleteTableParentWithEmbedded.Child) q.execute("embedded val 3");
    assertEmbeddedChildContents(child);

    q = pm.newQuery("select from " + child.getClass().getName() + " where embeddedBase2.val2 == :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setUnique(true);
    child = (CompleteTableParentWithEmbedded.Child) q.execute("embedded base val 2");
    assertEmbeddedChildContents(child);

    q = pm.newQuery("select embedded.val1, embedded.val0, embeddedBase.val0, embedded2.val2, embedded2.val3, embeddedBase2.val2 from " +
                    child.getClass().getName() + " where embeddedBase2.val2 == :p");
    q.setUnique(true);
    Object[] result = (Object[]) q.execute("embedded base val 2");
    assertEquals("embedded val 1", result[0]);
    assertEquals("embedded val 0", result[1]);
    assertEquals("embedded base val 0", result[2]);
    assertEquals("embedded val 2", result[3]);
    assertEquals("embedded val 3", result[4]);
    assertEquals("embedded base val 2", result[5]);

    pm.deletePersistent(child);
    commitTxn();
    try {
      ldth.ds.get(key);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  public void testEmbedded_Parent() throws Exception {
    CompleteTableParentWithEmbedded parent = new CompleteTableParentWithEmbedded();
    parent.setAString("aString");
    SubclassesJDO.IsEmbeddedOnly embedded = new SubclassesJDO.IsEmbeddedOnly();
    embedded.setVal0("embedded val 0");
    embedded.setVal1("embedded val 1");
    parent.setEmbedded(embedded);
    SubclassesJDO.IsEmbeddedOnlyBase embeddedBase = new SubclassesJDO.IsEmbeddedOnlyBase();
    embeddedBase.setVal0("embedded base val 0");
    parent.setEmbeddedBase(embeddedBase);
    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Key key = KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId());
    Entity e = ldth.ds.get(key);
    assertEquals("aString", e.getProperty("aString"));
    assertEquals("embedded val 0", e.getProperty("val0"));
    assertEquals("embedded val 1", e.getProperty("val1"));
    assertEquals("embedded base val 0", e.getProperty("VAL0"));
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEmbeddedParentContents(parent);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    Query q = pm.newQuery(
        "select from " + parent.getClass().getName() + " where embedded.val1 == :p "
        + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setUnique(true);
    parent = (CompleteTableParentWithEmbedded) q.execute("embedded val 1");
    assertEmbeddedParentContents(parent);

    q = pm.newQuery(
        "select from " + parent.getClass().getName() + " where embedded.val0 == :p "
        + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setUnique(true);
    parent = (CompleteTableParentWithEmbedded) q.execute("embedded val 0");
    assertEmbeddedParentContents(parent);

    q = pm.newQuery(
        "select from " + parent.getClass().getName() + " where embeddedBase.val0 == :p "
        + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setUnique(true);
    parent = (CompleteTableParentWithEmbedded) q.execute("embedded base val 0");
    assertEmbeddedParentContents(parent);

    q = pm.newQuery("select embedded.val1, embedded.val0, embeddedBase.val0 from " +
                    parent.getClass().getName() + " where embeddedBase.val0 == :p "
        + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setUnique(true);
    Object[] result = (Object[]) q.execute("embedded base val 0");
    assertEquals("embedded val 1", result[0]);
    assertEquals("embedded val 0", result[1]);
    assertEquals("embedded base val 0", result[2]);

    pm.deletePersistent(parent);
    commitTxn();
    try {
      ldth.ds.get(key);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  // This is absurd, but if the signature of this method and the one below
  // refers to the actual type we want the runtime enhancer gets totally
  // confused.
  private void assertEmbeddedChildContents(Object obj) {
    CompleteTableParentWithEmbedded.Child child = (CompleteTableParentWithEmbedded.Child) obj;
    assertEquals("bString", child.getBString());
    assertEquals("embedded val 2", child.getEmbedded2().getVal2());
    assertEquals("embedded val 3", child.getEmbedded2().getVal3());
    assertEquals("embedded base val 2", child.getEmbeddedBase2().getVal2());
    assertEmbeddedParentContents(child);
  }

  private void assertEmbeddedParentContents(Object obj) {
    CompleteTableParentWithEmbedded parentWithEmbedded = (CompleteTableParentWithEmbedded) obj;
    assertEquals("aString", parentWithEmbedded.getAString());
    assertEquals("embedded val 0", parentWithEmbedded.getEmbedded().getVal0());
    assertEquals("embedded val 1", parentWithEmbedded.getEmbedded().getVal1());
    assertEquals("embedded base val 0", parentWithEmbedded.getEmbeddedBase().getVal0());
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

  private void testInsertChild(SubclassesJDO.Child child) throws Exception {
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

  private void testFetchChild(Class<? extends SubclassesJDO.Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.Child child = pm.getObjectById(childClass, e.getKey());
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
    e.setProperty("aString", "z8");
    ldth.ds.put(e);

    e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "z9");
    ldth.ds.put(e);

    beginTxn();
    Parent parent = ((List<Parent>) pm.newQuery(
        "select from " + parentClass.getName() + " where aString == 'z8'").execute()).get(0);
    assertEquals(parentClass, parent.getClass());
    assertEquals("z8", parent.getAString());
    commitTxn();

    beginTxn();
    List<Parent> parents = ((List<Parent>) pm.newQuery(
        "select from " + parentClass.getName() + " where aString >= 'z8' order by aString desc").execute());
    assertEquals(2, parents.size());
    assertEquals("z9", parents.get(0).getAString());
    assertEquals("z8", parents.get(1).getAString());
    commitTxn();

    beginTxn();
    String aString = ((List<String>) pm.newQuery(
        "select aString from " + parentClass.getName() + " where aString == 'z8'").execute()).get(0);
    assertEquals("z8", aString);
    commitTxn();
  }

  private void testQueryChild(Class<? extends SubclassesJDO.Child> childClass) {
    Entity e1 = new Entity(kindForClass(childClass));
    e1.setProperty("aString", "a2");
    e1.setProperty("bString", "b2");
    ldth.ds.put(e1);

    Entity e2 = new Entity(kindForClass(childClass));
    e2.setProperty("aString", "a2");
    e2.setProperty("bString", "b3");
    ldth.ds.put(e2);

    Entity e3 = new Entity(kindForClass(childClass));
    e3.setProperty("aString", "a2");
    e3.setProperty("bString", "b3");
    ldth.ds.put(e3);

    beginTxn();

    SubclassesJDO.Child child = ((List<SubclassesJDO.Child>) pm.newQuery(
        "select from " + childClass.getName() + " where aString == 'a2'").execute()).get(0);
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    child = ((List<SubclassesJDO.Child>) pm.newQuery(
        "select from " + childClass.getName() + " where bString == 'b2'").execute()).get(0);
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    List<SubclassesJDO.Child> kids = ((List<SubclassesJDO.Child>) pm.newQuery(
        "select from " + childClass.getName() + " where aString == 'a2' order by bString desc").execute());
    assertEquals(3, kids.size());
    assertEquals(e2.getKey().getId(), kids.get(0).getId().longValue());
    assertEquals(e3.getKey().getId(), kids.get(1).getId().longValue());
    assertEquals(e1.getKey().getId(), kids.get(2).getId().longValue());

    kids = ((List<SubclassesJDO.Child>) pm.newQuery("select from " + childClass.getName() + " where aString == 'a2' order by aString desc").execute());
    assertEquals(3, kids.size());
    assertEquals(e1.getKey().getId(), kids.get(0).getId().longValue());
    assertEquals(e2.getKey().getId(), kids.get(1).getId().longValue());
    assertEquals(e3.getKey().getId(), kids.get(2).getId().longValue());

    Object[] result = ((List<Object[]>) pm.newQuery("select bString, aString from " + childClass.getName() + " where bString == 'b2'").execute()).get(0);
    assertEquals(2, result.length);
    assertEquals("b2", result[0]);
    assertEquals("a2", result[1]);

    commitTxn();
  }

  private void testQueryGrandchild(Class<? extends org.datanucleus.test.SubclassesJDO.Grandchild> grandchildClass) {
    Entity e1 = new Entity(kindForClass(grandchildClass));
    e1.setProperty("aString", "a2");
    e1.setProperty("bString", "b1");
    e1.setProperty("cString", "c2");
    ldth.ds.put(e1);

    Entity e2 = new Entity(kindForClass(grandchildClass));
    e2.setProperty("aString", "a2");
    e2.setProperty("bString", "b3");
    e2.setProperty("cString", "c3");
    ldth.ds.put(e2);

    Entity e3 = new Entity(kindForClass(grandchildClass));
    e3.setProperty("aString", "a2");
    e3.setProperty("bString", "b2");
    e3.setProperty("cString", "c3");
    ldth.ds.put(e3);

    beginTxn();
    Grandchild grandchild = ((List<Grandchild>) pm.newQuery(
        "select from " + grandchildClass.getName() + " where aString == 'a2'").execute()).get(0);
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b1", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    grandchild = ((List<Grandchild>) pm.newQuery(
        "select from " + grandchildClass.getName() + " where bString == 'b2'").execute()).get(0);
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b2", grandchild.getBString());
    assertEquals("c3", grandchild.getCString());

    grandchild = ((List<Grandchild>) pm.newQuery(
        "select from " + grandchildClass.getName() + " where cString == 'c2'").execute()).get(0);
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b1", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    List<Grandchild> grandkids = ((List<Grandchild>) pm.newQuery(
        "select from " + grandchildClass.getName() + " where aString == 'a2' order by bString desc").execute());
    assertEquals(3, grandkids.size());
    assertEquals(e2.getKey().getId(), grandkids.get(0).getId().longValue());
    assertEquals(e3.getKey().getId(), grandkids.get(1).getId().longValue());
    assertEquals(e1.getKey().getId(), grandkids.get(2).getId().longValue());

    grandkids = ((List<Grandchild>) pm.newQuery(
        "select from " + grandchildClass.getName() + " where aString == 'a2' order by aString desc").execute());
    assertEquals(3, grandkids.size());
    assertEquals(e1.getKey().getId(), grandkids.get(0).getId().longValue());
    assertEquals(e2.getKey().getId(), grandkids.get(1).getId().longValue());
    assertEquals(e3.getKey().getId(), grandkids.get(2).getId().longValue());

    grandkids = ((List<Grandchild>) pm.newQuery(
        "select from " + grandchildClass.getName() + " where aString == 'a2' order by cString desc").execute());
    assertEquals(3, grandkids.size());
    assertEquals(e2.getKey().getId(), grandkids.get(0).getId().longValue());
    assertEquals(e3.getKey().getId(), grandkids.get(1).getId().longValue());
    assertEquals(e1.getKey().getId(), grandkids.get(2).getId().longValue());

    Object[] result = ((List<Object[]>) pm.newQuery(
        "select bString, aString, cString from " + grandchildClass.getName() + " where cString == 'c2'").execute()).get(0);
    assertEquals(3, result.length);
    assertEquals("b1", result[0]);
    assertEquals("a2", result[1]);
    assertEquals("c2", result[2]);

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

  private void testDeleteChild(Class<? extends SubclassesJDO.Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.Child child = pm.getObjectById(childClass, e.getKey());
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
    SubclassesJDO.Child child = pm.getObjectById(grandchildClass, e.getKey());
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

  private void testUpdateChild(Class<? extends SubclassesJDO.Child> childClass) throws Exception {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.Child child = pm.getObjectById(childClass, e.getKey());
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

  private void testChild(SubclassesJDO.Child child) throws Exception {
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
