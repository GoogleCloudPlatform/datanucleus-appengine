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
import org.datanucleus.test.SubclassesJDO;
import org.datanucleus.test.SubclassesJDO.CompleteTableParent;
import org.datanucleus.test.SubclassesJDO.NewTableParent;
import org.datanucleus.test.SubclassesJDO.SubclassTableParent;

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
public class JDOSubclassTest extends JDOTestCase {

  public void testInsertChild_Complete() throws EntityNotFoundException {
    CompleteTableParent.CompleteTableChild
        completeTableTableChild = new CompleteTableParent.CompleteTableChild();
    completeTableTableChild.setAString("a");
    completeTableTableChild.setBString("b");
    beginTxn();
    pm.makePersistent(completeTableTableChild);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(CompleteTableParent.CompleteTableChild.class), completeTableTableChild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }

  public void testInsertGrandChild_Complete() throws EntityNotFoundException {
    CompleteTableParent.CompleteTableGrandchild
        completeTableGrandchild = new CompleteTableParent.CompleteTableGrandchild();
    completeTableGrandchild.setAString("a");
    completeTableGrandchild.setBString("b");
    completeTableGrandchild.setCString("c");
    beginTxn();
    pm.makePersistent(completeTableGrandchild);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(CompleteTableParent.CompleteTableGrandchild.class), completeTableGrandchild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
    assertEquals("c", e.getProperty("cString"));
  }

  public void testUnsupportedStrategies_GAE() {
    assertUnsupportedByGAE(new NewTableParent.CompleteTableChild());
    assertUnsupportedByGAE(new NewTableParent.NewTableChild());

    // The datanucleus enhancer doesn't handle these correctly
    // and we end up with an IAE way before we get any callbacks that would
    // allow us to detect the problem.
//    assertUnsupportedByDataNucleus(new NewParent.SubChild());
//    assertUnsupportedByDataNucleus(new SubParent.SubChild());
  }

  public void testInsertParent_Complete() throws EntityNotFoundException {
    SubclassesJDO.CompleteTableParent completeTableParent = new SubclassesJDO.CompleteTableParent();
    completeTableParent.setAString("a");
    beginTxn();
    pm.makePersistent(completeTableParent);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(SubclassesJDO.CompleteTableParent.class), completeTableParent.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  public void testFetchChild_Complete() {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableChild
        completeTableTableChild = pm.getObjectById(CompleteTableParent.CompleteTableChild.class, e.getKey());
    assertEquals("a", completeTableTableChild.getAString());
    assertEquals("b", completeTableTableChild.getBString());
  }

  public void testFetchGrandChild_Complete() {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableGrandchild completeTableGrandchild =
        pm.getObjectById(CompleteTableParent.CompleteTableGrandchild.class, e.getKey());
    assertEquals("a", completeTableGrandchild.getAString());
    assertEquals("b", completeTableGrandchild.getBString());
    assertEquals("c", completeTableGrandchild.getCString());
  }

  public void testFetchParent_Complete() {
    Entity e = new Entity(kindForClass(SubclassesJDO.CompleteTableParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent
        completeTableParent = pm.getObjectById(CompleteTableParent.class, e.getKey());
    assertEquals("a", completeTableParent.getAString());
  }

  public void testQueryChild_Complete() {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableChild completeTableTableChild = ((List<CompleteTableParent.CompleteTableChild>)
        pm.newQuery(CompleteTableParent.CompleteTableChild.class).execute()).get(0);
    assertEquals("a", completeTableTableChild.getAString());
    assertEquals("b", completeTableTableChild.getBString());
  }

  public void testQueryGrandChild_Complete() {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableGrandchild grandchild = ((List<CompleteTableParent.CompleteTableGrandchild>)pm.newQuery(
        CompleteTableParent.CompleteTableGrandchild.class).execute()).get(0);
    assertEquals("a", grandchild.getAString());
    assertEquals("b", grandchild.getBString());
    assertEquals("c", grandchild.getCString());
  }

  public void testQueryParent_Complete() {
    Entity e = new Entity(kindForClass(SubclassesJDO.CompleteTableParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.CompleteTableParent
        completeTableParent = ((List<CompleteTableParent>)pm.newQuery(SubclassesJDO.CompleteTableParent.class).execute()).get(0);
    assertEquals("a", completeTableParent.getAString());
  }

  public void testDeleteChild_Complete() {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableChild completeTableTableChild = pm.getObjectById(
        CompleteTableParent.CompleteTableChild.class, e.getKey());
    pm.deletePersistent(completeTableTableChild);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testDeleteGrandChild_Complete() {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableGrandchild grandChild =
        pm.getObjectById(CompleteTableParent.CompleteTableGrandchild.class, e.getKey());
    pm.deletePersistent(grandChild);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testDeleteParent_Complete() {
    Entity e = new Entity(kindForClass(SubclassesJDO.CompleteTableParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.CompleteTableParent
        completeTableParent = pm.getObjectById(SubclassesJDO.CompleteTableParent.class, e.getKey());
    pm.deletePersistent(completeTableParent);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testUpdateChild_Complete() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableChild
        completeTableTableChild = pm.getObjectById(CompleteTableParent.CompleteTableChild.class, e.getKey());
    completeTableTableChild.setAString("not a");
    completeTableTableChild.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  public void testUpdateGrandChild_Complete() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(CompleteTableParent.CompleteTableGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    CompleteTableParent.CompleteTableGrandchild grandChild =
        pm.getObjectById(CompleteTableParent.CompleteTableGrandchild.class, e.getKey());
    grandChild.setAString("not a");
    grandChild.setBString("not b");
    grandChild.setCString("not c");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
    assertEquals("not c", e.getProperty("cString"));
  }

  public void testUpdateParent_Complete() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(SubclassesJDO.CompleteTableParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.CompleteTableParent
        completeTableParent = pm.getObjectById(CompleteTableParent.class, e.getKey());
    completeTableParent.setAString("not a");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
  }

  public void testInsertParent_NewTable() throws EntityNotFoundException {
    NewTableParent newTable = new NewTableParent();
    newTable.setAString("a");
    beginTxn();
    pm.makePersistent(newTable);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(NewTableParent.class), newTable.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  public void testInsertChild_SubclassParentCompleteChild() throws EntityNotFoundException {
    SubclassTableParent.CompleteTableChild child = new SubclassTableParent.CompleteTableChild();
    child.setAString("a");
    child.setBString("b");
    beginTxn();
    pm.makePersistent(child);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(SubclassTableParent.CompleteTableChild.class), child.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }


  public void testInsertParent_SubclassParentCompleteChild() throws EntityNotFoundException {
    SubclassTableParent parent = new SubclassTableParent();
    parent.setAString("a");
    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected exception");
    } catch (NoPersistenceInformationException e) {
      // good
    }
    rollbackTxn();
  }

  public void testFetchChild_SubclassParentCompleteChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.CompleteTableChild child = pm.getObjectById(SubclassTableParent.CompleteTableChild.class, e.getKey());
    assertEquals("a", child.getAString());
    assertEquals("b", child.getBString());
  }

  public void testFetchParent_SubclassParentCompleteChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    try {
      pm.getObjectById(SubclassTableParent.class, e.getKey());
      fail("expected exception");
    } catch (NoPersistenceInformationException ex) {
      // good
    }
    rollbackTxn();
  }

  public void testQueryChild_SubclassParentCompleteChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.CompleteTableChild child = ((List<SubclassTableParent.CompleteTableChild>)
        pm.newQuery(SubclassTableParent.CompleteTableChild.class).execute()).get(0);
    assertEquals("a", child.getAString());
    assertEquals("b", child.getBString());
  }

  public void testDeleteChild_SubclassParentCompleteChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.CompleteTableChild child = pm.getObjectById(
        SubclassTableParent.CompleteTableChild.class, e.getKey());
    pm.deletePersistent(child);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testUpdateChild_SubclassParentCompleteChild() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(SubclassTableParent.CompleteTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.CompleteTableChild child = pm.getObjectById(SubclassTableParent.CompleteTableChild.class, e.getKey());
    child.setAString("not a");
    child.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  public void testInsertChild_SubclassParentNewTableChild() throws EntityNotFoundException {
    SubclassTableParent.NewTableChild child = new SubclassTableParent.NewTableChild();
    child.setAString("a");
    child.setBString("b");
    beginTxn();
    pm.makePersistent(child);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(SubclassTableParent.NewTableChild.class), child.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }


  public void testInsertParent_SubclassParentNewTableChild() throws EntityNotFoundException {
    SubclassTableParent parent = new SubclassTableParent();
    parent.setAString("a");
    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected exception");
    } catch (NoPersistenceInformationException e) {
      // good
    }
    rollbackTxn();
  }

  public void testFetchChild_SubclassParentNewTableChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.NewTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.NewTableChild child = pm.getObjectById(SubclassTableParent.NewTableChild.class, e.getKey());
    assertEquals("a", child.getAString());
    assertEquals("b", child.getBString());
  }

  public void testFetchParent_SubclassParentNewTableChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    try {
      pm.getObjectById(SubclassTableParent.class, e.getKey());
      fail("expected exception");
    } catch (NoPersistenceInformationException ex) {
      // good
    }
    rollbackTxn();
  }

  public void testQueryChild_SubclassParentNewTableChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.NewTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.NewTableChild child = ((List<SubclassTableParent.NewTableChild>)
        pm.newQuery(SubclassTableParent.NewTableChild.class).execute()).get(0);
    assertEquals("a", child.getAString());
    assertEquals("b", child.getBString());
  }

  public void testDeleteChild_SubclassParentNewTableChild() {
    Entity e = new Entity(kindForClass(SubclassTableParent.NewTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.NewTableChild child = pm.getObjectById(
        SubclassTableParent.NewTableChild.class, e.getKey());
    pm.deletePersistent(child);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testUpdateChild_SubclassParentNewTableChild() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(SubclassTableParent.NewTableChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    SubclassTableParent.NewTableChild child = pm.getObjectById(SubclassTableParent.NewTableChild.class, e.getKey());
    child.setAString("not a");
    child.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  private void assertUnsupportedByGAE(Object obj) {
    assertUnsupported(obj, DatastoreManager.UnsupportedInheritanceStrategyException.class);
  }

  private void assertUnsupported(Object obj, Class<? extends Exception> expectedCause) {
    switchDatasource(PersistenceManagerFactoryName.transactional);
    beginTxn();
    try {
      pm.makePersistent(obj);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause().getClass().getName(), expectedCause.isAssignableFrom(e.getCause().getClass()));
    }
    rollbackTxn();
  }
}
