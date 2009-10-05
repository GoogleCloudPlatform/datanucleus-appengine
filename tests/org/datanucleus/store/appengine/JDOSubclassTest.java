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

import org.datanucleus.test.SubclassesJDO;
import org.datanucleus.test.SubclassesJDO.CompleteParent;
import org.datanucleus.test.SubclassesJDO.NewParent;
import org.datanucleus.test.SubclassesJDO.SubParent;

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
// TODO(maxr): Tests where the key isn't on the parent
// TODO(maxr): Tests where there are relationships on the parent/child/grandchild
// TODO(maxr): Tests where there are embedded fields on the parent/child/grandchild
public class JDOSubclassTest extends JDOTestCase {

  public void testInsertChild_KeyOnParent() throws EntityNotFoundException {
    CompleteParent.CompleteChild completeTableChild = new CompleteParent.CompleteChild();
    completeTableChild.setAString("a");
    completeTableChild.setBString("b");
    beginTxn();
    pm.makePersistent(completeTableChild);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(CompleteParent.CompleteChild.class), completeTableChild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }

  public void testInsertGrandChild_KeyOnParent() throws EntityNotFoundException {
    CompleteParent.CompleteGrandchild completeTableGrandchild = new CompleteParent.CompleteGrandchild();
    completeTableGrandchild.setAString("a");
    completeTableGrandchild.setBString("b");
    completeTableGrandchild.setCString("c");
    beginTxn();
    pm.makePersistent(completeTableGrandchild);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(CompleteParent.CompleteGrandchild.class), completeTableGrandchild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
    assertEquals("c", e.getProperty("cString"));
  }

  public void testUnsupportedStrategies_GAE() {
    assertUnsupportedByGAE(new NewParent.CompleteChild());
    assertUnsupportedByGAE(new NewParent.NewChild());

    assertUnsupportedByGAE(new SubParent.CompleteChild());
    assertUnsupportedByGAE(new SubParent.NewChild());
    assertUnsupportedByGAE(new SubParent());

    // The datanucleus enhancer doesn't handle these correctly
    // and we end up with an IAE way before we get any callbacks that would
    // allow us to detect the problem.
//    assertUnsupportedByDataNucleus(new NewParent.SubChild());
//    assertUnsupportedByDataNucleus(new SubParent.SubChild());
  }

  public void testInsertParent_KeyOnParent() throws EntityNotFoundException {
    CompleteParent completeParent = new CompleteParent();
    completeParent.setAString("a");
    beginTxn();
    pm.makePersistent(completeParent);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(CompleteParent.class), completeParent.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  public void testFetchChild_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent.CompleteChild completeTableChild = pm.getObjectById(CompleteParent.CompleteChild.class, e.getKey());
    assertEquals("a", completeTableChild.getAString());
    assertEquals("b", completeTableChild.getBString());
  }

  public void testFetchGrandChild_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent.CompleteGrandchild completeTableGrandchild = 
        pm.getObjectById(CompleteParent.CompleteGrandchild.class, e.getKey());
    assertEquals("a", completeTableGrandchild.getAString());
    assertEquals("b", completeTableGrandchild.getBString());
    assertEquals("c", completeTableGrandchild.getCString());
  }

  public void testFetchParent_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent
        completeParent = pm.getObjectById(CompleteParent.class, e.getKey());
    assertEquals("a", completeParent.getAString());
  }

  public void testQueryChild_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent.CompleteChild completeTableChild = ((List<CompleteParent.CompleteChild>)
        pm.newQuery(CompleteParent.CompleteChild.class).execute()).get(0);
    assertEquals("a", completeTableChild.getAString());
    assertEquals("b", completeTableChild.getBString());
  }

  public void testQueryGrandChild_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent.CompleteGrandchild grandchild = ((List<CompleteParent.CompleteGrandchild>)pm.newQuery(
        CompleteParent.CompleteGrandchild.class).execute()).get(0);
    assertEquals("a", grandchild.getAString());
    assertEquals("b", grandchild.getBString());
    assertEquals("c", grandchild.getCString());
  }

  public void testQueryParent_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent
        completeParent = ((List<CompleteParent>)pm.newQuery(CompleteParent.class).execute()).get(0);
    assertEquals("a", completeParent.getAString());
  }

  public void testDeleteChild_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent.CompleteChild completeTableChild = pm.getObjectById(CompleteParent.CompleteChild.class, e.getKey());
    pm.deletePersistent(completeTableChild);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testDeleteGrandChild_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.CompleteParent.CompleteGrandchild grandChild =
        pm.getObjectById(CompleteParent.CompleteGrandchild.class, e.getKey());
    pm.deletePersistent(grandChild);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testDeleteParent_KeyOnParent() {
    Entity e = new Entity(kindForClass(CompleteParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent completeParent = pm.getObjectById(CompleteParent.class, e.getKey());
    pm.deletePersistent(completeParent);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testUpdateChild_KeyOnParent() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent.CompleteChild completeTableChild = pm.getObjectById(CompleteParent.CompleteChild.class, e.getKey());
    completeTableChild.setAString("not a");
    completeTableChild.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  public void testUpdateGrandChild_KeyOnParent() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(CompleteParent.CompleteGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    SubclassesJDO.CompleteParent.CompleteGrandchild grandChild =
        pm.getObjectById(SubclassesJDO.CompleteParent.CompleteGrandchild.class, e.getKey());
    grandChild.setAString("not a");
    grandChild.setBString("not b");
    grandChild.setCString("not c");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
    assertEquals("not c", e.getProperty("cString"));
  }

  public void testUpdateParent_KeyOnParent() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(CompleteParent.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    CompleteParent completeParent = pm.getObjectById(CompleteParent.class, e.getKey());
    completeParent.setAString("not a");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
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
