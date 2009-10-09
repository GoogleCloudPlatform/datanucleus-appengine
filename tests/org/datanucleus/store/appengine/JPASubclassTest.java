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

import org.datanucleus.exceptions.NoPersistenceInformationException;
import org.datanucleus.test.SubclassesJPA.Child;
import org.datanucleus.test.SubclassesJPA.Grandchild;
import org.datanucleus.test.SubclassesJPA.Joined;
import org.datanucleus.test.SubclassesJPA.JoinedChild;
import org.datanucleus.test.SubclassesJPA.MappedSuperclassChild;
import org.datanucleus.test.SubclassesJPA.MappedSuperclassParent;
import org.datanucleus.test.SubclassesJPA.Parent;
import org.datanucleus.test.SubclassesJPA.SingleTable;
import org.datanucleus.test.SubclassesJPA.SingleTableChild;
import org.datanucleus.test.SubclassesJPA.TablePerClass;
import org.datanucleus.test.SubclassesJPA.TablePerClassChild;
import org.datanucleus.test.SubclassesJPA.TablePerClassGrandchild;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPASubclassTest extends JPATestCase {

  public void testUnsupportedStrategies_GAE() {
    // Child classes need to go first due to datanuc runtime enhancer weirdness
    assertUnsupportedByGAE(new JoinedChild());
    assertUnsupportedByGAE(new SingleTableChild());
  }

  public void testGrandchildren() throws Exception {
    testGrandchild(new TablePerClassGrandchild());
  }

  public void testChildren() throws Exception {
    testChild(new TablePerClassChild());
    testChild(new MappedSuperclassChild());
  }

  public void testParents() throws Exception {
    testParent(new TablePerClass());
    testParent(new Joined());
    testParent(new SingleTable());
  }

  public void testInsertParent_MappedSuperclass() throws EntityNotFoundException {
    MappedSuperclassParent parent = new MappedSuperclassParent();
    parent.setAString("a");
    beginTxn();
    try {
      em.persist(parent);
      commitTxn();
      fail("expected pe");
    } catch (PersistenceException pe) {
      assertTrue(pe.getCause() instanceof NoPersistenceInformationException);
    }
  }

  public void testAttributeOverride() throws EntityNotFoundException {
    MappedSuperclassChild child = new MappedSuperclassChild();
    child.setOverriddenString("overridden");
    beginTxn();
    em.persist(child);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(child.getClass()), child.getId()));
    assertEquals("overridden", e.getProperty("overridden_string"));
    assertFalse(e.hasProperty("overriddenString"));
  }

  private void assertUnsupportedByGAE(Object obj) {
    switchDatasource(EntityManagerFactoryName.transactional_ds_non_transactional_ops_not_allowed);
    beginTxn();
    em.persist(obj);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      assertTrue(e.getCause() instanceof DatastoreManager.UnsupportedInheritanceStrategyException);
      // good
    }
    rollbackTxn();
  }

  private void testInsertParent(Parent parent) throws Exception {
    parent.setAString("a");
    beginTxn();
    em.persist(parent);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  private void testInsertChild(Child child) throws Exception {
    child.setAString("a");
    child.setBString("b");
    beginTxn();
    em.persist(child);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(child.getClass()), child.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }

  private void testInsertGrandchild(Grandchild grandchild) throws Exception {
    grandchild.setAString("a");
    grandchild.setBString("b");
    grandchild.setCString("c");
    beginTxn();
    em.persist(grandchild);
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
    Parent parent = em.find(parentClass, e.getKey());
    assertEquals(parentClass, parent.getClass());
    assertEquals("a", parent.getAString());
    commitTxn();
  }

  private void testFetchChild(Class<? extends Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    Child child = em.find(childClass, e.getKey());
    assertEquals(childClass, child.getClass());
    assertEquals("a", child.getAString());
    assertEquals("b", child.getBString());
    commitTxn();
  }

  private void testFetchGrandchild(Class<? extends Grandchild> grandchildClass) {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    Grandchild grandchild = em.find(grandchildClass, e.getKey());
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
    Query q = em.createQuery("select from " + parentClass.getName() + " where aString = :p");
    q.setParameter("p", "a2");
    Parent parent = (Parent) q.getSingleResult();
    assertEquals(parentClass, parent.getClass());
    assertEquals("a2", parent.getAString());
    commitTxn();
  }

  private void testQueryChild(Class<? extends Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a2");
    e.setProperty("bString", "b2");
    ldth.ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + childClass.getName() + " where aString = :p");
    q.setParameter("p", "a2");
    Child child = (Child) q.getSingleResult();
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    q = em.createQuery("select from " + childClass.getName() + " where bString = :p");
    q.setParameter("p", "b2");
    child = (Child) q.getSingleResult();
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    commitTxn();
  }

  private void testQueryGrandchild(Class<? extends Grandchild> grandchildClass) {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a2");
    e.setProperty("bString", "b2");
    e.setProperty("cString", "c2");
    ldth.ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + grandchildClass.getName() + " where aString = :p");
    q.setParameter("p", "a2");
    Grandchild grandchild = (Grandchild) q.getSingleResult();

    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b2", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    q = em.createQuery("select from " + grandchildClass.getName() + " where bString = :p");
    q.setParameter("p", "b2");
    grandchild = (Grandchild) q.getSingleResult();

    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b2", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    q = em.createQuery("select from " + grandchildClass.getName() + " where cString = :p");
    q.setParameter("p", "c2");
    grandchild = (Grandchild) q.getSingleResult();

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
    Parent parent = em.find(parentClass, e.getKey());
    em.remove(parent);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  private void testDeleteChild(Class<? extends Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    Child child = em.find(childClass, e.getKey());
    em.remove(child);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  private void testDeleteGrandchild(Class<? extends Grandchild> grandchildClass) {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    Child child = em.find(grandchildClass, e.getKey());
    em.remove(child);
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
    Parent parent = em.find(parentClass, e.getKey());
    parent.setAString("not a");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
  }

  private void testUpdateChild(Class<? extends Child> childClass) throws Exception {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    Child child = em.find(childClass, e.getKey());
    child.setAString("not a");
    child.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  private void testUpdateGrandchild(Class<? extends Grandchild> grandchildClass) throws Exception {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    Grandchild grandchild = em.find(grandchildClass, e.getKey());
    grandchild.setAString("not a");
    grandchild.setBString("not b");
    grandchild.setCString("not c");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
    assertEquals("not c", e.getProperty("cString"));
  }

  private void testGrandchild(Grandchild grandchild) throws Exception {
    testInsertGrandchild(grandchild);
    testUpdateGrandchild(grandchild.getClass());
    testDeleteGrandchild(grandchild.getClass());
    testFetchGrandchild(grandchild.getClass());
    testQueryGrandchild(grandchild.getClass());
  }

  private void testChild(Child child) throws Exception {
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