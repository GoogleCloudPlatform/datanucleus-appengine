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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.IsEmbeddedWithEmbeddedSuperclass;
import com.google.appengine.datanucleus.test.IsEmbeddedWithEmbeddedSuperclass2;
import com.google.appengine.datanucleus.test.SubclassesJPA;
import com.google.appengine.datanucleus.test.SubclassesJPA.Child;
import com.google.appengine.datanucleus.test.SubclassesJPA.ChildEmbeddedInTablePerClass;
import com.google.appengine.datanucleus.test.SubclassesJPA.DurableChild;
import com.google.appengine.datanucleus.test.SubclassesJPA.Grandchild;
import com.google.appengine.datanucleus.test.SubclassesJPA.Joined;
import com.google.appengine.datanucleus.test.SubclassesJPA.JoinedChild;
import com.google.appengine.datanucleus.test.SubclassesJPA.MappedSuperclassChild;
import com.google.appengine.datanucleus.test.SubclassesJPA.MappedSuperclassParent;
import com.google.appengine.datanucleus.test.SubclassesJPA.Parent;
import com.google.appengine.datanucleus.test.SubclassesJPA.SingleTable;
import com.google.appengine.datanucleus.test.SubclassesJPA.SingleTableChild;
import com.google.appengine.datanucleus.test.SubclassesJPA.TablePerClass;
import com.google.appengine.datanucleus.test.SubclassesJPA.TablePerClassChild;
import com.google.appengine.datanucleus.test.SubclassesJPA.TablePerClassGrandchild;
import com.google.appengine.datanucleus.test.SubclassesJPA.TablePerClassParentWithEmbedded;

import org.datanucleus.exceptions.NoPersistenceInformationException;

import java.util.List;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPASubclassTest extends JPATestCase {

  public void testUnsupportedStrategies_GAE() {
    // Child classes need to go first due to datanuc runtime enhancer weirdness
    assertUnsupportedByGAE(new JoinedChild(), "JOINED");
  }

  public void testGrandchildren() throws Exception {
    testGrandchild(new TablePerClassGrandchild());
  }

  public void testChildren() throws Exception {
    testChild(new TablePerClassChild(), null);
    testChild(new MappedSuperclassChild(), null);
    testChild(new SingleTableChild(), SingleTableChild.class.getName());
  }

  public void testParents() throws Exception {
    testParent(new TablePerClass(), null);
    testParent(new Joined(), null);
    testParent(new SingleTable(), SingleTable.class.getName());
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
    Entity e = ds.get(KeyFactory.createKey(kindForClass(child.getClass()), child.getId()));
    assertEquals("overridden", e.getProperty("overridden_string"));
    assertFalse(e.hasProperty("overriddenString"));
  }

  public void testEmbedded_Child() throws Exception {
    ChildEmbeddedInTablePerClass child = new ChildEmbeddedInTablePerClass();
    child.setAString("aString");
    child.setBString("bString");
    IsEmbeddedWithEmbeddedSuperclass embedded = new IsEmbeddedWithEmbeddedSuperclass();
    embedded.setVal0("embedded val 0");
    embedded.setVal1("embedded val 1");
    child.setEmbedded(embedded);
    SubclassesJPA.IsEmbeddedBase embeddedBase = new SubclassesJPA.IsEmbeddedBase();
    embeddedBase.setVal0("embedded base val 0");
    child.setEmbeddedBase(embeddedBase);
    IsEmbeddedWithEmbeddedSuperclass2 embedded2 = new IsEmbeddedWithEmbeddedSuperclass2();
    embedded2.setVal2("embedded val 2");
    embedded2.setVal3("embedded val 3");
    child.setEmbedded2(embedded2);
    SubclassesJPA.IsEmbeddedBase2
        embeddedBase2 = new SubclassesJPA.IsEmbeddedBase2();
    embeddedBase2.setVal2("embedded base val 2");
    child.setEmbeddedBase2(embeddedBase2);
    beginTxn();
    em.persist(child);
    commitTxn();
    Key key = KeyFactory.createKey(kindForClass(child.getClass()), child.getId());
    Entity e = ds.get(key);
    assertEquals("aString", e.getProperty("aString"));
    assertEquals("bString", e.getProperty("bString"));
    assertEquals("embedded val 0", e.getProperty("val0"));
    assertEquals("embedded val 1", e.getProperty("val1"));
    assertEquals("embedded base val 0", e.getProperty("VAL0"));
    assertEquals("embedded val 2", e.getProperty("val2"));
    assertEquals("embedded val 3", e.getProperty("val3"));
    assertEquals("embedded base val 2", e.getProperty("VAL2"));
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    child = em.find(child.getClass(), child.getId());
    assertEmbeddedChildContents(child);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    Query q = em.createQuery("select from " + child.getClass().getName() + " where embedded.val1 = :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setParameter("p", "embedded val 1");
    child = (ChildEmbeddedInTablePerClass) q.getSingleResult();
    assertEmbeddedChildContents(child);

    q = em.createQuery("select from " + child.getClass().getName() + " where embedded.val0 = :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setParameter("p", "embedded val 0");
    child = (ChildEmbeddedInTablePerClass) q.getSingleResult();
    assertEmbeddedChildContents(child);

    q = em.createQuery("select from " + child.getClass().getName() + " where embeddedBase.val0 = :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setParameter("p", "embedded base val 0");
    child = (ChildEmbeddedInTablePerClass) q.getSingleResult();
    assertEmbeddedChildContents(child);

    q = em.createQuery("select from " + child.getClass().getName() + " where embedded2.val2 = :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setParameter("p", "embedded val 2");
    child = (ChildEmbeddedInTablePerClass) q.getSingleResult();
    assertEmbeddedChildContents(child);

    q = em.createQuery("select from " + child.getClass().getName() + " where embedded2.val3 = :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setParameter("p", "embedded val 3");
    child = (ChildEmbeddedInTablePerClass) q.getSingleResult();
    assertEmbeddedChildContents(child);

    q = em.createQuery("select from " + child.getClass().getName() + " where embeddedBase2.val2 = :p "
      + "order by embedded.val1 desc, embedded.val0 desc, embeddedBase.val0 desc, "
      + "embedded2.val2 desc, embedded2.val3 desc, embeddedBase2.val2");
    q.setParameter("p", "embedded base val 2");
    child = (ChildEmbeddedInTablePerClass) q.getSingleResult();
    assertEmbeddedChildContents(child);

    q = em.createQuery("select embedded.val1, embedded.val0, embeddedBase.val0, embedded2.val2, embedded2.val3, embeddedBase2.val2 from " +
                    child.getClass().getName() + " where embeddedBase2.val2 = :p");
    q.setParameter("p", "embedded base val 2");

    Object[] result = (Object[]) q.getSingleResult();
    assertEquals("embedded val 1", result[0]);
    assertEquals("embedded val 0", result[1]);
    assertEquals("embedded base val 0", result[2]);
    assertEquals("embedded val 2", result[3]);
    assertEquals("embedded val 3", result[4]);
    assertEquals("embedded base val 2", result[5]);

    em.remove(child);
    commitTxn();
    try {
      ds.get(key);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  public void testEmbedded_Parent() throws Exception {
    TablePerClassParentWithEmbedded parent = new TablePerClassParentWithEmbedded();
    parent.setAString("aString");
    IsEmbeddedWithEmbeddedSuperclass embedded = new IsEmbeddedWithEmbeddedSuperclass();
    embedded.setVal0("embedded val 0");
    embedded.setVal1("embedded val 1");
    parent.setEmbedded(embedded);
    SubclassesJPA.IsEmbeddedBase
        embeddedBase = new SubclassesJPA.IsEmbeddedBase();
    embeddedBase.setVal0("embedded base val 0");
    parent.setEmbeddedBase(embeddedBase);
    beginTxn();
    em.persist(parent);
    commitTxn();
    Key key = KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId());
    Entity e = ds.get(key);
    assertEquals("aString", e.getProperty("aString"));
    assertEquals("embedded val 0", e.getProperty("val0"));
    assertEquals("embedded val 1", e.getProperty("val1"));
    assertEquals("embedded base val 0", e.getProperty("VAL0"));
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    parent = em.find(parent.getClass(), parent.getId());
    assertEmbeddedParentContents(parent);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    Query q = em.createQuery("select from " + parent.getClass().getName() + " where embedded.val1 = :p "
      + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setParameter("p", "embedded val 1");
    parent = (TablePerClassParentWithEmbedded) q.getSingleResult();
    assertEmbeddedParentContents(parent);

    q = em.createQuery("select from " + parent.getClass().getName() + " where embedded.val0 = :p "
      + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setParameter("p", "embedded val 0");
    parent = (TablePerClassParentWithEmbedded) q.getSingleResult();
    assertEmbeddedParentContents(parent);

    q = em.createQuery("select from " + parent.getClass().getName() + " where embeddedBase.val0 = :p "
      + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setParameter("p", "embedded base val 0");
    parent = (TablePerClassParentWithEmbedded) q.getSingleResult();
    assertEmbeddedParentContents(parent);

    q = em.createQuery("select embedded.val1, embedded.val0, embeddedBase.val0 from " +
                    parent.getClass().getName() + " where embeddedBase.val0 = :p "
      + "order by embedded.val1 desc, embedded.val0 asc, embeddedBase.val0 desc");
    q.setParameter("p", "embedded base val 0");

    Object[] result = (Object[]) q.getSingleResult();
    assertEquals("embedded val 1", result[0]);
    assertEquals("embedded val 0", result[1]);
    assertEquals("embedded base val 0", result[2]);

    em.remove(parent);
    commitTxn();
    try {
      ds.get(key);
      fail("expected enfe");
    } catch (EntityNotFoundException enfe) {
      // good
    }
  }

  public void testNondurableParent() {
    DurableChild dc = new DurableChild();
    dc.setStr("yar");
    beginTxn();
    em.persist(dc);
    commitTxn();
    beginTxn();
    dc = em.find(DurableChild.class, dc.getId());
    assertEquals("yar", dc.getStr());
  }

  // This is absurd, but if the signature of this method and the one below
  // refers to the actual type we want the runtime enhancer gets totally
  // confused.  
  private void assertEmbeddedParentContents(Object obj) {
    TablePerClassParentWithEmbedded parentWithEmbedded = (TablePerClassParentWithEmbedded) obj;
    assertEquals("aString", parentWithEmbedded.getAString());
    assertEquals("embedded val 0", parentWithEmbedded.getEmbedded().getVal0());
    assertEquals("embedded val 1", parentWithEmbedded.getEmbedded().getVal1());
    assertEquals("embedded base val 0", parentWithEmbedded.getEmbeddedBase().getVal0());
  }

  private void assertEmbeddedChildContents(Object obj) {
    ChildEmbeddedInTablePerClass child = (ChildEmbeddedInTablePerClass) obj;
    assertEquals("bString", child.getBString());
    assertEquals("embedded val 2", child.getEmbedded2().getVal2());
    assertEquals("embedded val 3", child.getEmbedded2().getVal3());
    assertEquals("embedded base val 2", child.getEmbeddedBase2().getVal2());
    assertEmbeddedParentContents(child);
  }

  private void assertUnsupportedByGAE(Object obj, String msgMustContain) {
    switchDatasource(EntityManagerFactoryName.transactional_ds_non_transactional_ops_not_allowed);
    beginTxn();
    em.persist(obj);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof DatastoreManager.UnsupportedInheritanceStrategyException);
      assertTrue(e.getCause().getMessage().contains(msgMustContain));
    }
    rollbackTxn();
  }

  private void testInsertParent(Parent parent) throws Exception {
    parent.setAString("a");
    beginTxn();
    em.persist(parent);
    commitTxn();

    Entity e = ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  private void testInsertChild(Child child) throws Exception {
    child.setAString("a");
    child.setBString("b");
    beginTxn();
    em.persist(child);
    commitTxn();

    Entity e = ds.get(KeyFactory.createKey(kindForClass(child.getClass()), child.getId()));
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

    Entity e = ds.get(KeyFactory.createKey(kindForClass(grandchild.getClass()), grandchild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
    assertEquals("c", e.getProperty("cString"));
  }

  private void testFetchParent(Class<? extends Parent> parentClass) {
    Entity e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "a");
    ds.put(e);

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
    ds.put(e);

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
    ds.put(e);

    beginTxn();
    Grandchild grandchild = em.find(grandchildClass, e.getKey());
    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a", grandchild.getAString());
    assertEquals("b", grandchild.getBString());
    assertEquals("c", grandchild.getCString());
    commitTxn();
  }

  private void testQueryParent(Class<? extends Parent> parentClass, String discriminator) {
    Entity e = new Entity(kindForClass(parentClass));
    if (discriminator != null) {
      e.setProperty("TYPE", discriminator);
    }
    e.setProperty("aString", "z8");
    ds.put(e);

    e = new Entity(kindForClass(parentClass));
    if (discriminator != null) {
      e.setProperty("TYPE", discriminator);
    }
    e.setProperty("aString", "z9");
    ds.put(e);

    beginTxn();
    Query q = em.createQuery("select from " + parentClass.getName() + " where aString = :p");
    q.setParameter("p", "z8");
    Parent parent = (Parent) q.getSingleResult();
    assertEquals(parentClass, parent.getClass());
    assertEquals("z8", parent.getAString());

    q = em.createQuery("select from " + parentClass.getName() + " where aString >= :p order by aString desc");
    q.setParameter("p", "z8");
    List<Parent> parents = q.getResultList();
    assertEquals("z9", parents.get(0).getAString());
    assertEquals("z8", parents.get(1).getAString());

    q = em.createQuery("select aString from " + parentClass.getName() + " where aString = :p");
    q.setParameter("p", "z8");
    String result = (String) q.getSingleResult();
    assertEquals("z8", result);

    commitTxn();

  }

  private void testQueryChild(Class<? extends Child> childClass, String discriminator) {
    Entity e1 = new Entity(kindForClass(childClass));
    if (discriminator != null) {
      e1.setProperty("TYPE", discriminator);
    }
    e1.setProperty("aString", "a2");
    e1.setProperty("bString", "b2");
    ds.put(e1);

    Entity e2 = new Entity(kindForClass(childClass));
    if (discriminator != null) {
      e2.setProperty("TYPE", discriminator);
    }
    e2.setProperty("aString", "a2");
    e2.setProperty("bString", "b3");
    ds.put(e2);

    Entity e3 = new Entity(kindForClass(childClass));
    if (discriminator != null) {
      e3.setProperty("TYPE", discriminator);
    }
    e3.setProperty("aString", "a2");
    e3.setProperty("bString", "b3");
    ds.put(e3);

    beginTxn();
    Query q = em.createQuery("select from " + childClass.getName() + " where aString = :p");
    q.setParameter("p", "a2");
    Child child = (Child) q.getResultList().get(0);
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    q = em.createQuery("select from " + childClass.getName() + " where bString = :p");
    q.setParameter("p", "b2");
    child = (Child) q.getSingleResult();
    assertEquals(childClass, child.getClass());
    assertEquals("a2", child.getAString());
    assertEquals("b2", child.getBString());

    List<Child> kids = ((List<Child>) em.createQuery(
        "select from " + childClass.getName() + " where aString = 'a2' order by bString desc").getResultList());
    assertEquals(3, kids.size());
    assertEquals(e2.getKey().getId(), kids.get(0).getId().longValue());
    assertEquals(e3.getKey().getId(), kids.get(1).getId().longValue());
    assertEquals(e1.getKey().getId(), kids.get(2).getId().longValue());

    kids = ((List<Child>) em.createQuery(
        "select from " + childClass.getName() + " where aString = 'a2' order by aString desc").getResultList());
    assertEquals(3, kids.size());
    assertEquals(e1.getKey().getId(), kids.get(0).getId().longValue());
    assertEquals(e2.getKey().getId(), kids.get(1).getId().longValue());
    assertEquals(e3.getKey().getId(), kids.get(2).getId().longValue());

    q = em.createQuery("select bString, aString from " + childClass.getName() + " where bString = :p");
    q.setParameter("p", "b2");
    Object[] result = (Object[]) q.getSingleResult();
    assertEquals(2, result.length);
    assertEquals("b2", result[0]);
    assertEquals("a2", result[1]);

    commitTxn();
  }

  private void testQueryGrandchild(Class<? extends Grandchild> grandchildClass) {
    Entity e1 = new Entity(kindForClass(grandchildClass));
    e1.setProperty("aString", "a2");
    e1.setProperty("bString", "b1");
    e1.setProperty("cString", "c2");
    ds.put(e1);

    Entity e2 = new Entity(kindForClass(grandchildClass));
    e2.setProperty("aString", "a2");
    e2.setProperty("bString", "b3");
    e2.setProperty("cString", "c3");
    ds.put(e2);

    Entity e3 = new Entity(kindForClass(grandchildClass));
    e3.setProperty("aString", "a2");
    e3.setProperty("bString", "b2");
    e3.setProperty("cString", "c3");
    ds.put(e3);

    beginTxn();
    Query q = em.createQuery(
        "select from " + grandchildClass.getName() + " where aString = :p");
    q.setParameter("p", "a2");
    Grandchild grandchild = (Grandchild) q.getResultList().get(0);

    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b1", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    q = em.createQuery(
        "select from " + grandchildClass.getName() + " where bString = :p");
    q.setParameter("p", "b2");
    grandchild = (Grandchild) q.getSingleResult();

    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b2", grandchild.getBString());
    assertEquals("c3", grandchild.getCString());

    q = em.createQuery(
        "select from " + grandchildClass.getName() + " where cString = :p");
    q.setParameter("p", "c2");
    grandchild = (Grandchild) q.getSingleResult();

    assertEquals(grandchildClass, grandchild.getClass());
    assertEquals("a2", grandchild.getAString());
    assertEquals("b1", grandchild.getBString());
    assertEquals("c2", grandchild.getCString());

    List<Grandchild> grandkids = ((List<Grandchild>) em.createQuery(
        "select from " + grandchildClass.getName() + " where aString = 'a2' order by bString desc").getResultList());
    assertEquals(3, grandkids.size());
    assertEquals(e2.getKey().getId(), grandkids.get(0).getId().longValue());
    assertEquals(e3.getKey().getId(), grandkids.get(1).getId().longValue());
    assertEquals(e1.getKey().getId(), grandkids.get(2).getId().longValue());

    grandkids = ((List<Grandchild>) em.createQuery(
        "select from " + grandchildClass.getName() + " where aString = 'a2' order by aString desc").getResultList());
    assertEquals(3, grandkids.size());
    assertEquals(e1.getKey().getId(), grandkids.get(0).getId().longValue());
    assertEquals(e2.getKey().getId(), grandkids.get(1).getId().longValue());
    assertEquals(e3.getKey().getId(), grandkids.get(2).getId().longValue());

    grandkids = ((List<Grandchild>) em.createQuery(
        "select from " + grandchildClass.getName() + " where aString = 'a2' order by cString desc").getResultList());
    assertEquals(3, grandkids.size());
    assertEquals(e2.getKey().getId(), grandkids.get(0).getId().longValue());
    assertEquals(e3.getKey().getId(), grandkids.get(1).getId().longValue());
    assertEquals(e1.getKey().getId(), grandkids.get(2).getId().longValue());

    q = em.createQuery("select bString, aString, cString from " + grandchildClass.getName() + " where cString = :p");
    q.setParameter("p", "c2");
    Object[] result = (Object[]) q.getSingleResult();
    assertEquals(3, result.length);
    assertEquals("b1", result[0]);
    assertEquals("a2", result[1]);
    assertEquals("c2", result[2]);

    commitTxn();
  }

  private void testDeleteParent(Class<? extends Parent> parentClass) {
    Entity e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "a");
    ds.put(e);

    beginTxn();
    Parent parent = em.find(parentClass, e.getKey());
    em.remove(parent);
    commitTxn();
    try {
      ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  private void testDeleteChild(Class<? extends Child> childClass) {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ds.put(e);

    beginTxn();
    Child child = em.find(childClass, e.getKey());
    em.remove(child);
    commitTxn();
    try {
      ds.get(e.getKey());
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
    ds.put(e);

    beginTxn();
    Child child = em.find(grandchildClass, e.getKey());
    em.remove(child);
    commitTxn();
    try {
      ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  private void testUpdateParent(Class<? extends Parent> parentClass) throws Exception {
    Entity e = new Entity(kindForClass(parentClass));
    e.setProperty("aString", "a");
    ds.put(e);

    beginTxn();
    Parent parent = em.find(parentClass, e.getKey());
    parent.setAString("not a");
    commitTxn();
    e = ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
  }

  private void testUpdateChild(Class<? extends Child> childClass) throws Exception {
    Entity e = new Entity(kindForClass(childClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ds.put(e);

    beginTxn();
    Child child = em.find(childClass, e.getKey());
    child.setAString("not a");
    child.setBString("not b");
    commitTxn();
    e = ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  private void testUpdateGrandchild(Class<? extends Grandchild> grandchildClass) throws Exception {
    Entity e = new Entity(kindForClass(grandchildClass));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ds.put(e);

    beginTxn();
    Grandchild grandchild = em.find(grandchildClass, e.getKey());
    grandchild.setAString("not a");
    grandchild.setBString("not b");
    grandchild.setCString("not c");
    commitTxn();
    e = ds.get(e.getKey());
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

  private void testChild(Child child, String discriminator) throws Exception {
    testInsertChild(child);
    testUpdateChild(child.getClass());
    testDeleteChild(child.getClass());
    testFetchChild(child.getClass());
    testQueryChild(child.getClass(), discriminator);
  }

  private void testParent(Parent parent, String discriminator) throws Exception {
    testInsertParent(parent);
    testUpdateParent(parent.getClass());
    testDeleteParent(parent.getClass());
    testFetchParent(parent.getClass());
    testQueryParent(parent.getClass(), discriminator);
  }
}
