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
import org.datanucleus.test.SubclassesJPA.Joined;
import org.datanucleus.test.SubclassesJPA.JoinedChild;
import org.datanucleus.test.SubclassesJPA.MappedSuperclassChild;
import org.datanucleus.test.SubclassesJPA.MappedSuperclassParent;
import org.datanucleus.test.SubclassesJPA.SingleTable;
import org.datanucleus.test.SubclassesJPA.SingleTableChild;
import org.datanucleus.test.SubclassesJPA.TablePerClass;
import org.datanucleus.test.SubclassesJPA.TablePerClassChild;
import org.datanucleus.test.SubclassesJPA.TablePerClassGrandchild;

import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPASubclassTest extends JPATestCase {

  public void testUnsupportedStrategies_GAE() {
    // Child classes need to go first due to datanuc runtime enhancer weirdness
    assertUnsupportedByGAE(new JoinedChild());
    assertUnsupportedByGAE(new SingleTableChild());
  }

  public void testInsertChild_TablePerClass() throws EntityNotFoundException {
    TablePerClassChild tablePerClassChild = new TablePerClassChild();
    tablePerClassChild.setAString("a");
    tablePerClassChild.setBString("b");
    beginTxn();
    em.persist(tablePerClassChild);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(TablePerClassChild.class), tablePerClassChild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }

  public void testInsertGrandChild_TablePerClass() throws EntityNotFoundException {
    TablePerClassGrandchild tablePerClassGrandchild = new TablePerClassGrandchild();
    tablePerClassGrandchild.setAString("a");
    tablePerClassGrandchild.setBString("b");
    tablePerClassGrandchild.setCString("c");
    beginTxn();
    em.persist(tablePerClassGrandchild);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(TablePerClassGrandchild.class), tablePerClassGrandchild.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
    assertEquals("c", e.getProperty("cString"));
  }

  public void testInsertParent_TablePerClass() throws EntityNotFoundException {
    TablePerClass tablePerClass = new TablePerClass();
    tablePerClass.setAString("a");
    beginTxn();
    em.persist(tablePerClass);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(TablePerClass.class), tablePerClass.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  public void testFetchChild_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassChild
        tablePerClassChild = em.find(TablePerClassChild.class, e.getKey());
    assertEquals("a", tablePerClassChild.getAString());
    assertEquals("b", tablePerClassChild.getBString());
  }

  public void testFetchGrandChild_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClassGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassGrandchild
        tablePerClassGrandchild = em.find(TablePerClassGrandchild.class, e.getKey());
    assertEquals("a", tablePerClassGrandchild.getAString());
    assertEquals("b", tablePerClassGrandchild.getBString());
    assertEquals("c", tablePerClassGrandchild.getCString());
  }

  public void testFetchParent_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClass.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    TablePerClass
        tablePerClass = em.find(TablePerClass.class, e.getKey());
    assertEquals("a", tablePerClass.getAString());
  }

  public void testQueryChild_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassChild
        tablePerClassChild = (TablePerClassChild) em.createQuery("select from " + TablePerClassChild.class.getName()).getSingleResult();
    assertEquals("a", tablePerClassChild.getAString());
    assertEquals("b", tablePerClassChild.getBString());
  }

  public void testQueryGrandChild_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClassGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassGrandchild
        tablePerClassGrandchild = (TablePerClassGrandchild) em.createQuery("select from " + TablePerClassGrandchild.class.getName()).getSingleResult();
    assertEquals("a", tablePerClassGrandchild.getAString());
    assertEquals("b", tablePerClassGrandchild.getBString());
    assertEquals("c", tablePerClassGrandchild.getCString());
  }

  public void testQueryParent_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClass.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    TablePerClass
        tablePerClass = (TablePerClass) em.createQuery("select from " + TablePerClass.class.getName()).getSingleResult();
    assertEquals("a", tablePerClass.getAString());
  }

  public void testDeleteChild_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassChild
        tablePerClassChild = em.find(TablePerClassChild.class, e.getKey());
    em.remove(tablePerClassChild);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testDeleteGrandChild_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClassGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassGrandchild
        tablePerClassGrandchild = em.find(TablePerClassGrandchild.class, e.getKey());
    em.remove(tablePerClassGrandchild);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testDeleteParent_TablePerClass() {
    Entity e = new Entity(kindForClass(TablePerClass.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    TablePerClass tablePerClass = em.find(TablePerClass.class, e.getKey());
    em.remove(tablePerClass);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testUpdateChild_TablePerClass() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(TablePerClassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassChild
        tablePerClassChild = em.find(TablePerClassChild.class, e.getKey());
    tablePerClassChild.setAString("not a");
    tablePerClassChild.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
  }

  public void testUpdateGrandChild_TablePerClass() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(TablePerClassGrandchild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    e.setProperty("cString", "c");
    ldth.ds.put(e);

    beginTxn();
    TablePerClassGrandchild
        tablePerClassGrandchild = em.find(TablePerClassGrandchild.class, e.getKey());
    tablePerClassGrandchild.setAString("not a");
    tablePerClassGrandchild.setBString("not b");
    tablePerClassGrandchild.setCString("not c");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
    assertEquals("not c", e.getProperty("cString"));
  }

  public void testUpdateParent_TablePerClass() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(TablePerClass.class));
    e.setProperty("aString", "a");
    ldth.ds.put(e);

    beginTxn();
    TablePerClass
        tablePerClass = em.find(TablePerClass.class, e.getKey());
    tablePerClass.setAString("not a");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
  }

  public void testInsertParent_Joined() throws EntityNotFoundException {
    Joined joined = new Joined();
    joined.setAString("a");
    beginTxn();
    em.persist(joined);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(Joined.class), joined.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  public void testInsertParent_SingleTable() throws EntityNotFoundException {
    SingleTable singleTable = new SingleTable();
    singleTable.setAString("a");
    beginTxn();
    em.persist(singleTable);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(SingleTable.class), singleTable.getId()));
    assertEquals("a", e.getProperty("aString"));
  }

  public void testInsertChild_MappedSuperclass() throws EntityNotFoundException {
    MappedSuperclassChild child = new MappedSuperclassChild();
    child.setAString("a");
    child.setBString("b");
    beginTxn();
    em.persist(child);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(kindForClass(MappedSuperclassChild.class), child.getId()));
    assertEquals("a", e.getProperty("aString"));
    assertEquals("b", e.getProperty("bString"));
  }

  public void testFetchChild_MappedSuperclass() {
    Entity e = new Entity(kindForClass(MappedSuperclassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    MappedSuperclassChild mappedSuperclassChild = em.find(MappedSuperclassChild.class, e.getKey());
    assertEquals("a", mappedSuperclassChild.getAString());
    assertEquals("b", mappedSuperclassChild.getBString());
  }

  public void testQueryChild_MappedSuperclass() {
    Entity e = new Entity(kindForClass(MappedSuperclassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    MappedSuperclassChild mappedSuperclassChild = (MappedSuperclassChild)
        em.createQuery("select from " + MappedSuperclassChild.class.getName()).getSingleResult();
    assertEquals("a", mappedSuperclassChild.getAString());
    assertEquals("b", mappedSuperclassChild.getBString());
  }

  public void testDeleteChild_MappedSuperclass() {
    Entity e = new Entity(kindForClass(MappedSuperclassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    MappedSuperclassChild mappedSuperclassChild = em.find(MappedSuperclassChild.class, e.getKey());
    em.remove(mappedSuperclassChild);
    commitTxn();
    try {
      ldth.ds.get(e.getKey());
      fail("expected exception");
    } catch (EntityNotFoundException e1) {
      // good
    }
  }

  public void testUpdateChild_MappedSuperclass() throws EntityNotFoundException {
    Entity e = new Entity(kindForClass(MappedSuperclassChild.class));
    e.setProperty("aString", "a");
    e.setProperty("bString", "b");
    ldth.ds.put(e);

    beginTxn();
    MappedSuperclassChild mappedSuperclassChild = em.find(MappedSuperclassChild.class, e.getKey());
    mappedSuperclassChild.setAString("not a");
    mappedSuperclassChild.setBString("not b");
    commitTxn();
    e = ldth.ds.get(e.getKey());
    assertEquals("not a", e.getProperty("aString"));
    assertEquals("not b", e.getProperty("bString"));
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
}