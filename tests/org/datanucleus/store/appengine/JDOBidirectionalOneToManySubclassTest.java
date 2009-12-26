/*
 * Copyright (C) 2009 Max Ross.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.BidirectionalOneToManySubclassesJDO.Example1;
import org.datanucleus.test.BidirectionalOneToManySubclassesJDO.Example2;
import org.datanucleus.test.BidirectionalOneToManySubclassesJDO.Example3;
import org.datanucleus.test.BidirectionalOneToManySubclassesJDO.Example4;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JDOBidirectionalOneToManySubclassTest extends JDOTestCase {

  public void testExample1Subclass() throws EntityNotFoundException {
    // insertion
    Example1.B parent = new Example1.B();
    parent.setAString("a string");
    parent.setBString("b string");

    Example1.X child = new Example1.X();
    child.setXString("x string");
    parent.getChildren().add(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity childEntity = ldth.ds.get(child.getId());
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));
    assertEquals("b string", parentEntity.getProperty("bString"));
    assertEquals(Utils.newArrayList(childEntity.getKey()), parentEntity.getProperty("children"));

    assertEquals(1, childEntity.getProperties().size());
    assertEquals("x string", childEntity.getProperty("xString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    assertEquals("b string", parent.getBString());
    assertEquals(1, parent.getChildren().size());
    assertEquals(child.getId(), parent.getChildren().get(0).getId());
    commitTxn();

    beginTxn();
    child = pm.getObjectById(child.getClass(), child.getId());
    assertEquals("x string", child.getXString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(child.getClass()));
  }

  public void testExample1Superclass() throws EntityNotFoundException {
    // insertion
    Example1.A parent = new Example1.A();
    parent.setAString("a string");

    Example1.X child = new Example1.X();
    child.setXString("x string");
    parent.getChildren().add(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity childEntity = ldth.ds.get(child.getId());

    assertEquals(2, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));
    assertEquals(Utils.newArrayList(childEntity.getKey()), parentEntity.getProperty("children"));
    assertEquals(1, childEntity.getProperties().size());
    assertEquals("x string", childEntity.getProperty("xString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    assertEquals(1, parent.getChildren().size());
    assertEquals(child.getId(), parent.getChildren().get(0).getId());
    commitTxn();

    beginTxn();
    child = pm.getObjectById(child.getClass(), child.getId());
    assertEquals("x string", child.getXString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(child.getClass()));
  }

  public void testExample2Subclass() throws EntityNotFoundException {
    // insertion
    Example2.B parent = new Example2.B();
    parent.setAString("a string");
    parent.setBString("b string");

    Example2.Y child = new Example2.Y();
    child.setXString("x string");
    child.setYString("y string");
    parent.getChildren().add(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity childEntity = ldth.ds.get(child.getId());

    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));
    assertEquals("b string", parentEntity.getProperty("bString"));
    assertEquals(Utils.newArrayList(childEntity.getKey()), parentEntity.getProperty("children"));
    assertEquals(2, childEntity.getProperties().size());
    assertEquals("x string", childEntity.getProperty("xString"));
    assertEquals("y string", childEntity.getProperty("yString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    assertEquals("b string", parent.getBString());
    assertEquals(1, parent.getChildren().size());
    assertEquals(child.getId(), parent.getChildren().get(0).getId());
    commitTxn();

    beginTxn();
    child = pm.getObjectById(child.getClass(), child.getId());
    assertEquals("x string", child.getXString());
    assertEquals("y string", child.getYString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(child.getClass()));
  }

  public void testExample2Superclass() throws EntityNotFoundException {
    // insertion
    Example2.A parent = new Example2.A();
    parent.setAString("a string");

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    commitTxn();

    // delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
  }

  public void testExample3Subclass() throws EntityNotFoundException {
    // insertion
    Example3.B parent = new Example3.B();
    parent.setAString("a string");
    parent.setBString("b string");

    Example3.Y child = new Example3.Y();
    child.setXString("x string");
    child.setYString("y string");
    parent.getChildren().add(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity childEntity = ldth.ds.get(child.getId());

    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));
    assertEquals("b string", parentEntity.getProperty("bString"));
    assertEquals(Utils.newArrayList(childEntity.getKey()), parentEntity.getProperty("children"));
    assertEquals(2, childEntity.getProperties().size());
    assertEquals("x string", childEntity.getProperty("xString"));
    assertEquals("y string", childEntity.getProperty("yString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    assertEquals("b string", parent.getBString());
    assertEquals(1, parent.getChildren().size());
    assertEquals(child.getId(), parent.getChildren().get(0).getId());
    commitTxn();

    beginTxn();
    child = pm.getObjectById(child.getClass(), child.getId());
    assertEquals("x string", child.getXString());
    assertEquals("y string", child.getYString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(child.getClass()));
  }

  public void testExample3Superclass() throws EntityNotFoundException {
    // insertion
    Example3.A parent = new Example3.A();
    parent.setAString("a string");

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    commitTxn();

    // delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
  }

  public void testExample4Subclass() throws EntityNotFoundException {
    // insertion
    Example4.B parent = new Example4.B();
    parent.setAString("a string");
    parent.setBString("b string");

    Example4.Y child = new Example4.Y();
    child.setXString("x string");
    child.setYString("y string");
    parent.getChildren().add(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity childEntity = ldth.ds.get(child.getId());

    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));
    assertEquals("b string", parentEntity.getProperty("bString"));
    assertEquals(Utils.newArrayList(childEntity.getKey()), parentEntity.getProperty("children"));
    assertEquals(2, childEntity.getProperties().size());
    assertEquals("x string", childEntity.getProperty("xString"));
    assertEquals("y string", childEntity.getProperty("yString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    assertEquals("b string", parent.getBString());
    assertEquals(1, parent.getChildren().size());
    assertEquals(child.getId(), parent.getChildren().get(0).getId());
    commitTxn();

    beginTxn();
    child = pm.getObjectById(child.getClass(), child.getId());
    assertEquals("x string", child.getXString());
    assertEquals("y string", child.getYString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(child.getClass()));
  }

  public void testExample4Superclass() throws EntityNotFoundException {
    // insertion
    Example4.A parent = new Example4.A();
    parent.setAString("a string");

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals(2, parentEntity.getProperties().size());
    assertEquals("a string", parentEntity.getProperty("aString"));
    assertTrue(parentEntity.hasProperty("children"));
    assertNull(parentEntity.getProperty("children"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("a string", parent.getAString());
    commitTxn();

    // delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
  }

  public void testWrongChildType() {
    Example1.A parent = new Example1.A();
    parent.setAString("a string");
    Example1.Y child = new Example1.Y();
    child.setXString("x string");
    parent.getChildren().add(child);

    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected exception");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
    rollbackTxn();
  }

  public void testWrongChildType_Update() {
    Example1.A parent = new Example1.A();
    parent.setAString("a string");
    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    beginTxn();
    Example1.Y child = new Example1.Y();
    child.setXString("x string");
    parent.getChildren().add(child);

    try {
      commitTxn();
      fail("expected exception");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
  }
}