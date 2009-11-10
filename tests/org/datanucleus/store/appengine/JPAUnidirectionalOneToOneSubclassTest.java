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

import org.datanucleus.test.UnidirectionalOneToOneSubclassesJPA.SubChild;
import org.datanucleus.test.UnidirectionalOneToOneSubclassesJPA.SubParentWithSubChild;
import org.datanucleus.test.UnidirectionalOneToOneSubclassesJPA.SubParentWithSuperChild;
import org.datanucleus.test.UnidirectionalOneToOneSubclassesJPA.SuperChild;
import org.datanucleus.test.UnidirectionalOneToOneSubclassesJPA.SuperParentWithSubChild;
import org.datanucleus.test.UnidirectionalOneToOneSubclassesJPA.SuperParentWithSuperChild;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JPAUnidirectionalOneToOneSubclassTest extends JPATestCase {

  public void testSubParentWithSubChild() throws EntityNotFoundException {
    // insertion
    SubParentWithSubChild parent = new SubParentWithSubChild();
    parent.setSuperParentString("super parent string");
    parent.setSubParentString("sub parent string");
    SubChild subChild = new SubChild();
    subChild.setAString("a string");
    subChild.setBString("b string");
    parent.setSuperParentSubChild(subChild);

    beginTxn();
    em.persist(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals(2, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));
    assertEquals("sub parent string", parentEntity.getProperty("subParentString"));

    Entity superParentSubChildEntity = ldth.ds.get(subChild.getId());
    assertEquals(2, superParentSubChildEntity.getProperties().size());
    assertEquals("a string", superParentSubChildEntity.getProperty("aString"));
    assertEquals("b string", superParentSubChildEntity.getProperty("bString"));

    // lookup
    beginTxn();
    parent = em.find(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals("sub parent string", parent.getSubParentString());
    assertEquals(subChild.getId(), parent.getSuperParentSubChild().getId());
    commitTxn();

    beginTxn();
    subChild = em.find(subChild.getClass(), subChild.getId());
    assertEquals("a string", subChild.getAString());
    assertEquals("b string", subChild.getBString());
    commitTxn();

    // cascade delete
    beginTxn();
    em.remove(em.merge(parent));
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(subChild.getClass()));
  }

  public void testSubParentWithSuperChild() throws EntityNotFoundException {
    // insertion
    SubParentWithSuperChild parent = new SubParentWithSuperChild();
    parent.setSuperParentString("super parent string");
    parent.setSubParentString("sub parent string");

    SuperChild superChild = new SuperChild();
    superChild.setAString("a string");
    parent.setSuperParentSuperChild(superChild);

    beginTxn();
    em.persist(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals(2, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));
    assertEquals("sub parent string", parentEntity.getProperty("subParentString"));

    Entity superParentSuperChildEntity = ldth.ds.get(superChild.getId());
    assertEquals(1, superParentSuperChildEntity.getProperties().size());
    assertEquals("a string", superParentSuperChildEntity.getProperty("aString"));

    // lookup
    beginTxn();
    parent = em.find(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals("sub parent string", parent.getSubParentString());
    assertEquals(superChild.getId(), parent.getSuperParentSuperChild().getId());
    commitTxn();

    beginTxn();
    superChild = em.find(superChild.getClass(), superChild.getId());
    assertEquals("a string", superChild.getAString());
    commitTxn();

    // cascade delete
    beginTxn();
    em.remove(em.merge(parent));
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(superChild.getClass()));
  }

  public void testSuperParentWithSuperChild() throws EntityNotFoundException {
    // insertion
    SuperParentWithSuperChild parent = new SuperParentWithSuperChild();
    parent.setSuperParentString("super parent string");

    SuperChild superChild = new SuperChild();
    superChild.setAString("a string");
    parent.setSuperParentSuperChild(superChild);

    beginTxn();
    em.persist(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));
    Entity superParentSuperChildEntity = ldth.ds.get(superChild.getId());
    assertEquals(1, superParentSuperChildEntity.getProperties().size());
    assertEquals("a string", superParentSuperChildEntity.getProperty("aString"));

    // lookup
    beginTxn();
    parent = em.find(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals(superChild.getId(), parent.getSuperParentSuperChild().getId());
    commitTxn();

    beginTxn();
    superChild = em.find(superChild.getClass(), superChild.getId());
    assertEquals("a string", superChild.getAString());
    commitTxn();

    // cascade delete
    beginTxn();
    em.remove(em.merge(parent));
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(superChild.getClass()));
  }

  public void testSuperParentWithSubChild() throws EntityNotFoundException {
    // insertion
    SuperParentWithSubChild parent = new SuperParentWithSubChild();
    parent.setSuperParentString("super parent string");

    SubChild subChild = new SubChild();
    subChild.setAString("a string");
    subChild.setBString("b string");
    parent.setSuperParentSubChild(subChild);

    beginTxn();
    em.persist(parent);
    commitTxn();
    Entity parentEntity =
        ldth.ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    assertEquals(1, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));

    Entity superParentSubChildEntity = ldth.ds.get(subChild.getId());
    assertEquals(2, superParentSubChildEntity.getProperties().size());
    assertEquals("a string", superParentSubChildEntity.getProperty("aString"));
    assertEquals("b string", superParentSubChildEntity.getProperty("bString"));

    // lookup
    beginTxn();
    parent = em.find(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals(subChild.getId(), parent.getSuperParentSubChild().getId());
    commitTxn();

    beginTxn();
    subChild = em.find(subChild.getClass(), subChild.getId());
    assertEquals("a string", subChild.getAString());
    assertEquals("b string", subChild.getBString());
    commitTxn();

    // cascade delete
    beginTxn();
    em.remove(em.merge(parent));
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(subChild.getClass()));
  }

  public void testWrongChildType() throws IllegalAccessException, InstantiationException {
    SuperParentWithSuperChild parent = new SuperParentWithSuperChild();
    parent.setSuperParentString("a string");
    // working around more runtime enhancer madness
    Object child = SubChild.class.newInstance();
    parent.setSuperParentSuperChild((SuperChild) child);

    beginTxn();
    em.persist(parent);
    try {
      commitTxn();
      fail("expected exception");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
  }

  public void testWrongChildType_Update() throws IllegalAccessException, InstantiationException {
    SuperParentWithSuperChild parent = new SuperParentWithSuperChild();
    parent.setSuperParentString("a string");
    beginTxn();
    em.persist(parent);
    commitTxn();
    beginTxn();
    parent = em.find(parent.getClass(), parent.getId());
    // working around more runtime enhancer madness
    Object child = SubChild.class.newInstance();
    parent.setSuperParentSuperChild((SuperChild) child);

    try {
      commitTxn();
      fail("expected exception");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
  }
}