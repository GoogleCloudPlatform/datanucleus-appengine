/*
 * Copyright (C) 2010 Google Inc
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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.UnidirectionalOneToManySubclassesJDO.SubChild;
import com.google.appengine.datanucleus.test.UnidirectionalOneToManySubclassesJDO.SubParentWithSubChild;
import com.google.appengine.datanucleus.test.UnidirectionalOneToManySubclassesJDO.SubParentWithSuperChild;
import com.google.appengine.datanucleus.test.UnidirectionalOneToManySubclassesJDO.SuperChild;
import com.google.appengine.datanucleus.test.UnidirectionalOneToManySubclassesJDO.SuperParentWithSubChild;
import com.google.appengine.datanucleus.test.UnidirectionalOneToManySubclassesJDO.SuperParentWithSuperChild;


import java.util.Collections;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class JDOUnidirectionalOneToManySubclassTest extends JDOTestCase {

  public void testSubParentWithSubChild() throws EntityNotFoundException {
    // insertion
    SubParentWithSubChild parent = new SubParentWithSubChild();
    parent.setSuperParentString("super parent string");
    parent.setSubParentString("sub parent string");
    SubChild subChild = new SubChild();
    subChild.setAString("a string");
    subChild.setBString("b string");
    parent.getSuperParentSubChildren().add(subChild);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity superParentSubChildEntity = ds.get(subChild.getId());
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));
    assertEquals("sub parent string", parentEntity.getProperty("subParentString"));
    assertEquals(Collections.singletonList(superParentSubChildEntity.getKey()),
                 parentEntity.getProperty("subChildren"));

    assertEquals(2, superParentSubChildEntity.getProperties().size());
    assertEquals("a string", superParentSubChildEntity.getProperty("aString"));
    assertEquals("b string", superParentSubChildEntity.getProperty("bString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals("sub parent string", parent.getSubParentString());
    assertEquals(1, parent.getSuperParentSubChildren().size());
    assertEquals(subChild.getId(), parent.getSuperParentSubChildren().get(0).getId());
    commitTxn();

    beginTxn();
    subChild = pm.getObjectById(subChild.getClass(), subChild.getId());
    assertEquals("a string", subChild.getAString());
    assertEquals("b string", subChild.getBString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
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
    parent.getSuperParentSuperChildren().add(superChild);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity superParentSuperChildEntity = ds.get(superChild.getId());
    assertEquals(3, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));
    assertEquals("sub parent string", parentEntity.getProperty("subParentString"));
    assertEquals(Collections.singletonList(superParentSuperChildEntity.getKey()),
                 parentEntity.getProperty("superChildren"));

    assertEquals(1, superParentSuperChildEntity.getProperties().size());
    assertEquals("a string", superParentSuperChildEntity.getProperty("aString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals("sub parent string", parent.getSubParentString());
    assertEquals(1, parent.getSuperParentSuperChildren().size());
    assertEquals(superChild.getId(), parent.getSuperParentSuperChildren().get(0).getId());
    commitTxn();

    beginTxn();
    superChild = pm.getObjectById(superChild.getClass(), superChild.getId());
    assertEquals("a string", superChild.getAString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
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
    parent.getSuperParentSuperChildren().add(superChild);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity superParentSuperChildEntity = ds.get(superChild.getId());
    assertEquals(2, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));
    assertEquals(Collections.singletonList(superParentSuperChildEntity.getKey()),
                 parentEntity.getProperty("superChildren"));
    assertEquals(1, superParentSuperChildEntity.getProperties().size());
    assertEquals("a string", superParentSuperChildEntity.getProperty("aString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals(1, parent.getSuperParentSuperChildren().size());
    assertEquals(superChild.getId(), parent.getSuperParentSuperChildren().get(0).getId());
    commitTxn();

    beginTxn();
    superChild = pm.getObjectById(superChild.getClass(), superChild.getId());
    assertEquals("a string", superChild.getAString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
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
    parent.getSuperParentSubChildren().add(subChild);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    Entity parentEntity =
        ds.get(KeyFactory.createKey(kindForClass(parent.getClass()), parent.getId()));
    Entity superParentSubChildEntity = ds.get(subChild.getId());
    assertEquals(2, parentEntity.getProperties().size());
    assertEquals("super parent string", parentEntity.getProperty("superParentString"));
    assertEquals(Collections.singletonList(superParentSubChildEntity.getKey()),
                 parentEntity.getProperty("subChildren"));

    assertEquals(2, superParentSubChildEntity.getProperties().size());
    assertEquals("a string", superParentSubChildEntity.getProperty("aString"));
    assertEquals("b string", superParentSubChildEntity.getProperty("bString"));

    // lookup
    beginTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());
    assertEquals("super parent string", parent.getSuperParentString());
    assertEquals(1, parent.getSuperParentSubChildren().size());
    assertEquals(subChild.getId(), parent.getSuperParentSubChildren().get(0).getId());
    commitTxn();

    beginTxn();
    subChild = pm.getObjectById(subChild.getClass(), subChild.getId());
    assertEquals("a string", subChild.getAString());
    assertEquals("b string", subChild.getBString());
    commitTxn();

    // cascade delete
    beginTxn();
    pm.deletePersistent(parent);
    commitTxn();

    assertEquals(0, countForClass(parent.getClass()));
    assertEquals(0, countForClass(subChild.getClass()));
  }

  public void testWrongChildType() {
    SuperParentWithSuperChild parent = new SuperParentWithSuperChild();
    parent.setSuperParentString("a string");
    SubChild child = new SubChild();
    parent.getSuperParentSuperChildren().add(child);

    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected exception");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
    rollbackTxn();
  }

  public void testWrongChildType_Update() throws InstantiationException, IllegalAccessException {
    // need a non-txn datasource so we can access multiple entity groups
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    SubChild child = SubChild.class.newInstance();
    SuperParentWithSuperChild parent = new SuperParentWithSuperChild();
    parent.setSuperParentString("a string");
    beginTxn();
    pm.makePersistent(parent);
    commitTxn();
    parent = pm.getObjectById(parent.getClass(), parent.getId());

    parent.getSuperParentSuperChildren().add(child);
    try {
      pm.close();
      fail("expected exception");
    } catch (UnsupportedOperationException uoe) {
      // good
    }
  }
}
