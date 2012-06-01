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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.jpa.HasKeyAncestorKeyPkJPA;
import com.google.appengine.datanucleus.test.jpa.HasOneToOneJPA;
import com.google.appengine.datanucleus.test.jpa.HasOneToOneParentJPA;
import com.google.appengine.datanucleus.test.jpa.HasOneToOnesWithDifferentCascadesJPA;
import com.google.appengine.datanucleus.test.jpa.HasStringAncestorKeyPkJPA;
import com.google.appengine.datanucleus.test.jpa.HasStringAncestorStringPkJPA;

import static com.google.appengine.datanucleus.TestUtils.assertKeyParentEquals;
import static com.google.appengine.datanucleus.TestUtils.assertKeyParentNull;


import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAImplicitEntityGroupTest extends JPATestCase {

  public void testOneToOnePersistCascadeAll() throws EntityNotFoundException {
    HasOneToOneJPA parent = new HasOneToOneJPA();
    HasOneToOneParentJPA child = new HasOneToOneParentJPA();
    parent.setHasParent(child);
    child.setParent(parent);

    beginTxn();
    em.persist(parent);
    commitTxn();

    Entity childEntity = ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    ds.get(KeyFactory.stringToKey(parent.getId()));
  }

  public void testOneToOnePersistChildWithAncestorCascadeAll() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();
    HasStringAncestorStringPkJPA child = new HasStringAncestorStringPkJPA();
    parent.setCascadeAllChild(child);

    beginTxn();
    em.persist(parent);
    commitTxn();

    assertEquals(parent.getId(), child.getAncestorId());

    Entity childEntity = ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    ds.get(KeyFactory.stringToKey(parent.getId()));
  }

  public void testOneToOnePersistCascadePersist() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();
    HasStringAncestorKeyPkJPA child = new HasStringAncestorKeyPkJPA();
    parent.setCascadePersistChild(child);

    beginTxn();
    em.persist(parent);
    assertEquals(parent.getId(), child.getAncestorKey());
    commitTxn();

    Entity childEntity = ds.get(child.getKey());
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadeRemove() throws EntityNotFoundException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);

    HasKeyAncestorKeyPkJPA child = new HasKeyAncestorKeyPkJPA();

    beginTxn();
    em.persist(child);
    commitTxn();
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();
    parent.setCascadeRemoveChild(child);
    beginTxn();
    em.persist(parent);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      rollbackTxn();
    }

    Entity childEntity = ds.get(child.getKey());
    assertKeyParentNull(childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadeAll_OverrideParentManually() {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();

    HasOneToOnesWithDifferentCascadesJPA anotherParent = new HasOneToOnesWithDifferentCascadesJPA();
    beginTxn();
    em.persist(anotherParent);
    commitTxn();

    HasStringAncestorStringPkJPA child = new HasStringAncestorStringPkJPA(anotherParent.getId());
    parent.setCascadeAllChild(child);

    beginTxn();
    em.persist(parent);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testOneToOnePersistCascadePersist_OverrideParentManually() {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();

    HasOneToOnesWithDifferentCascadesJPA anotherParent = new HasOneToOnesWithDifferentCascadesJPA();
    beginTxn();
    em.persist(anotherParent);
    commitTxn();

    HasStringAncestorKeyPkJPA child = new HasStringAncestorKeyPkJPA();
    child.setAncestorKey(anotherParent.getId());
    parent.setCascadePersistChild(child);

    beginTxn();
    em.persist(parent);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
    }
  }

  public void testOneToOneRemoveParentCascadeAll() {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity childEntity = new Entity(HasStringAncestorStringPkJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJPA parent = em.find(
        HasOneToOnesWithDifferentCascadesJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeAllChild());
    em.remove(parent);
    commitTxn();

    try {
      ds.get(parentEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }

    try {
      ds.get(childEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }
  }

  public void testOneToOneRemoveChildCascadeAll() throws EntityNotFoundException {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity childEntity = new Entity(HasStringAncestorStringPkJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJPA parent = em.find(
        HasOneToOnesWithDifferentCascadesJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeAllChild());
    assertKeyParentEquals(parent.getId(), childEntity, parent.getCascadeAllChild().getId());
    em.remove(parent.getCascadeAllChild());
    commitTxn();

    parentEntity = ds.get(parentEntity.getKey());
    assertEquals(childEntity.getKey(), parentEntity.getProperty("cascadeall"));
    try {
      ds.get(childEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }
  }

  public void testOneToOneRemoveChildCascadeRemove() throws EntityNotFoundException {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJPA.class.getSimpleName());
    ds.put(parentEntity);
    Entity childEntity = new Entity(HasKeyAncestorKeyPkJPA.class.getSimpleName(), parentEntity.getKey());
    ds.put(childEntity);
    parentEntity.setProperty("cascaderemove", childEntity.getKey());
    ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJPA parent = em.find(
        HasOneToOnesWithDifferentCascadesJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeRemoveChild());
    assertKeyParentEquals(parent.getId(), childEntity, parent.getCascadeRemoveChild().getKey());
    em.remove(parent.getCascadeRemoveChild());
    commitTxn();

    parentEntity = ds.get(parentEntity.getKey());
    assertEquals(childEntity.getKey(), parentEntity.getProperty("cascaderemove"));
    try {
      ds.get(childEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }
  }

  public void testChildGoesIntoEntityGroupOfExistingParent() throws EntityNotFoundException {
    HasOneToOneJPA parent = new HasOneToOneJPA();

    beginTxn();
    em.persist(parent);
    commitTxn();

    HasOneToOneParentJPA child = new HasOneToOneParentJPA();
    parent.setHasParent(child);
    child.setParent(parent);

    beginTxn();
    em.merge(parent);
    commitTxn();

    Entity childEntity = ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    ds.get(KeyFactory.stringToKey(parent.getId()));
  }
}
