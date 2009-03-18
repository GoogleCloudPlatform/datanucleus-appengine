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

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import org.datanucleus.test.HasKeyAncestorStringPkJDO;
import org.datanucleus.test.HasOneToOneJDO;
import org.datanucleus.test.HasOneToOneParentJDO;
import org.datanucleus.test.HasOneToOnesWithDifferentCascadesJDO;
import org.datanucleus.test.HasStringAncestorStringPkJDO;

import javax.jdo.JDOFatalUserException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOImplicitEntityGroupTest extends JDOTestCase {

  public void testOneToOnePersistCascadeAll() throws EntityNotFoundException {
    HasOneToOneJDO parent = new HasOneToOneJDO();
    HasOneToOneParentJDO child = new HasOneToOneParentJDO();
    parent.setHasParent(child);
    child.setParent(parent);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getKey()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    beginTxn();
    parent = pm.getObjectById(HasOneToOneJDO.class, parent.getId());
    assertNotNull(parent.getHasParent());
    assertNotNull(parent.getHasParent().getParent());
    commitTxn();
  }

  public void testOneToOnePersistChildWithAncestorCascadeAll() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();
    HasStringAncestorStringPkJDO child = new HasStringAncestorStringPkJDO();
    parent.setCascadeAllChild(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    beginTxn();
    parent = pm.getObjectById(HasOneToOnesWithDifferentCascadesJDO.class, parent.getId());
    assertNotNull(parent.getCascadeAllChild());
    child = pm.getObjectById(HasStringAncestorStringPkJDO.class, child.getId());
    assertEquals(parent.getId(), child.getAncestorId());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistChildWithKeyAncestorCascadeAll() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();
    HasKeyAncestorStringPkJDO child = new HasKeyAncestorStringPkJDO();
    parent.setCascadeAllChildWithKeyAncestor(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    beginTxn();
    parent = pm.getObjectById(HasOneToOnesWithDifferentCascadesJDO.class, parent.getId());
    child = pm.getObjectById(HasKeyAncestorStringPkJDO.class, child.getKey());
    assertEquals(parent.getId(), KeyFactory.keyToString(child.getAncestorKey()));
    assertNotNull(parent.getCascadeAllChildWithKeyAncestor());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getKey()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadePersist() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();
    HasStringAncestorStringPkJDO child = new HasStringAncestorStringPkJDO();
    parent.setCascadePersistChild(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    beginTxn();
    parent = pm.getObjectById(HasOneToOnesWithDifferentCascadesJDO.class, parent.getId());
    child = pm.getObjectById(HasStringAncestorStringPkJDO.class, child.getId());
    assertEquals(parent.getId(), child.getAncestorId());
    assertNotNull(parent.getCascadePersistChild());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadeRemove() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();
    HasStringAncestorStringPkJDO child = new HasStringAncestorStringPkJDO();
    parent.setCascadeRemoveChild(child);

    beginTxn();
    pm.makePersistent(child);
    commitTxn();
    beginTxn();
    try {
      // this fails because it attempts to establish a parent
      // for an entity that was originally persisted without a parent.
      pm.makePersistent(parent);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      rollbackTxn();
    }
  }

  public void testOneToOnePersistCascadeAll_OverrideParentManually() {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();

    HasOneToOnesWithDifferentCascadesJDO anotherParent = new HasOneToOnesWithDifferentCascadesJDO();
    beginTxn();
    pm.makePersistent(anotherParent);
    commitTxn();

    HasStringAncestorStringPkJDO child = new HasStringAncestorStringPkJDO(anotherParent.getId());
    parent.setCascadeAllChild(child);

    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    } finally {
      rollbackTxn();
    }
  }

  public void testOneToOnePersistCascadePersist_OverrideParentManually() {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();

    HasOneToOnesWithDifferentCascadesJDO anotherParent = new HasOneToOnesWithDifferentCascadesJDO();
    beginTxn();
    pm.makePersistent(anotherParent);
    commitTxn();

    HasStringAncestorStringPkJDO child = new HasStringAncestorStringPkJDO(anotherParent.getId());
    parent.setCascadePersistChild(child);

    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
    } finally {
      rollbackTxn();
    }
  }

  public void testOneToOneRemoveParentCascadeAll() {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasStringAncestorStringPkJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJDO parent = pm.getObjectById(
        HasOneToOnesWithDifferentCascadesJDO.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeAllChild());
    pm.deletePersistent(parent);
    commitTxn();

    try {
      ldth.ds.get(parentEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }

    try {
      ldth.ds.get(childEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }
  }

  public void testOneToOneRemoveChildCascadeAll() throws EntityNotFoundException {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasStringAncestorStringPkJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJDO parent = pm.getObjectById(
        HasOneToOnesWithDifferentCascadesJDO.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeAllChild());
    assertKeyParentEquals(parent.getId(), childEntity, parent.getCascadeAllChild().getId());
    pm.deletePersistent(parent.getCascadeAllChild());
    commitTxn();

    parentEntity = ldth.ds.get(parentEntity.getKey());
    assertEquals(childEntity.getKey(), parentEntity.getProperty("cascadeall"));
    try {
      ldth.ds.get(childEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }
  }

  public void testOneToOneRemoveChildCascadeRemove() throws EntityNotFoundException {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasStringAncestorStringPkJDO.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascaderemove", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJDO parent = pm.getObjectById(
        HasOneToOnesWithDifferentCascadesJDO.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeRemoveChild());
    assertKeyParentEquals(parent.getId(), childEntity, parent.getCascadeRemoveChild().getId());
    pm.deletePersistent(parent.getCascadeRemoveChild());
    commitTxn();

    parentEntity = ldth.ds.get(parentEntity.getKey());
    assertEquals(childEntity.getKey(), parentEntity.getProperty("cascaderemove"));
    try {
      ldth.ds.get(childEntity.getKey());
      fail("expected enfe");
    } catch (EntityNotFoundException e) {
      // good
    }
  }

  public void testChildGoesIntoEntityGroupOfExistingParent() throws EntityNotFoundException {
    HasOneToOneJDO parent = new HasOneToOneJDO();

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    beginTxn();
    HasOneToOneParentJDO child = new HasOneToOneParentJDO();
    parent.setHasParent(child);
    child.setParent(parent);

    pm.makePersistent(parent);
    commitTxn();
    beginTxn();
    parent = pm.getObjectById(HasOneToOneJDO.class, parent.getId());
    assertNotNull(parent.getHasParent());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getKey()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }
}
