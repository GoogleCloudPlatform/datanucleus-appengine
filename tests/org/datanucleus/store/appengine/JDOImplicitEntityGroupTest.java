// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import static org.datanucleus.store.appengine.TestUtils.assertKeyParentNull;
import org.datanucleus.test.HasAncestorJDO;
import org.datanucleus.test.HasKeyAncestorKeyStringPkJDO;
import org.datanucleus.test.HasOneToOneJDO;
import org.datanucleus.test.HasOneToOneParentJDO;
import org.datanucleus.test.HasOneToOnesWithDifferentCascadesJDO;

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
    HasAncestorJDO child = new HasAncestorJDO();
    parent.setCascadeAllChild(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    beginTxn();
    parent = pm.getObjectById(HasOneToOnesWithDifferentCascadesJDO.class, parent.getId());
    assertNotNull(parent.getCascadeAllChild());
    child = pm.getObjectById(HasAncestorJDO.class, child.getId());
    assertEquals(parent.getId(), child.getAncestorId());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistChildWithKeyAncestorCascadeAll() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();
    HasKeyAncestorKeyStringPkJDO child = new HasKeyAncestorKeyStringPkJDO();
    parent.setCascadeAllChildWithKeyAncestor(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    beginTxn();
    parent = pm.getObjectById(HasOneToOnesWithDifferentCascadesJDO.class, parent.getId());
    child = pm.getObjectById(HasKeyAncestorKeyStringPkJDO.class, child.getKey());
    assertEquals(parent.getId(), KeyFactory.keyToString(child.getAncestorKey()));
    assertNotNull(parent.getCascadeAllChildWithKeyAncestor());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getKey()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadePersist() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();
    HasAncestorJDO child = new HasAncestorJDO();
    parent.setCascadePersistChild(child);

    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    beginTxn();
    parent = pm.getObjectById(HasOneToOnesWithDifferentCascadesJDO.class, parent.getId());
    child = pm.getObjectById(HasAncestorJDO.class, child.getId());
    assertEquals(parent.getId(), child.getAncestorId());
    assertNotNull(parent.getCascadePersistChild());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadeRemove() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();
    HasAncestorJDO child = new HasAncestorJDO();
    parent.setCascadeRemoveChild(child);

    beginTxn();
    pm.makePersistent(child);
    commitTxn();
    beginTxn();
    pm.makePersistent(parent);
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentNull(childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadeAll_OverrideParentManually() {
    HasOneToOnesWithDifferentCascadesJDO parent = new HasOneToOnesWithDifferentCascadesJDO();

    HasOneToOnesWithDifferentCascadesJDO anotherParent = new HasOneToOnesWithDifferentCascadesJDO();
    beginTxn();
    pm.makePersistent(anotherParent);
    commitTxn();

    HasAncestorJDO child = new HasAncestorJDO(anotherParent.getId());
    parent.setCascadeAllChild(child);

    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
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

    HasAncestorJDO child = new HasAncestorJDO(anotherParent.getId());
    parent.setCascadePersistChild(child);

    beginTxn();
    try {
      pm.makePersistent(parent);
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    } finally {
      rollbackTxn();
    }
  }

  public void testOneToOneRemoveParentCascadeAll() {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJDO.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasAncestorJDO.class.getSimpleName(), parentEntity.getKey());
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
    Entity childEntity = new Entity(HasAncestorJDO.class.getSimpleName(), parentEntity.getKey());
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
    Entity childEntity = new Entity(HasAncestorJDO.class.getSimpleName(), parentEntity.getKey());
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
