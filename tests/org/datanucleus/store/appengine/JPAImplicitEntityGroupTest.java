// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import static org.datanucleus.store.appengine.TestUtils.assertKeyParentEquals;
import static org.datanucleus.store.appengine.TestUtils.assertKeyParentNull;
import org.datanucleus.test.HasAncestorJPA;
import org.datanucleus.test.HasOneToOneJPA;
import org.datanucleus.test.HasOneToOneParentJPA;
import org.datanucleus.test.HasOneToOnesWithDifferentCascadesJPA;

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

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(parent.getId()));
  }

  public void testOneToOnePersistChildWithAncestorCascadeAll() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();
    HasAncestorJPA child = new HasAncestorJPA();
    parent.setCascadeAllChild(child);

    beginTxn();
    em.persist(parent);
    commitTxn();

    assertEquals(parent.getId(), child.getAncestorId());

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(parent.getId()));
  }

// Test commented out because JPA doesn't support non-pk fields of arbitrary types.
// Should be a feasible test for JDO though
//  public void testOneToOnePersistChildWithKeyAncestorCascadeAll() throws EntityNotFoundException {
//    HasOneToOnesWithDifferentCascades parent = new HasOneToOnesWithDifferentCascades();
//    HasKeyAncestorKeyStringPkJPA child = new HasKeyAncestorKeyStringPkJPA();
//    parent.setCascadeAllChildWithKeyAncestor(child);
//
//    beginTxn();
//    em.persist(parent);
//    commitTxn();
//
//    assertEquals(parent.getKey(), child.getAncestorKey());
//
//    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getKey()));
//    assertKeyParentEquals(parent.getKey(), childEntity, childEntity.getKey());
//
//    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(parent.getKey()));
//    assertKeyParentEquals(parent.getKey(), childEntity, (Key) parentEntity.getProperty("cascadeallwithkeyancestor"));
//  }

  public void testOneToOnePersistCascadePersist() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();
    HasAncestorJPA child = new HasAncestorJPA();
    parent.setCascadePersistChild(child);

    beginTxn();
    em.persist(parent);
    assertEquals(parent.getId(), child.getAncestorId());
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadeRemove() throws EntityNotFoundException {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);

    HasAncestorJPA child = new HasAncestorJPA();

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

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentNull(childEntity, childEntity.getKey());
  }

  public void testOneToOnePersistCascadeAll_OverrideParentManually() {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();

    HasOneToOnesWithDifferentCascadesJPA anotherParent = new HasOneToOnesWithDifferentCascadesJPA();
    beginTxn();
    em.persist(anotherParent);
    commitTxn();

    HasAncestorJPA child = new HasAncestorJPA(anotherParent.getId());
    parent.setCascadeAllChild(child);

    beginTxn();
    em.persist(parent);
    try {
      commitTxn();
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  public void testOneToOnePersistCascadePersist_OverrideParentManually() {
    HasOneToOnesWithDifferentCascadesJPA parent = new HasOneToOnesWithDifferentCascadesJPA();

    HasOneToOnesWithDifferentCascadesJPA anotherParent = new HasOneToOnesWithDifferentCascadesJPA();
    beginTxn();
    em.persist(anotherParent);
    commitTxn();

    HasAncestorJPA child = new HasAncestorJPA(anotherParent.getId());
    parent.setCascadePersistChild(child);

    beginTxn();
    em.persist(parent);
    try {
      commitTxn();
      fail("expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
  }

  public void testOneToOneRemoveParentCascadeAll() {
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJPA.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasAncestorJPA.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJPA parent = em.find(
        HasOneToOnesWithDifferentCascadesJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeAllChild());
    em.remove(parent);
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
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJPA.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasAncestorJPA.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJPA parent = em.find(
        HasOneToOnesWithDifferentCascadesJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeAllChild());
    assertKeyParentEquals(parent.getId(), childEntity, parent.getCascadeAllChild().getId());
    em.remove(parent.getCascadeAllChild());
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
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascadesJPA.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasAncestorJPA.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascaderemove", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascadesJPA parent = em.find(
        HasOneToOnesWithDifferentCascadesJPA.class, KeyFactory.keyToString(parentEntity.getKey()));
    assertNotNull(parent.getCascadeRemoveChild());
    assertKeyParentEquals(parent.getId(), childEntity, parent.getCascadeRemoveChild().getId());
    em.remove(parent.getCascadeRemoveChild());
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

    Entity childEntity = ldth.ds.get(KeyFactory.stringToKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.stringToKey(parent.getId()));

  }
}
