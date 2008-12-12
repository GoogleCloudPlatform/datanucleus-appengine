// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.test.HasAncestorJPA;
import org.datanucleus.test.HasOneToOneJPA;
import org.datanucleus.test.HasOneToOneParentJPA;
import org.datanucleus.test.HasOneToOnesWithDifferentCascades;

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

    Entity childEntity = ldth.ds.get(KeyFactory.decodeKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(parent.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, (Key) parentEntity.getProperty("hasparent_id"));
  }

  public void testOneToOnePersistChildWithAncestorCascadeAll() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascades parent = new HasOneToOnesWithDifferentCascades();
    HasAncestorJPA child = new HasAncestorJPA();
    parent.setCascadeAllChild(child);

    beginTxn();
    em.persist(parent);
    commitTxn();

    assertEquals(parent.getId(), child.getAncestorId());

    Entity childEntity = ldth.ds.get(KeyFactory.decodeKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(parent.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, (Key) parentEntity.getProperty("cascadeall"));
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
//    assertEquals(parent.getId(), child.getAncestorKey());
//
//    Entity childEntity = ldth.ds.get(KeyFactory.decodeKey(child.getKey()));
//    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());
//
//    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(parent.getId()));
//    assertKeyParentEquals(parent.getId(), childEntity, (Key) parentEntity.getProperty("cascadeallwithkeyancestor"));
//  }

  public void testOneToOnePersistCascadePersist() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascades parent = new HasOneToOnesWithDifferentCascades();
    HasAncestorJPA child = new HasAncestorJPA();
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

  public void testOneToOnePersistCascadeRemove() throws EntityNotFoundException {
    HasOneToOnesWithDifferentCascades parent = new HasOneToOnesWithDifferentCascades();
    HasAncestorJPA child = new HasAncestorJPA();
    parent.setCascadeRemoveChild(child);

    beginTxn();
    em.persist(child);
    commitTxn();
    beginTxn();
    em.persist(parent);
    commitTxn();

    Entity childEntity = ldth.ds.get(KeyFactory.decodeKey(child.getId()));
    assertKeyParentNull(childEntity, childEntity.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(parent.getId()));
    assertKeyParentNull(childEntity, (Key) parentEntity.getProperty("cascaderemove"));
  }

  public void testOneToOnePersistCascadeAll_OverrideParentManually() {
    HasOneToOnesWithDifferentCascades parent = new HasOneToOnesWithDifferentCascades();

    HasOneToOnesWithDifferentCascades anotherParent = new HasOneToOnesWithDifferentCascades();
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
    HasOneToOnesWithDifferentCascades parent = new HasOneToOnesWithDifferentCascades();

    HasOneToOnesWithDifferentCascades anotherParent = new HasOneToOnesWithDifferentCascades();
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
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascades.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasAncestorJPA.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascades parent = em.find(
        HasOneToOnesWithDifferentCascades.class, KeyFactory.encodeKey(parentEntity.getKey()));
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
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascades.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasAncestorJPA.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascadeall", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascades parent = em.find(
        HasOneToOnesWithDifferentCascades.class, KeyFactory.encodeKey(parentEntity.getKey()));
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
    Entity parentEntity = new Entity(HasOneToOnesWithDifferentCascades.class.getSimpleName());
    ldth.ds.put(parentEntity);
    Entity childEntity = new Entity(HasAncestorJPA.class.getSimpleName(), parentEntity.getKey());
    ldth.ds.put(childEntity);
    parentEntity.setProperty("cascaderemove", childEntity.getKey());
    ldth.ds.put(parentEntity);

    beginTxn();
    HasOneToOnesWithDifferentCascades parent = em.find(
        HasOneToOnesWithDifferentCascades.class, KeyFactory.encodeKey(parentEntity.getKey()));
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

    Entity childEntity = ldth.ds.get(KeyFactory.decodeKey(child.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, childEntity.getKey());

    Entity parentEntity = ldth.ds.get(KeyFactory.decodeKey(parent.getId()));
    assertKeyParentEquals(parent.getId(), childEntity, (Key) parentEntity.getProperty("hasparent_id"));

  }
}
