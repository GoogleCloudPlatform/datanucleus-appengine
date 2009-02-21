// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.test.IllegalMappingsJPA;
import org.datanucleus.test.IllegalMappingsJPA.HasLongPkWithStringAncestor;
import org.datanucleus.test.IllegalMappingsJPA.HasUnencodedStringPkWithStringAncestor;

import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAMetaDataValidatorTest extends JPATestCase {
  public void testStringAncestorPlusNameOnlyPK() {
    HasUnencodedStringPkWithStringAncestor pojo = new HasUnencodedStringPkWithStringAncestor();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testStringAncestorPlusLongPK() {
    HasLongPkWithStringAncestor pojo = new HasLongPkWithStringAncestor();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testMultiplePKNameFields() {
    IllegalMappingsJPA.HasMultiplePkNameFields pojo = new IllegalMappingsJPA.HasMultiplePkNameFields();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testMultiplePKIdFields() {
    IllegalMappingsJPA.HasMultiplePkIdFields pojo = new IllegalMappingsJPA.HasMultiplePkIdFields();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testMultipleAncestors() {
    IllegalMappingsJPA.MultipleAncestors pojo = new IllegalMappingsJPA.MultipleAncestors();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testEncodedPkOnNonPrimaryKeyField() {
    IllegalMappingsJPA.EncodedPkOnNonPrimaryKeyField pojo = new IllegalMappingsJPA.EncodedPkOnNonPrimaryKeyField();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testEncodedPkOnNonStringPrimaryKeyField() {
    IllegalMappingsJPA.EncodedPkOnNonStringPrimaryKeyField pojo = new IllegalMappingsJPA.EncodedPkOnNonStringPrimaryKeyField();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkNameOnNonStringField() {
    IllegalMappingsJPA.PkNameOnNonStringField pojo = new IllegalMappingsJPA.PkNameOnNonStringField();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkIdOnNonLongField() {
    IllegalMappingsJPA.PkIdOnNonLongField pojo = new IllegalMappingsJPA.PkIdOnNonLongField();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  // Fails due to a datanuc bug.
  public void testPkMarkedAsAncestor() {
    IllegalMappingsJPA.PkMarkedAsAncestor pojo = new IllegalMappingsJPA.PkMarkedAsAncestor();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkMarkedAsPkId() {
    IllegalMappingsJPA.PkMarkedAsPkId pojo = new IllegalMappingsJPA.PkMarkedAsPkId();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkMarkedAsPkName() {
    IllegalMappingsJPA.PkMarkedAsPkName pojo = new IllegalMappingsJPA.PkMarkedAsPkName();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkIdWithUnencodedStringPrimaryKey() {
    IllegalMappingsJPA.PkIdWithUnencodedStringPrimaryKey pojo = new IllegalMappingsJPA.PkIdWithUnencodedStringPrimaryKey();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkNameWithUnencodedStringPrimaryKey() {
    IllegalMappingsJPA.PkNameWithUnencodedStringPrimaryKey pojo = new IllegalMappingsJPA.PkNameWithUnencodedStringPrimaryKey();
    beginTxn();
    em.persist(pojo);
    try {
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }
}