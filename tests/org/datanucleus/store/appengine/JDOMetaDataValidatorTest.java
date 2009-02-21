// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.test.IllegalMappingsJDO.EncodedPkOnNonPrimaryKeyField;
import org.datanucleus.test.IllegalMappingsJDO.EncodedPkOnNonStringPrimaryKeyField;
import org.datanucleus.test.IllegalMappingsJDO.HasLongPkWithKeyAncestor;
import org.datanucleus.test.IllegalMappingsJDO.HasLongPkWithStringAncestor;
import org.datanucleus.test.IllegalMappingsJDO.HasMultiplePkIdFields;
import org.datanucleus.test.IllegalMappingsJDO.HasMultiplePkNameFields;
import org.datanucleus.test.IllegalMappingsJDO.HasUnencodedStringPkWithKeyAncestor;
import org.datanucleus.test.IllegalMappingsJDO.HasUnencodedStringPkWithStringAncestor;
import org.datanucleus.test.IllegalMappingsJDO.MultipleAncestors;
import org.datanucleus.test.IllegalMappingsJDO.PkIdOnNonLongField;
import org.datanucleus.test.IllegalMappingsJDO.PkIdWithUnencodedStringPrimaryKey;
import org.datanucleus.test.IllegalMappingsJDO.PkMarkedAsAncestor;
import org.datanucleus.test.IllegalMappingsJDO.PkMarkedAsPkId;
import org.datanucleus.test.IllegalMappingsJDO.PkMarkedAsPkName;
import org.datanucleus.test.IllegalMappingsJDO.PkNameOnNonStringField;
import org.datanucleus.test.IllegalMappingsJDO.PkNameWithUnencodedStringPrimaryKey;

import javax.jdo.JDOUserException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOMetaDataValidatorTest extends JDOTestCase {
  public void testKeyAncestorPlusNameOnlyPK() {
    HasUnencodedStringPkWithKeyAncestor pojo = new HasUnencodedStringPkWithKeyAncestor();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testStringAncestorPlusNameOnlyPK() {
    HasUnencodedStringPkWithStringAncestor pojo = new HasUnencodedStringPkWithStringAncestor();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testKeyAncestorPlusLongPK() {
    HasLongPkWithKeyAncestor pojo = new HasLongPkWithKeyAncestor();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testStringAncestorPlusLongPK() {
    HasLongPkWithStringAncestor pojo = new HasLongPkWithStringAncestor();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testMultiplePKNameFields() {
    HasMultiplePkNameFields pojo = new HasMultiplePkNameFields();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testMultiplePKIdFields() {
    HasMultiplePkIdFields pojo = new HasMultiplePkIdFields();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testMultipleAncestors() {
    MultipleAncestors pojo = new MultipleAncestors();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testEncodedPkOnNonPrimaryKeyField() {
    EncodedPkOnNonPrimaryKeyField pojo = new EncodedPkOnNonPrimaryKeyField();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testEncodedPkOnNonStringPrimaryKeyField() {
    EncodedPkOnNonStringPrimaryKeyField pojo = new EncodedPkOnNonStringPrimaryKeyField();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkNameOnNonStringField() {
    PkNameOnNonStringField pojo = new PkNameOnNonStringField();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkIdOnNonLongField() {
    PkIdOnNonLongField pojo = new PkIdOnNonLongField();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkMarkedAsAncestor() {
    PkMarkedAsAncestor pojo = new PkMarkedAsAncestor();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkMarkedAsPkId() {
    PkMarkedAsPkId pojo = new PkMarkedAsPkId();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkMarkedAsPkName() {
    PkMarkedAsPkName pojo = new PkMarkedAsPkName();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkIdWithUnencodedStringPrimaryKey() {
    PkIdWithUnencodedStringPrimaryKey pojo = new PkIdWithUnencodedStringPrimaryKey();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testPkNameWithUnencodedStringPrimaryKey() {
    PkNameWithUnencodedStringPrimaryKey pojo = new PkNameWithUnencodedStringPrimaryKey();
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }
}