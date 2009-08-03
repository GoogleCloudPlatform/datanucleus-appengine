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

import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.test.IgnorableMappingsJDO.HasUniqueConstraint;
import org.datanucleus.test.IgnorableMappingsJDO.HasUniqueConstraints;
import org.datanucleus.test.IgnorableMappingsJDO.OneToManyParentWithEagerlyFetchedChild;
import org.datanucleus.test.IgnorableMappingsJDO.OneToManyParentWithEagerlyFetchedChildList;
import org.datanucleus.test.IllegalMappingsJDO.EncodedPkOnNonPrimaryKeyField;
import org.datanucleus.test.IllegalMappingsJDO.EncodedPkOnNonStringPrimaryKeyField;
import org.datanucleus.test.IllegalMappingsJDO.HasLongPkWithKeyAncestor;
import org.datanucleus.test.IllegalMappingsJDO.HasLongPkWithStringAncestor;
import org.datanucleus.test.IllegalMappingsJDO.HasMultiplePkIdFields;
import org.datanucleus.test.IllegalMappingsJDO.HasMultiplePkNameFields;
import org.datanucleus.test.IllegalMappingsJDO.HasUnencodedStringPkWithKeyAncestor;
import org.datanucleus.test.IllegalMappingsJDO.HasUnencodedStringPkWithStringAncestor;
import org.datanucleus.test.IllegalMappingsJDO.LongParent;
import org.datanucleus.test.IllegalMappingsJDO.MultipleAncestors;
import org.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyLongBiChild;
import org.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyLongUniChild;
import org.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyStringBiChild;
import org.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyStringUniChild;
import org.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyLongBiChild;
import org.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyLongUniChild;
import org.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyStringBiChild;
import org.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyStringUniChild;
import org.datanucleus.test.IllegalMappingsJDO.PkIdOnNonLongField;
import org.datanucleus.test.IllegalMappingsJDO.PkIdWithUnencodedStringPrimaryKey;
import org.datanucleus.test.IllegalMappingsJDO.PkMarkedAsAncestor;
import org.datanucleus.test.IllegalMappingsJDO.PkMarkedAsPkId;
import org.datanucleus.test.IllegalMappingsJDO.PkMarkedAsPkName;
import org.datanucleus.test.IllegalMappingsJDO.PkNameOnNonStringField;
import org.datanucleus.test.IllegalMappingsJDO.PkNameWithUnencodedStringPrimaryKey;

import javax.jdo.JDOFatalUserException;
import javax.jdo.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOMetaDataValidatorTest extends JDOTestCase {

  public void testKeyAncestorPlusNameOnlyPK() {
    HasUnencodedStringPkWithKeyAncestor pojo = new HasUnencodedStringPkWithKeyAncestor();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testStringAncestorPlusNameOnlyPK() {
    HasUnencodedStringPkWithStringAncestor pojo = new HasUnencodedStringPkWithStringAncestor();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testKeyAncestorPlusLongPK() {
    HasLongPkWithKeyAncestor pojo = new HasLongPkWithKeyAncestor();
    assertMetaDataException(pojo);
  }

  public void testStringAncestorPlusLongPK() {
    HasLongPkWithStringAncestor pojo = new HasLongPkWithStringAncestor();
    assertMetaDataException(pojo);
  }

  public void testMultiplePKNameFields() {
    HasMultiplePkNameFields pojo = new HasMultiplePkNameFields();
    assertMetaDataException(pojo);
  }

  public void testMultiplePKIdFields() {
    HasMultiplePkIdFields pojo = new HasMultiplePkIdFields();
    assertMetaDataException(pojo);
  }

  public void testMultipleAncestors() {
    MultipleAncestors pojo = new MultipleAncestors();
    assertMetaDataException(pojo);
  }

  public void testEncodedPkOnNonPrimaryKeyField() {
    EncodedPkOnNonPrimaryKeyField pojo = new EncodedPkOnNonPrimaryKeyField();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testEncodedPkOnNonStringPrimaryKeyField() {
    EncodedPkOnNonStringPrimaryKeyField pojo = new EncodedPkOnNonStringPrimaryKeyField();
    assertMetaDataException(pojo);
  }

  public void testPkNameOnNonStringField() {
    PkNameOnNonStringField pojo = new PkNameOnNonStringField();
    assertMetaDataException(pojo);
  }

  public void testPkIdOnNonLongField() {
    PkIdOnNonLongField pojo = new PkIdOnNonLongField();
    assertMetaDataException(pojo);
  }

  public void testPkMarkedAsAncestor() {
    PkMarkedAsAncestor pojo = new PkMarkedAsAncestor();
    assertMetaDataException(pojo);
  }

  public void testPkMarkedAsPkId() {
    PkMarkedAsPkId pojo = new PkMarkedAsPkId();
    assertMetaDataException(pojo);
  }

  public void testPkMarkedAsPkName() {
    PkMarkedAsPkName pojo = new PkMarkedAsPkName();
    assertMetaDataException(pojo);
  }

  public void testPkIdWithUnencodedStringPrimaryKey() {
    PkIdWithUnencodedStringPrimaryKey pojo = new PkIdWithUnencodedStringPrimaryKey();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testPkNameWithUnencodedStringPrimaryKey() {
    PkNameWithUnencodedStringPrimaryKey pojo = new PkNameWithUnencodedStringPrimaryKey();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testLongPkWithUnidirectionalOneToManyChild() {
    OneToManyParentWithRootOnlyLongUniChild pojo = new OneToManyParentWithRootOnlyLongUniChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testLongPkWithBidirectionalOneToManyChild() {
    OneToManyParentWithRootOnlyLongBiChild pojo = new OneToManyParentWithRootOnlyLongBiChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testStringPkWithUnidirectionalOneToManyChild() {
    OneToManyParentWithRootOnlyStringUniChild pojo = new OneToManyParentWithRootOnlyStringUniChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testStringPkWithBidirectionalOneToManyChild() {
    OneToManyParentWithRootOnlyStringBiChild pojo = new OneToManyParentWithRootOnlyStringBiChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testLongPkWithUnidirectionalOneToOneChild() {
    OneToOneParentWithRootOnlyLongUniChild pojo = new OneToOneParentWithRootOnlyLongUniChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testLongPkWithUnidirectionalOneToOneChild_Fetch() {
    beginTxn();
    try {
      pm.getObjectById(OneToOneParentWithRootOnlyLongUniChild.class, "yar");
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testLongPkWithUnidirectionalOneToOneChild_Query() {
    beginTxn();
    try {
      Query q = pm.newQuery(OneToOneParentWithRootOnlyLongUniChild.class);
      q.execute();
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }

  public void testLongPkWithBidirectionalOneToOneChild() {
    OneToOneParentWithRootOnlyLongBiChild pojo = new OneToOneParentWithRootOnlyLongBiChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testStringPkWithUnidirectionalOneToOneChild() {
    OneToOneParentWithRootOnlyStringUniChild pojo = new OneToOneParentWithRootOnlyStringUniChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testStringPkWithBidirectionalOneToOneChild() {
    OneToOneParentWithRootOnlyStringBiChild pojo = new OneToOneParentWithRootOnlyStringBiChild();
    pojo.id = "yar";
    assertMetaDataException(pojo);
  }

  public void testAncestorOfIllegalType() {
    assertMetaDataException(new LongParent());
  }

  public void testOneToManyWithEagerlyFetchedChildList() {
    assertMetaDataException(new OneToManyParentWithEagerlyFetchedChildList());
  }

  public void testOneToManyWithEagerlyFetchedChild() {
    assertMetaDataException(new OneToManyParentWithEagerlyFetchedChild());
  }

  public void testUniqueConstraint() {
    assertMetaDataException(new HasUniqueConstraint());
  }

  public void testUniqueConstraints() {
    assertMetaDataException(new HasUniqueConstraints());
  }

  public void testIsJPA() {
    MetaDataManager mdm = ((JDOPersistenceManagerFactory)pmf).getOMFContext().getMetaDataManager();
    MetaDataValidator mdv = new MetaDataValidator(null, mdm, null);
    assertFalse(mdv.isJPA());
  }

  private void assertMetaDataException(Object pojo) {
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause() instanceof MetaDataValidator.DatastoreMetaDataException);
      rollbackTxn();
    }
  }
}
