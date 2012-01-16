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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.datanucleus.test.IgnorableMappingsJDO.HasUniqueConstraint;
import com.google.appengine.datanucleus.test.IgnorableMappingsJDO.HasUniqueConstraints;
import com.google.appengine.datanucleus.test.IgnorableMappingsJDO.OneToManyParentWithEagerlyFetchedChild;
import com.google.appengine.datanucleus.test.IgnorableMappingsJDO.OneToManyParentWithEagerlyFetchedChildList;
import com.google.appengine.datanucleus.test.IgnorableMappingsJDO.OneToManyParentWithEagerlyFetchedEmbeddedChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.EncodedPkOnNonPrimaryKeyField;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.EncodedPkOnNonStringPrimaryKeyField;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasLongPkWithKeyAncestor;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasLongPkWithStringAncestor;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasMultiplePkIdFields;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasMultiplePkNameFields;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasPkIdSortOnOneToMany;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasPkNameSortOnOneToMany;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasTwoOneToOnesWithSharedBaseClass;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasUnencodedStringPkWithKeyAncestor;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.HasUnencodedStringPkWithStringAncestor;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.LongParent;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.ManyToMany1;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.ManyToMany2;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.MultipleAncestors;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyLongBiChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyLongUniChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyStringBiChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToManyParentWithRootOnlyStringUniChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyLongBiChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyLongUniChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyStringBiChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.OneToOneParentWithRootOnlyStringUniChild;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.PkIdOnNonLongField;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.PkIdWithUnencodedStringPrimaryKey;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.PkMarkedAsAncestor;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.PkMarkedAsPkId;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.PkMarkedAsPkName;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.PkNameOnNonStringField;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.PkNameWithUnencodedStringPrimaryKey;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.SequenceOnEncodedStringPk;
import com.google.appengine.datanucleus.test.IllegalMappingsJDO.SequenceOnKeyPk;

import javax.jdo.JDOFatalUserException;
import javax.jdo.Query;

import org.datanucleus.metadata.InvalidMetaDataException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOMetaDataValidatorTest extends JDOTestCase {

  @Override
  protected void tearDown() throws Exception {
    // force a new pmf for each test
    if (pm != null && !pm.isClosed() && pm.currentTransaction().isActive()) {
      rollbackTxn();
    }
    pmf.close();
    super.tearDown();
  }

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
      assertTrue(e.getCause() instanceof InvalidMetaDataException);
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
      assertTrue(e.getCause() instanceof InvalidMetaDataException);
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
    beginTxn();
    pm.makePersistent(new OneToManyParentWithEagerlyFetchedEmbeddedChild());
    commitTxn();
  }

  public void testUniqueConstraint() {
    assertMetaDataException(new HasUniqueConstraint());
  }

  public void testUniqueConstraints() {
    assertMetaDataException(new HasUniqueConstraints());
  }

  public void testManyToMany() {
    assertMetaDataException(new ManyToMany1());
    assertMetaDataException(new ManyToMany2());
  }

  public void testEncodedStringPkWithSequence() {
    assertMetaDataException(new SequenceOnEncodedStringPk());
  }

  public void testKeyPkWithSequence() {
    assertMetaDataException(new SequenceOnKeyPk());
  }

  // Only applicable to earlier storage versions
/*  public void testHasMultipleRelationshipFieldsOfSameType() {
    assertMetaDataException(new Has2CollectionsOfSameType());
    assertMetaDataException(new Has2OneToOnesOfSameType());
    assertMetaDataException(new HasOneToOneAndOneToManyOfSameType());
    assertMetaDataException(new Has2CollectionsOfSameTypeChild());
    assertMetaDataException(new Has2CollectionsOfAssignableType());
    assertMetaDataException(new Has2CollectionsOfAssignableTypeSub());
  }*/

  public void testHasKeySubComponentSortOnOneToMany() {
    assertMetaDataException(new HasPkIdSortOnOneToMany());
    assertMetaDataException(new HasPkNameSortOnOneToMany());
  }

  public void testHasTwoOneToOnesWithSharedBaseClass() throws Exception {
    beginTxn();
    pm.makePersistent(new HasTwoOneToOnesWithSharedBaseClass());
    commitTxn();
  }

  private void assertMetaDataException(Object pojo) {
    beginTxn();
    try {
      pm.makePersistent(pojo);
      fail("expected exception");
    } catch (JDOFatalUserException e) {
      // good
      assertTrue(e.getCause().getClass().getName(),
                 e.getCause() instanceof InvalidMetaDataException);
      rollbackTxn();
    }
  }
}
