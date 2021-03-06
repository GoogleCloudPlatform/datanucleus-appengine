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

import com.google.appengine.datanucleus.test.jpa.IgnorableMappingsJPA.HasInitialSequenceValue;
import com.google.appengine.datanucleus.test.jpa.IgnorableMappingsJPA.HasUniqueConstraint;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.EncodedPkOnNonPrimaryKeyField;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.EncodedPkOnNonStringPrimaryKeyField;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.HasLongPkWithStringAncestor;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.HasMultiplePkIdFields;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.HasMultiplePkNameFields;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.HasPkIdSortOnOneToMany;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.HasPkNameSortOnOneToMany;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.HasUnencodedStringPkWithStringAncestor;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.LongParent;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.ManyToMany1;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.ManyToMany2;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.MultipleAncestors;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToManyParentWithRootOnlyLongBiChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToManyParentWithRootOnlyLongUniChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToManyParentWithRootOnlyStringBiChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToManyParentWithRootOnlyStringUniChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToOneParentWithRootOnlyLongBiChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToOneParentWithRootOnlyLongUniChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToOneParentWithRootOnlyStringBiChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.OneToOneParentWithRootOnlyStringUniChild;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.PkIdOnNonLongField;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.PkIdWithUnencodedStringPrimaryKey;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.PkMarkedAsAncestor;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.PkMarkedAsPkId;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.PkMarkedAsPkName;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.PkNameOnNonStringField;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.PkNameWithUnencodedStringPrimaryKey;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.SequenceOnEncodedStringPk;
import com.google.appengine.datanucleus.test.jpa.IllegalMappingsJPA.SequenceOnKeyPk;

import javax.persistence.PersistenceException;

import org.datanucleus.metadata.InvalidMetaDataException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAMetaDataValidatorTest extends JPATestCase {

  public void testStringAncestorPlusNameOnlyPK() {
    HasUnencodedStringPkWithStringAncestor pojo = new HasUnencodedStringPkWithStringAncestor();
    pojo.id = "yar";
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
  public void testAncestorOfIllegalType_Long() {
    LongParent pojo = new LongParent();
    assertMetaDataException(pojo);
  }

  public void testUniqueConstraints() {
    assertMetaDataException(new HasUniqueConstraint());
  }

  public void testManyToMany() {
    assertMetaDataException(new ManyToMany1());
    assertMetaDataException(new ManyToMany2());
  }

  public void testInitialSequenceValue() {
    assertMetaDataException(new HasInitialSequenceValue());
  }

  private void assertMetaDataException(Object pojo) {
    beginTxn();
    try {
      em.persist(pojo);
      commitTxn();
      fail("expected exception");
    } catch (PersistenceException e) {
      // good
      assertTrue(e.getCause().getClass().getName(),
                 e.getCause() instanceof InvalidMetaDataException);
      rollbackTxn();
    }
  }

  public void testEncodedStringPkWithSequence() {
    assertMetaDataException(new SequenceOnEncodedStringPk());
  }

  public void testKeyPkWithSequence() {
    assertMetaDataException(new SequenceOnKeyPk());
  }

  // Only applicable to earlier storage versions
  /*public void testHasMultipleRelationshipFieldsOfSameType() {
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

}
