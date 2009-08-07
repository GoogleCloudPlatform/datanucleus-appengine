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

import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.appengine.jpa.DatastoreEntityManager;
import org.datanucleus.test.IgnorableMappingsJDO.HasUniqueConstraint;
import org.datanucleus.test.IgnorableMappingsJPA;
import org.datanucleus.test.IgnorableMappingsJPA.OneToManyParentWithEagerlyFetchedChild;
import org.datanucleus.test.IgnorableMappingsJPA.OneToManyParentWithEagerlyFetchedChildList;
import org.datanucleus.test.IllegalMappingsJPA.EncodedPkOnNonPrimaryKeyField;
import org.datanucleus.test.IllegalMappingsJPA.EncodedPkOnNonStringPrimaryKeyField;
import org.datanucleus.test.IllegalMappingsJPA.HasLongPkWithStringAncestor;
import org.datanucleus.test.IllegalMappingsJPA.HasMultiplePkIdFields;
import org.datanucleus.test.IllegalMappingsJPA.HasMultiplePkNameFields;
import org.datanucleus.test.IllegalMappingsJPA.HasUnencodedStringPkWithStringAncestor;
import org.datanucleus.test.IllegalMappingsJPA.LongParent;
import org.datanucleus.test.IllegalMappingsJPA.ManyToMany1;
import org.datanucleus.test.IllegalMappingsJPA.ManyToMany2;
import org.datanucleus.test.IllegalMappingsJPA.MultipleAncestors;
import org.datanucleus.test.IllegalMappingsJPA.OneToManyParentWithRootOnlyLongBiChild;
import org.datanucleus.test.IllegalMappingsJPA.OneToManyParentWithRootOnlyLongUniChild;
import org.datanucleus.test.IllegalMappingsJPA.OneToManyParentWithRootOnlyStringBiChild;
import org.datanucleus.test.IllegalMappingsJPA.OneToManyParentWithRootOnlyStringUniChild;
import org.datanucleus.test.IllegalMappingsJPA.OneToOneParentWithRootOnlyLongBiChild;
import org.datanucleus.test.IllegalMappingsJPA.OneToOneParentWithRootOnlyLongUniChild;
import org.datanucleus.test.IllegalMappingsJPA.OneToOneParentWithRootOnlyStringBiChild;
import org.datanucleus.test.IllegalMappingsJPA.OneToOneParentWithRootOnlyStringUniChild;
import org.datanucleus.test.IllegalMappingsJPA.PkIdOnNonLongField;
import org.datanucleus.test.IllegalMappingsJPA.PkIdWithUnencodedStringPrimaryKey;
import org.datanucleus.test.IllegalMappingsJPA.PkMarkedAsAncestor;
import org.datanucleus.test.IllegalMappingsJPA.PkMarkedAsPkId;
import org.datanucleus.test.IllegalMappingsJPA.PkMarkedAsPkName;
import org.datanucleus.test.IllegalMappingsJPA.PkNameOnNonStringField;
import org.datanucleus.test.IllegalMappingsJPA.PkNameWithUnencodedStringPrimaryKey;

import javax.persistence.PersistenceException;

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

  public void testOneToManyWithEagerlyFetchedChildList() {
    OneToManyParentWithEagerlyFetchedChildList pojo = new OneToManyParentWithEagerlyFetchedChildList();
    assertMetaDataException(pojo);
  }

  public void testOneToManyWithEagerlyFetchedChild() {
    OneToManyParentWithEagerlyFetchedChild pojo = new OneToManyParentWithEagerlyFetchedChild();
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
    assertMetaDataException(new IgnorableMappingsJPA.HasInitialSequenceValue());
  }

  private void assertMetaDataException(Object pojo) {
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

  public void testIsJPA() {
    MetaDataManager mdm = ((DatastoreEntityManager) em).getObjectManager().getOMFContext().getMetaDataManager();
    MetaDataValidator mdv = new MetaDataValidator(null, mdm, null);
    assertTrue(mdv.isJPA());
  }
}
