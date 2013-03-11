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
package com.google.appengine.datanucleus.test.jdo;

import com.google.appengine.api.datastore.Key;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.Extensions;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.Order;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
public class IllegalMappingsJDO {

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasLongPkWithKeyAncestor {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private Key illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasLongPkWithStringAncestor {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasUnencodedStringPkWithKeyAncestor {

    @PrimaryKey
    public String id;

    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private Key illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasUnencodedStringPkWithStringAncestor {

    @PrimaryKey
    public String id;

    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasMultiplePkNameFields {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    @Persistent
    private String firstIsOk;

    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    @Persistent
    private String secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasMultiplePkIdFields {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    @Persistent
    private Long firstIsOk;

    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    @Persistent
    private Long secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class MultipleAncestors {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String firstIsOk;

    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class EncodedPkOnNonPrimaryKeyField {

    @PrimaryKey
    public String id;

    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class EncodedPkOnNonStringPrimaryKeyField {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private Long id;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkNameOnNonStringField {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkIdOnNonLongField {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsAncestor {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extensions({
      @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true"),
      @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")}
    )
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsPkId {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsPkName {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkIdWithUnencodedStringPrimaryKey {

    @PrimaryKey
    public String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkNameWithUnencodedStringPrimaryKey {

    @PrimaryKey
    public String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyLongUniChild {
    @PrimaryKey
    public String id;

    @Persistent
    private List<HasLongPkJDO> uniChildren = new ArrayList<HasLongPkJDO>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyLongBiChild {
    @PrimaryKey
    public String id;

    @Persistent(mappedBy = "parent")
    private List<RootOnlyLongBiOneToManyChild> biChildren = new ArrayList<RootOnlyLongBiOneToManyChild>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyLongBiOneToManyChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    private OneToManyParentWithRootOnlyLongBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyStringUniChild {
    @PrimaryKey
    public String id;

    @Persistent
    private List<HasUnencodedStringPkJDO> uniChildren = new ArrayList<HasUnencodedStringPkJDO>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyStringBiChild {
    @PrimaryKey
    public String id;

    @Persistent(mappedBy = "parent")
    private List<RootOnlyStringBiOneToManyChild> biChildren = new ArrayList<RootOnlyStringBiOneToManyChild>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyStringBiOneToManyChild {
    @PrimaryKey
    public String id;

    @Persistent
    private OneToManyParentWithRootOnlyStringBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyLongUniChild {
    @PrimaryKey
    public String id;

    @Persistent
    private HasLongPkJDO uniChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyLongBiChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    private RootOnlyLongBiOneToManyChild biChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyLongBiOneToOneChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent(mappedBy = "biChild")
    private OneToOneParentWithRootOnlyLongBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyStringUniChild {
    @PrimaryKey
    public String id;

    @Persistent
    private HasUnencodedStringPkJDO uniChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyStringBiChild {
    @PrimaryKey
    public String id;

    @Persistent
    private RootOnlyStringBiOneToManyChild biChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyStringBiOneToOneChild {
    @PrimaryKey
    public String id;

    @Persistent(mappedBy = "biChild")
    private OneToOneParentWithRootOnlyStringBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class LongParent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    @Extension(vendorName = "datanucleus", key="gae.parent-pk", value="true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class ManyToMany1 {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent(mappedBy = "manyToMany")
    private List<ManyToMany2> manyToMany;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class ManyToMany2 {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent(mappedBy = "manyToMany")
    private List<ManyToMany1> manyToMany;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class SequenceOnEncodedStringPk {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class SequenceOnKeyPk {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    private Key id;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfSameType {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    private List<Flight> flights1;

    @Persistent
    private List<Flight> flights2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2OneToOnesOfSameType {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    private Flight f1;

    @Persistent
    private Flight f2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasOneToOneAndOneToManyOfSameType {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    private List<Flight> flights;

    @Persistent
    private Flight f2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class Has2CollectionsOfSameTypeParent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Flight> flights1;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfSameTypeChild extends Has2CollectionsOfSameTypeParent {
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Flight> flights2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class Has2CollectionsOfAssignableBaseTypeSuper {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    private String name;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfAssignableBaseTypeSub extends Has2CollectionsOfAssignableBaseTypeSuper {
    @SuppressWarnings("unused")
    private String str;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfAssignableType {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSuper> superList;

    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSub> subList;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class Has2CollectionsOfAssignableTypeSuper {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSuper> superList;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfAssignableTypeSub extends Has2CollectionsOfAssignableTypeSuper {
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSub> subList;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasTwoOneToOnesWithSharedBaseClass {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    private HasSharedBaseClass1 hsbc1;

    @Persistent
    private HasSharedBaseClass2 hsbc2;

    public HasSharedBaseClass1 getHsbc1() {
      return hsbc1;
    }

    public void setHsbc1(HasSharedBaseClass1 hsbc1) {
      this.hsbc1 = hsbc1;
    }

    public HasSharedBaseClass2 getHsbc2() {
      return hsbc2;
    }

    public void setHsbc2(HasSharedBaseClass2 hsbc2) {
      this.hsbc2 = hsbc2;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public abstract static class SharedBaseClass {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class HasSharedBaseClass1 extends SharedBaseClass {
    @SuppressWarnings("unused")
    private String str;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class HasSharedBaseClass2 extends SharedBaseClass {
    @SuppressWarnings("unused")
    private String str;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasPkIdSortOnOneToMany {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id"))
    private List<HasEncodedStringPkSeparateIdFieldJDO> list;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasPkNameSortOnOneToMany {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name"))
    private List<HasEncodedStringPkSeparateNameFieldJDO> list;
  }
}
