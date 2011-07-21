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
package com.google.appengine.datanucleus.test;

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

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private Key illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasLongPkWithStringAncestor {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasUnencodedStringPkWithKeyAncestor {

    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private Key illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasUnencodedStringPkWithStringAncestor {

    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasMultiplePkNameFields {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    @Persistent
    private String firstIsOk;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    @Persistent
    private String secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasMultiplePkIdFields {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    @Persistent
    private Long firstIsOk;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    @Persistent
    private Long secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class MultipleAncestors {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String firstIsOk;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    @Persistent
    private String secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class EncodedPkOnNonPrimaryKeyField {

    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class EncodedPkOnNonStringPrimaryKeyField {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private Long id;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkNameOnNonStringField {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkIdOnNonLongField {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsAncestor {

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsPkName {

    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkIdWithUnencodedStringPrimaryKey {

    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkNameWithUnencodedStringPrimaryKey {

    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyLongUniChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent
    private List<HasLongPkJDO> uniChildren = new ArrayList<HasLongPkJDO>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyLongBiChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent(mappedBy = "parent")
    private List<RootOnlyLongBiOneToManyChild> biChildren = new ArrayList<RootOnlyLongBiOneToManyChild>();
  }

  @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
    @Persistent
    private List<HasUnencodedStringPkJDO> uniChildren = new ArrayList<HasUnencodedStringPkJDO>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyStringBiChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent(mappedBy = "parent")
    private List<RootOnlyStringBiOneToManyChild> biChildren = new ArrayList<RootOnlyStringBiOneToManyChild>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyStringBiOneToManyChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent
    private OneToManyParentWithRootOnlyStringBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyLongUniChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @Persistent(mappedBy = "biChild")
    private OneToOneParentWithRootOnlyLongBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyStringUniChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent
    private HasUnencodedStringPkJDO uniChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyStringBiChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent
    private RootOnlyStringBiOneToManyChild biChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyStringBiOneToOneChild {
    @PrimaryKey
    public String id;

    @SuppressWarnings("unused")
    @Persistent(mappedBy = "biChild")
    private OneToOneParentWithRootOnlyStringBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class LongParent {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @Persistent
    @Extension(vendorName = "datanucleus", key="gae.parent-pk", value="true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class ManyToMany1 {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent(mappedBy = "manyToMany")
    private List<ManyToMany2> manyToMany;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class ManyToMany2 {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent(mappedBy = "manyToMany")
    private List<ManyToMany1> manyToMany;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class SequenceOnEncodedStringPk {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class SequenceOnKeyPk {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    private Key id;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfSameType {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    private List<Flight> flights1;

    @SuppressWarnings("unused")
    @Persistent
    private List<Flight> flights2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2OneToOnesOfSameType {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    private Flight f1;

    @SuppressWarnings("unused")
    @Persistent
    private Flight f2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasOneToOneAndOneToManyOfSameType {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    private List<Flight> flights;

    @SuppressWarnings("unused")
    @Persistent
    private Flight f2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class Has2CollectionsOfSameTypeParent {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Flight> flights1;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfSameTypeChild extends Has2CollectionsOfSameTypeParent {
    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Flight> flights2;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class Has2CollectionsOfAssignableBaseTypeSuper {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSuper> superList;

    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSub> subList;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class Has2CollectionsOfAssignableTypeSuper {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSuper> superList;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class Has2CollectionsOfAssignableTypeSub extends Has2CollectionsOfAssignableTypeSuper {
    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name DESC"))
    private List<Has2CollectionsOfAssignableBaseTypeSub> subList;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasTwoOneToOnesWithSharedBaseClass {
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id"))
    private List<HasEncodedStringPkSeparateIdFieldJDO> list;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasPkNameSortOnOneToMany {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @Persistent
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="name"))
    private List<HasEncodedStringPkSeparateNameFieldJDO> list;
  }
}
