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

import org.datanucleus.api.jpa.annotations.Extension;
import org.datanucleus.api.jpa.annotations.Extensions;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.OrderBy;

/**
 * @author Max Ross <maxr@google.com>
 */
public class IllegalMappingsJPA {

  @Entity
  public static class HasLongPkWithStringAncestor {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    private String illegal;
  }

  @Entity
  public static class HasUnencodedStringPkWithStringAncestor {

    @Id
    public String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    private String illegal;
  }

  @Entity
  public static class HasMultiplePkNameFields {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String firstIsOk;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String secondIsIllegal;
  }

  @Entity
  public static class HasMultiplePkIdFields {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long firstIsOk;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long secondIsIllegal;
  }

  @Entity
  public static class MultipleAncestors {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    private String firstIsOk;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
    private String secondIsIllegal;
  }

  @Entity
  public static class EncodedPkOnNonPrimaryKeyField {

    @Id
    public String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String illegal;
  }

  @Entity
  public static class EncodedPkOnNonStringPrimaryKeyField {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private Long id;
  }

  @Entity
  public static class PkNameOnNonStringField {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private Long illegal;
  }

  @Entity
  public static class PkIdOnNonLongField {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private String illegal;
  }

  @Entity
  public static class PkMarkedAsAncestor {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extensions({
      @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true"),
      @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")}
    )
    private String illegal;
  }

  @Entity
  public static class PkMarkedAsPkId {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long illegal;
  }

  @Entity
  public static class PkMarkedAsPkName {

    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String illegal;
  }

  @Entity
  public static class PkIdWithUnencodedStringPrimaryKey {

    @Id
    public String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long illegal;
  }

  @Entity
  public static class PkNameWithUnencodedStringPrimaryKey {

    @Id
    public String id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String illegal;
  }

  @Entity
  public static class OneToManyParentWithRootOnlyLongUniChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToMany(cascade = CascadeType.ALL)
    private List<HasLongPkJPA> uniChildren = new ArrayList<HasLongPkJPA>();
  }

  @Entity
  public static class OneToManyParentWithRootOnlyLongBiChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<RootOnlyLongBiOneToManyChild> biChildren = new ArrayList<RootOnlyLongBiOneToManyChild>();
  }

  @Entity
  public static class RootOnlyLongBiOneToManyChild {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @ManyToOne(fetch = FetchType.LAZY)
    private OneToManyParentWithRootOnlyLongBiChild parent;
  }

  @Entity
  public static class OneToManyParentWithRootOnlyStringUniChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToMany(cascade = CascadeType.ALL)
    private List<HasUnencodedStringPkJPA> uniChildren = new ArrayList<HasUnencodedStringPkJPA>();
  }

  @Entity
  public static class OneToManyParentWithRootOnlyStringBiChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<RootOnlyStringBiOneToManyChild> biChildren = new ArrayList<RootOnlyStringBiOneToManyChild>();
  }

  @Entity
  public static class RootOnlyStringBiOneToManyChild {
    @SuppressWarnings("unused")
    @Id
    private String id;

    @SuppressWarnings("unused")
    @ManyToOne(fetch = FetchType.EAGER)
    private OneToManyParentWithRootOnlyStringBiChild parent;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyLongUniChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private HasLongPkJPA uniChild;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyLongBiChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToOne(fetch = FetchType.LAZY)
    private RootOnlyLongBiOneToOneChild biChild;
  }

  @Entity
  public static class RootOnlyLongBiOneToOneChild {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @OneToOne(mappedBy = "biChild", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private OneToOneParentWithRootOnlyLongBiChild parent;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyStringUniChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private HasUnencodedStringPkJPA uniChild;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyStringBiChild {
    @Id
    public String id;

    @SuppressWarnings("unused")
    @OneToOne(fetch = FetchType.LAZY)
    private RootOnlyStringBiOneToOneChild biChild;
  }

  @Entity
  public static class RootOnlyStringBiOneToOneChild {
    @SuppressWarnings("unused")
    @Id
    private String id;

    @SuppressWarnings("unused")
    @OneToOne(mappedBy = "biChild", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private OneToOneParentWithRootOnlyStringBiChild parent;
  }

  @Entity
  public static class LongParent {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @SuppressWarnings("unused")
    @Extension(vendorName = "datanucleus", key="gae.parent-pk", value="true")
    private Long illegal;
  }

  @Entity
  public static class ManyToMany1 {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @ManyToMany(mappedBy = "manyToMany")
    private List<ManyToMany2> manyToMany;
  }

  @Entity
  public static class ManyToMany2 {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @ManyToMany
    private List<ManyToMany1> manyToMany;
  }

  @Entity
  public static class SequenceOnEncodedStringPk {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy=GenerationType.SEQUENCE)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
    private String id;
  }

  @Entity
  public static class SequenceOnKeyPk {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy=GenerationType.SEQUENCE)
    private Key id;
  }

  @Entity
  public static class Has2CollectionsOfSameType {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    private List<Flight> flights1;

    @SuppressWarnings("unused")
    @OneToMany
    private List<Flight> flights2;
  }

  @Entity
  public static class Has2OneToOnesOfSameType {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    private Flight f1;

    @SuppressWarnings("unused")
    @OneToMany
    private Flight f2;
  }

  @Entity
  public static class HasOneToOneAndOneToManyOfSameType {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    private List<Flight> flights;

    @SuppressWarnings("unused")
    @OneToMany
    private Flight f2;
  }

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class Has2CollectionsOfSameTypeParent {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    private List<Flight> flights1;
  }

  @Entity
  public static class Has2CollectionsOfSameTypeChild extends Has2CollectionsOfSameTypeParent {
    @SuppressWarnings("unused")
    @OneToMany
    private List<Flight> flights2;
  }

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class Has2CollectionsOfAssignableBaseTypeSuper {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;
  }

  @Entity
  public static class Has2CollectionsOfAssignableBaseTypeSub extends Has2CollectionsOfAssignableBaseTypeSuper {
    @SuppressWarnings("unused")
    private String str;
  }

  @Entity
  public static class Has2CollectionsOfAssignableType {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    private List<Has2CollectionsOfAssignableBaseTypeSuper> superList;

    @SuppressWarnings("unused")
    @OneToMany
    private List<Has2CollectionsOfAssignableBaseTypeSub> subList;
  }

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class Has2CollectionsOfAssignableTypeSuper {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    private List<Has2CollectionsOfAssignableBaseTypeSuper> superList;
  }

  @Entity
  public static class Has2CollectionsOfAssignableTypeSub extends Has2CollectionsOfAssignableTypeSuper {
    @SuppressWarnings("unused")
    @OneToMany
    private List<Has2CollectionsOfAssignableBaseTypeSub> subList;
  }

  @Entity
  public static class HasPkIdSortOnOneToMany {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    @OrderBy("id")
    private List<HasEncodedStringPkSeparateIdFieldJPA> list;
  }

  @Entity
  public static class HasPkNameSortOnOneToMany {
    @SuppressWarnings("unused")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key id;

    @SuppressWarnings("unused")
    @OneToMany
    @OrderBy("name")
    private List<HasEncodedStringPkSeparateNameFieldJPA> list;
  }
}
