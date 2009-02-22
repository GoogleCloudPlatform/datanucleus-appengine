// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.jpa.annotations.Extension;
import org.datanucleus.jpa.annotations.Extensions;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
public class IllegalMappingsJPA {

  @Entity
  public static class HasLongPkWithStringAncestor {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    private String illegal;
  }

  @Entity
  public static class HasUnencodedStringPkWithStringAncestor {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    private String illegal;
  }

  @Entity
  public static class HasMultiplePkNameFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private String firstIsOk;

    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private String secondIsIllegal;
  }

  @Entity
  public static class HasMultiplePkIdFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private Long firstIsOk;

    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private Long secondIsIllegal;
  }

  @Entity
  public static class MultipleAncestors {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    private String firstIsOk;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    private String secondIsIllegal;
  }

  @Entity
  public static class EncodedPkOnNonPrimaryKeyField {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String illegal;
  }

  @Entity
  public static class EncodedPkOnNonStringPrimaryKeyField {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private Long id;
  }

  @Entity
  public static class PkNameOnNonStringField {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private Long illegal;
  }

  @Entity
  public static class PkIdOnNonLongField {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private String illegal;
  }

  @Entity
  public static class PkMarkedAsAncestor {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extensions({
      @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true"),
      @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")}
    )
    private String illegal;
  }

  @Entity
  public static class PkMarkedAsPkId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private Long illegal;
  }

  @Entity
  public static class PkMarkedAsPkName {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private String illegal;
  }

  @Entity
  public static class PkIdWithUnencodedStringPrimaryKey {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private Long illegal;
  }

  @Entity
  public static class PkNameWithUnencodedStringPrimaryKey {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private String illegal;
  }

  @Entity
  public static class OneToManyParentWithRootOnlyLongUniChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToMany(cascade = CascadeType.ALL)
    private List<HasLongPkJDO> uniChildren = new ArrayList<HasLongPkJDO>();
  }

  @Entity
  public static class OneToManyParentWithRootOnlyLongBiChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<RootOnlyLongBiOneToManyChild> biChildren = new ArrayList<RootOnlyLongBiOneToManyChild>();
  }

  @Entity
  public static class RootOnlyLongBiOneToManyChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    private OneToManyParentWithRootOnlyLongBiChild parent;
  }

  @Entity
  public static class OneToManyParentWithRootOnlyStringUniChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToMany(cascade = CascadeType.ALL)
    private List<HasUnencodedStringPkJDO> uniChildren = new ArrayList<HasUnencodedStringPkJDO>();
  }

  @Entity
  public static class OneToManyParentWithRootOnlyStringBiChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<RootOnlyStringBiOneToManyChild> biChildren = new ArrayList<RootOnlyStringBiOneToManyChild>();
  }

  @Entity
  public static class RootOnlyStringBiOneToManyChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @ManyToOne
    private OneToManyParentWithRootOnlyStringBiChild parent;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyLongUniChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToOne(cascade = CascadeType.ALL)
    private HasLongPkJDO uniChild;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyLongBiChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToOne
    private RootOnlyLongBiOneToOneChild biChild;
  }

  @Entity
  public static class RootOnlyLongBiOneToOneChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @OneToOne(mappedBy = "biChild", cascade = CascadeType.ALL)
    private OneToOneParentWithRootOnlyLongBiChild parent;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyStringUniChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToOne(cascade = CascadeType.ALL)
    private HasUnencodedStringPkJDO uniChild;
  }

  @Entity
  public static class OneToOneParentWithRootOnlyStringBiChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToOne
    private RootOnlyStringBiOneToOneChild biChild;
  }

  @Entity
  public static class RootOnlyStringBiOneToOneChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private String id;

    @OneToOne(mappedBy = "biChild", cascade = CascadeType.ALL)
    private OneToOneParentWithRootOnlyStringBiChild parent;
  }

}