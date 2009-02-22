// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.jpa.annotations.Extension;
import org.datanucleus.jpa.annotations.Extensions;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

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


}