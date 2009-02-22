// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.Extensions;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
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

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    @Persistent
    private Key illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasLongPkWithStringAncestor {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasUnencodedStringPkWithKeyAncestor {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    @Persistent
    private Key illegal;

    public void setId(String id) {
      this.id = id;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasUnencodedStringPkWithStringAncestor {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasMultiplePkNameFields {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    @Persistent
    private String firstIsOk;

    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    @Persistent
    private String secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasMultiplePkIdFields {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    @Persistent
    private Long firstIsOk;

    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    @Persistent
    private Long secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class MultipleAncestors {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    @Persistent
    private String firstIsOk;

    @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")
    @Persistent
    private String secondIsIllegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class EncodedPkOnNonPrimaryKeyField {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    @Persistent
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class EncodedPkOnNonStringPrimaryKeyField {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private Long id;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkNameOnNonStringField {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkIdOnNonLongField {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true")
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsAncestor {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extensions({
      @Extension(vendorName = "datanucleus", key = "encoded-pk", value = "true"),
      @Extension(vendorName = "datanucleus", key = "parent-pk", value = "true")}
    )
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsPkId {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkMarkedAsPkName {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkIdWithUnencodedStringPrimaryKey {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "pk-id", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkNameWithUnencodedStringPrimaryKey {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "pk-name", value = "true")
    private String illegal;
  }

}
