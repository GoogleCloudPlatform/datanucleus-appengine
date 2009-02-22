// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import java.util.ArrayList;
import java.util.List;

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
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Extension(vendorName = "datanucleus", key = "gae.parent-pk", value = "true")
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
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

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
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-id", value = "true")
    private Long illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class PkNameWithUnencodedStringPrimaryKey {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key = "gae.pk-name", value = "true")
    private String illegal;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyLongUniChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    private List<HasLongPkJDO> uniChildren = new ArrayList<HasLongPkJDO>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyLongBiChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

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
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    private List<HasUnencodedStringPkJDO> uniChildren = new ArrayList<HasUnencodedStringPkJDO>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithRootOnlyStringBiChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent(mappedBy = "parent")
    private List<RootOnlyStringBiOneToManyChild> biChildren = new ArrayList<RootOnlyStringBiOneToManyChild>();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyStringBiOneToManyChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    private OneToManyParentWithRootOnlyStringBiChild parent;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyLongUniChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    private HasLongPkJDO uniChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyLongBiChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

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
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    private HasUnencodedStringPkJDO uniChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToOneParentWithRootOnlyStringBiChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent
    private RootOnlyStringBiOneToManyChild biChild;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class RootOnlyStringBiOneToOneChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private String id;

    @Persistent(mappedBy = "biChild")
    private OneToOneParentWithRootOnlyStringBiChild parent;
  }

}
