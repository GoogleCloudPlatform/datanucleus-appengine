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
}
