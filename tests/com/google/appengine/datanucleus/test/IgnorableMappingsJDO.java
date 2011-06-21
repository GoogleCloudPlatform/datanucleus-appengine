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

import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.Embedded;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Unique;
import javax.jdo.annotations.Uniques;

/**
 * @author Max Ross <maxr@google.com>
 */
public class IgnorableMappingsJDO {

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithEagerlyFetchedChildList {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    public Long id;

    @Persistent(defaultFetchGroup = "true")
    @Element(dependent = "true")
    private List<HasKeyPkJDO> children;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithEagerlyFetchedChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    public Long id;

    @Persistent(dependent = "true", defaultFetchGroup = "true")
    private HasKeyPkJDO child;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class OneToManyParentWithEagerlyFetchedEmbeddedChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    public Long id;

    // this is actually ok because it's embedded
    @Persistent(dependent = "true", defaultFetchGroup = "true")
    @Embedded
    private HasKeyPkJDO child;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Unique(name = "blar", members = {"f1"})
  public static class HasUniqueConstraint {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    public Long id;

    @Persistent
    public String f1;
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Uniques({@Unique(name = "blar", members = {"f1"})})
  public static class HasUniqueConstraints {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    public Long id;

    @Persistent
    public String f1;
  }
}