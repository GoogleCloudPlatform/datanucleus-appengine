// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasOneToOnesWithDifferentCascadesJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName="datanucleus", key="encoded-pk", value="true")
  private String id;

  @Persistent(dependent = "true")
  private HasAncestorJDO cascadeAllChild;

  @Persistent(dependent = "true")
  private HasKeyAncestorKeyStringPkJDO cascadeAllChildWithKeyAncestor;

  @Persistent(dependent = "false")
  private HasAncestorJDO cascadePersistChild;

  @Persistent(dependent = "false")
  @Extension(vendorName="datanucleus", key="cascade-persist", value="false")
  private HasAncestorJDO cascadeRemoveChild;

  public String getId() {
    return id;
  }

  public HasAncestorJDO getCascadeAllChild() {
    return cascadeAllChild;
  }

  public void setCascadeAllChild(HasAncestorJDO cascadeAllChild) {
    this.cascadeAllChild = cascadeAllChild;
  }

  public HasAncestorJDO getCascadePersistChild() {
    return cascadePersistChild;
  }

  public void setCascadePersistChild(HasAncestorJDO cascadePersistChild) {
    this.cascadePersistChild = cascadePersistChild;
  }

  public HasAncestorJDO getCascadeRemoveChild() {
    return cascadeRemoveChild;
  }

  public void setCascadeRemoveChild(HasAncestorJDO cascadeRemoveChild) {
    this.cascadeRemoveChild = cascadeRemoveChild;
  }

  public HasKeyAncestorKeyStringPkJDO getCascadeAllChildWithKeyAncestor() {
    return cascadeAllChildWithKeyAncestor;
  }

  public void setCascadeAllChildWithKeyAncestor(
      HasKeyAncestorKeyStringPkJDO cascadeAllChildWithKeyAncestor) {
    this.cascadeAllChildWithKeyAncestor = cascadeAllChildWithKeyAncestor;
  }
}
