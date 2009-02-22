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
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  @Persistent(dependent = "true")
  private HasStringAncestorStringPkJDO cascadeAllChild;

  @Persistent(dependent = "true")
  private HasKeyAncestorStringPkJDO cascadeAllChildWithKeyAncestor;

  @Persistent(dependent = "false")
  private HasStringAncestorStringPkJDO cascadePersistChild;

  @Persistent(dependent = "false")
  @Extension(vendorName="datanucleus", key="cascade-persist", value="false")
  private HasStringAncestorStringPkJDO cascadeRemoveChild;

  public String getId() {
    return id;
  }

  public HasStringAncestorStringPkJDO getCascadeAllChild() {
    return cascadeAllChild;
  }

  public void setCascadeAllChild(HasStringAncestorStringPkJDO cascadeAllChild) {
    this.cascadeAllChild = cascadeAllChild;
  }

  public HasStringAncestorStringPkJDO getCascadePersistChild() {
    return cascadePersistChild;
  }

  public void setCascadePersistChild(HasStringAncestorStringPkJDO cascadePersistChild) {
    this.cascadePersistChild = cascadePersistChild;
  }

  public HasStringAncestorStringPkJDO getCascadeRemoveChild() {
    return cascadeRemoveChild;
  }

  public void setCascadeRemoveChild(HasStringAncestorStringPkJDO cascadeRemoveChild) {
    this.cascadeRemoveChild = cascadeRemoveChild;
  }

  public HasKeyAncestorStringPkJDO getCascadeAllChildWithKeyAncestor() {
    return cascadeAllChildWithKeyAncestor;
  }

  public void setCascadeAllChildWithKeyAncestor(
      HasKeyAncestorStringPkJDO cascadeAllChildWithKeyAncestor) {
    this.cascadeAllChildWithKeyAncestor = cascadeAllChildWithKeyAncestor;
  }
}
