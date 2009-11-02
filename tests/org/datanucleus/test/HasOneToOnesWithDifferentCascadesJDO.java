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
  private HasStringAncestorKeyPkJDO cascadePersistChild;

  @Persistent(dependent = "false")
  @Extension(vendorName="datanucleus", key="cascade-persist", value="false")
  private HasKeyAncestorKeyPkJDO cascadeRemoveChild;

  public String getId() {
    return id;
  }

  public HasStringAncestorStringPkJDO getCascadeAllChild() {
    return cascadeAllChild;
  }

  public void setCascadeAllChild(HasStringAncestorStringPkJDO cascadeAllChild) {
    this.cascadeAllChild = cascadeAllChild;
  }

  public HasStringAncestorKeyPkJDO getCascadePersistChild() {
    return cascadePersistChild;
  }

  public void setCascadePersistChild(HasStringAncestorKeyPkJDO cascadePersistChild) {
    this.cascadePersistChild = cascadePersistChild;
  }

  public HasKeyAncestorKeyPkJDO getCascadeRemoveChild() {
    return cascadeRemoveChild;
  }

  public void setCascadeRemoveChild(HasKeyAncestorKeyPkJDO cascadeRemoveChild) {
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
