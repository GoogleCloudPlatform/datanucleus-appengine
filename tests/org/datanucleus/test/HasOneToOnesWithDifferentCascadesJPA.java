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

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToOnesWithDifferentCascadesJPA {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "cascadeall")
  private HasStringAncestorStringPkJPA cascadeAllChild;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "cascadeallwithkeyancestor")
  private HasKeyAncestorStringPkJPA cascadeAllChildWithKeyAncestor;

  @OneToOne(cascade = CascadeType.PERSIST)
  @JoinColumn(name = "cascadepersist")
  private HasStringAncestorStringPkJPA cascadePersistChild;

  @OneToOne(cascade = CascadeType.REMOVE)
  @JoinColumn(name = "cascaderemove")
  private HasStringAncestorStringPkJPA cascadeRemoveChild;

  public String getId() {
    return id;
  }

  public HasStringAncestorStringPkJPA getCascadeAllChild() {
    return cascadeAllChild;
  }

  public void setCascadeAllChild(HasStringAncestorStringPkJPA cascadeAllChild) {
    this.cascadeAllChild = cascadeAllChild;
  }

  public HasStringAncestorStringPkJPA getCascadePersistChild() {
    return cascadePersistChild;
  }

  public void setCascadePersistChild(HasStringAncestorStringPkJPA cascadePersistChild) {
    this.cascadePersistChild = cascadePersistChild;
  }

  public HasStringAncestorStringPkJPA getCascadeRemoveChild() {
    return cascadeRemoveChild;
  }

  public void setCascadeRemoveChild(HasStringAncestorStringPkJPA cascadeRemoveChild) {
    this.cascadeRemoveChild = cascadeRemoveChild;
  }

  public HasKeyAncestorStringPkJPA getCascadeAllChildWithKeyAncestor() {
    return cascadeAllChildWithKeyAncestor;
  }

  public void setCascadeAllChildWithKeyAncestor(
      HasKeyAncestorStringPkJPA cascadeAllChildWithKeyAncestor) {
    this.cascadeAllChildWithKeyAncestor = cascadeAllChildWithKeyAncestor;
  }
}
