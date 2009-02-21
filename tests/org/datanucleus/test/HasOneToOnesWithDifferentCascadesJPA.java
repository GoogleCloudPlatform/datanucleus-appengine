// Copyright 2008 Google Inc. All Rights Reserved.
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
  @Extension(vendorName="datanucleus", key="encoded-pk", value="true")
  private String id;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "cascadeall")
  private HasAncestorJPA cascadeAllChild;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "cascadeallwithkeyancestor")
  private HasKeyAncestorKeyStringPkJPA cascadeAllChildWithKeyAncestor;

  @OneToOne(cascade = CascadeType.PERSIST)
  @JoinColumn(name = "cascadepersist")
  private HasAncestorJPA cascadePersistChild;

  @OneToOne(cascade = CascadeType.REMOVE)
  @JoinColumn(name = "cascaderemove")
  private HasAncestorJPA cascadeRemoveChild;

  public String getId() {
    return id;
  }

  public HasAncestorJPA getCascadeAllChild() {
    return cascadeAllChild;
  }

  public void setCascadeAllChild(HasAncestorJPA cascadeAllChild) {
    this.cascadeAllChild = cascadeAllChild;
  }

  public HasAncestorJPA getCascadePersistChild() {
    return cascadePersistChild;
  }

  public void setCascadePersistChild(HasAncestorJPA cascadePersistChild) {
    this.cascadePersistChild = cascadePersistChild;
  }

  public HasAncestorJPA getCascadeRemoveChild() {
    return cascadeRemoveChild;
  }

  public void setCascadeRemoveChild(HasAncestorJPA cascadeRemoveChild) {
    this.cascadeRemoveChild = cascadeRemoveChild;
  }

  public HasKeyAncestorKeyStringPkJPA getCascadeAllChildWithKeyAncestor() {
    return cascadeAllChildWithKeyAncestor;
  }

  public void setCascadeAllChildWithKeyAncestor(
      HasKeyAncestorKeyStringPkJPA cascadeAllChildWithKeyAncestor) {
    this.cascadeAllChildWithKeyAncestor = cascadeAllChildWithKeyAncestor;
  }
}
