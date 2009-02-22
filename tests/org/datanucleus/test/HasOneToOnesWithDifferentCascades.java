// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

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
public class HasOneToOnesWithDifferentCascades {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
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
