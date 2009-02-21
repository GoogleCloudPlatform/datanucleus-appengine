// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasOneToOneStringPkJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Persistent(dependent = "true")
  private Flight flight;

  @Persistent(dependent = "true")
  private HasKeyPkJDO hasKeyPK;

  @Persistent(dependent = "true")
  private HasOneToOneStringPkParentJDO hasParent;

  @Persistent(dependent = "true")
  private HasOneToOneStringPkParentKeyPkJDO hasParentKeyPK;

  public Flight getFlight() {
    return flight;
  }

  public void setFlight(Flight flight) {
    this.flight = flight;
  }

  public HasKeyPkJDO getHasKeyPK() {
    return hasKeyPK;
  }

  public void setHasKeyPK(HasKeyPkJDO hasKeyPK) {
    this.hasKeyPK = hasKeyPK;
  }

  public HasOneToOneStringPkParentJDO getHasParent() {
    return hasParent;
  }

  public void setHasParent(HasOneToOneStringPkParentJDO hasParent) {
    this.hasParent = hasParent;
  }

  public HasOneToOneStringPkParentKeyPkJDO getHasParentKeyPK() {
    return hasParentKeyPK;
  }

  public void setHasParentKeyPK(HasOneToOneStringPkParentKeyPkJDO hasParentKeyPK) {
    this.hasParentKeyPK = hasParentKeyPK;
  }
}