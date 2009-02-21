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
public class HasOneToOneLongPkJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  public Long getId() {
    return id;
  }

  @Persistent(dependent = "true")
  private Flight flight;

  @Persistent(dependent = "true")
  private HasKeyPkJDO hasKeyPK;

  @Persistent(dependent = "true")
  private HasOneToOneLongPkParentJDO hasParent;

  @Persistent(dependent = "true")
  private HasOneToOneLongPkParentKeyPkJDO hasParentKeyPK;

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

  public HasOneToOneLongPkParentJDO getHasParent() {
    return hasParent;
  }

  public void setHasParent(HasOneToOneLongPkParentJDO hasParent) {
    this.hasParent = hasParent;
  }

  public HasOneToOneLongPkParentKeyPkJDO getHasParentKeyPK() {
    return hasParentKeyPK;
  }

  public void setHasParentKeyPK(HasOneToOneLongPkParentKeyPkJDO hasParentKeyPK) {
    this.hasParentKeyPK = hasParentKeyPK;
  }
}