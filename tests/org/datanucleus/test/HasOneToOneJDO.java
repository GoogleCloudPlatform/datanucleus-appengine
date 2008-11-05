// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasOneToOneJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent(dependent = "true")
  @Column(name="flight_id")
  private Flight flight;

  @Persistent(dependent = "true")
  @Column(name = "haskeypk_id")
  private HasKeyPkJDO hasKeyPK;

  @Persistent(dependent = "true")
  @Column(name = "hasparent_id")
  private HasOneToOneParentJDO hasParent;

  @Persistent(dependent = "true")
  @Column(name = "hasparentkeypk_id")
  private HasOneToOneParentKeyPkJDO hasParentKeyPK;

  public String getId() {
    return id;
  }

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

  public HasOneToOneParentJDO getHasParent() {
    return hasParent;
  }

  public void setHasParent(HasOneToOneParentJDO hasParent) {
    this.hasParent = hasParent;
  }

  public HasOneToOneParentKeyPkJDO getHasParentKeyPK() {
    return hasParentKeyPK;
  }

  public void setHasParentKeyPK(HasOneToOneParentKeyPkJDO hasParentKeyPK) {
    this.hasParentKeyPK = hasParentKeyPK;
  }
}
