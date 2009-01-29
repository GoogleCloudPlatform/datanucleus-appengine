// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasOneToManyJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  private String val;

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private List<BidirectionalChildJDO> bidirChildren = new ArrayList<BidirectionalChildJDO>();

  @Element(dependent = "true")
  private List<Flight> flights = new ArrayList<Flight>();

  @Element(dependent = "true")
  private List<HasKeyPkJDO> hasKeyPks = new ArrayList<HasKeyPkJDO>();

  public String getId() {
    return id;
  }

  public List<BidirectionalChildJDO> getBidirChildren() {
    return bidirChildren;
  }

  public void setBidirChildren(List<BidirectionalChildJDO> bidirChildren) {
    this.bidirChildren = bidirChildren;
  }

  public List<Flight> getFlights() {
    return flights;
  }

  public void setFlights(List<Flight> flights) {
    this.flights = flights;
  }

  public List<HasKeyPkJDO> getHasKeyPks() {
    return hasKeyPks;
  }

  public void setHasKeyPks(List<HasKeyPkJDO> hasKeyPks) {
    this.hasKeyPks = hasKeyPks;
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }
}