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
public class HasOneToManyListJDO implements HasOneToManyJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  private String val;

  public String getId() {
    return id;
  }

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private List<BidirectionalChildListJDO> bidirChildren = new ArrayList<BidirectionalChildListJDO>();

  @Element(dependent = "true")
  private List<Flight> flights = new ArrayList<Flight>();

  @Element(dependent = "true")
  private List<HasKeyPkJDO> hasKeyPks = new ArrayList<HasKeyPkJDO>();

  public List<BidirectionalChildJDO> getBidirChildren() {
    return (List) bidirChildren;
  }

  public void addBidirChild(BidirectionalChildJDO bidirChild) {
    bidirChildren.add((BidirectionalChildListJDO) bidirChild);
  }

  public List<Flight> getFlights() {
    return flights;
  }

  public void addFlight(Flight flight) {
    this.flights.add(flight);
  }

  public List<HasKeyPkJDO> getHasKeyPks() {
    return hasKeyPks;
  }

  public void addHasKeyPk(HasKeyPkJDO hasKeyPk) {
    this.hasKeyPks.add(hasKeyPk);
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }

  public void nullBidirChildren() {
    bidirChildren = null;
  }

  public void nullFlights() {
    flights = null;
  }

  public void nullHasKeyPks() {
    hasKeyPks = null;
  }

  public void clearBidirChildren() {
    bidirChildren.clear();
  }

  public void clearFlights() {
    flights.clear();
  }

  public void clearHasKeyPks() {
    hasKeyPks.clear();
  }

  public void addBidirChildAtPosition(BidirectionalChildJDO bidir, int pos) {
    bidirChildren.set(pos, (BidirectionalChildListJDO) bidir);
  }

  public void addFlightAtPosition(Flight f, int pos) {
    flights.set(pos, f);
  }

  public void addHasKeyPkAtPosition(HasKeyPkJDO hasKeyPk, int pos) {
    hasKeyPks.set(pos, hasKeyPk);
  }

  public void removeBidirChildAtPosition(int i) {
    bidirChildren.remove(i);
  }

  public void removeFlightAtPosition(int i) {
    flights.remove(i);
  }

  public void removeHasKeyPkAtPosition(int i) {
    hasKeyPks.remove(i);
  }

  public void addAtPosition(int i, BidirectionalChildJDO bidir) {
    bidirChildren.add(i, (BidirectionalChildListJDO) bidir);
  }

  public void addAtPosition(int i, Flight f) {
    flights.add(i, f);
  }

  public void addAtPosition(int i, HasKeyPkJDO hasKeyPk) {
    hasKeyPks.add(i, hasKeyPk);
  }
}
