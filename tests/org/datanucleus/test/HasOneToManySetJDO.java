// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.HashSet;
import java.util.Set;

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
public class HasOneToManySetJDO implements HasOneToManyJDO {

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
  private Set<BidirectionalChildSetJDO> bidirChildren = new HashSet<BidirectionalChildSetJDO>();

  @Element(dependent = "true")
  private Set<Flight> flights = new HashSet<Flight>();

  @Element(dependent = "true")
  private Set<HasKeyPkJDO> hasKeyPks = new HashSet<HasKeyPkJDO>();

  public Set<BidirectionalChildJDO> getBidirChildren() {
    return new HashSet<BidirectionalChildJDO>(bidirChildren);
  }

  public void addBidirChild(BidirectionalChildJDO bidirChild) {
    bidirChildren.add((BidirectionalChildSetJDO) bidirChild);
  }

  public Set<Flight> getFlights() {
    return new HashSet<Flight>(flights);
  }

  public void addFlight(Flight flight) {
    this.flights.add(flight);
  }

  public Set<HasKeyPkJDO> getHasKeyPks() {
    return new HashSet<HasKeyPkJDO>(hasKeyPks);
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
    throw new UnsupportedOperationException();
  }

  public void addFlightAtPosition(Flight f, int pos) {
    throw new UnsupportedOperationException();
  }

  public void addHasKeyPkAtPosition(HasKeyPkJDO hasKeyPk, int pos) {
    throw new UnsupportedOperationException();
  }

  public void removeBidirChildAtPosition(int i) {
    throw new UnsupportedOperationException();
  }

  public void removeFlightAtPosition(int i) {
    throw new UnsupportedOperationException();
  }

  public void removeHasKeyPkAtPosition(int i) {
    throw new UnsupportedOperationException();
  }

  public void addAtPosition(int i, BidirectionalChildJDO bidir) {
    throw new UnsupportedOperationException();
  }

  public void addAtPosition(int i, Flight f) {
    throw new UnsupportedOperationException();
  }

  public void addAtPosition(int i, HasKeyPkJDO hasKeyPk) {
    throw new UnsupportedOperationException();
  }
}