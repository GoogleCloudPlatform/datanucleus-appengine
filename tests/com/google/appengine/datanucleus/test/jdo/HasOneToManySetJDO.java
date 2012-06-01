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
package com.google.appengine.datanucleus.test.jdo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;


/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(detachable = "true")
public class HasOneToManySetJDO implements HasOneToManyJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
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
    if (flights == null) {
      flights = new HashSet<Flight>();
    }
    return flights;
  }

  public void addFlight(Flight flight) {
    this.flights.add(flight);
  }

  public Set<HasKeyPkJDO> getHasKeyPks() {
    if (hasKeyPks == null) {
      hasKeyPks = new HashSet<HasKeyPkJDO>();
      }
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

  public void removeFlights(Collection<Flight> flights) {
    this.flights.removeAll(flights);
  }

  public void removeBidirChildren(Collection<BidirectionalChildJDO> bidirChildren) {
    this.bidirChildren.removeAll(bidirChildren);
  }

  public void setBidirChildren(Collection<BidirectionalChildJDO> childList) {
    this.bidirChildren = (Set) childList;
  }
}