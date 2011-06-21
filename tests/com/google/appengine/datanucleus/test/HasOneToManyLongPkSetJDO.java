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
package com.google.appengine.datanucleus.test;

import com.google.appengine.datanucleus.Utils;

import java.util.Collection;
import java.util.Set;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(detachable = "true")
public class HasOneToManyLongPkSetJDO implements HasOneToManyLongPkJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long key;

  @Element(dependent = "true")
  private Set<Flight> flights = Utils.newHashSet();

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private Set<BidirectionalChildLongPkSetJDO> bidirChildren = Utils.newHashSet();

  public void addFlight(Flight flight) {
    flights.add(flight);
  }

  public Collection<Flight> getFlights() {
    return flights;
  }

  public Long getId() {
    return key;
  }

  public void addBidirChild(BidirectionalChildLongPkJDO child) {
    bidirChildren.add((BidirectionalChildLongPkSetJDO) child);
  }

  public Collection<BidirectionalChildLongPkJDO> getBidirChildren() {
    return (Set) bidirChildren;
  }

  public void removeFlights(Collection<Flight> flights) {
    this.flights.removeAll(flights);
  }

  public void removeBidirChildren(Collection<BidirectionalChildLongPkJDO> bidirChildren) {
    this.bidirChildren.removeAll(bidirChildren);
  }
}