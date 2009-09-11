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
package org.datanucleus.test;

import org.datanucleus.store.appengine.Utils;

import java.util.ArrayList;
import java.util.Collection;
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
@PersistenceCapable(identityType = IdentityType.APPLICATION, detachable = "true")
public class HasOneToManyLongPkListJDO implements HasOneToManyLongPkJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long key;

  @Persistent
  private String val;

  @Element(dependent = "true")
  private List<Flight> flights = Utils.newArrayList();

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private List<BidirectionalChildLongPkListJDO> bidirChildren = new ArrayList<BidirectionalChildLongPkListJDO>();

  private List<HasKeyPkJDO> hasKeyPks = new ArrayList<HasKeyPkJDO>();

  public void addFlight(Flight flight) {
    flights.add(flight);
  }

  public Collection<Flight> getFlights() {
    return flights;
  }

  public void addBidirChild(BidirectionalChildLongPkJDO child) {
    bidirChildren.add((BidirectionalChildLongPkListJDO) child);
  }

  public Collection<BidirectionalChildLongPkJDO> getBidirChildren() {
    return (List) bidirChildren;
  }

  public Long getId() {
    return key;
  }

  public void addHasKeyPk(HasKeyPkJDO val) {
    hasKeyPks.add(val);
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }

  public void removeFlights(Collection<Flight> flights) {
    this.flights.removeAll(flights);
  }

  public void removeBidirChildren(Collection<BidirectionalChildLongPkJDO> bidirChildren) {
    this.bidirChildren.removeAll(bidirChildren);
  }
}