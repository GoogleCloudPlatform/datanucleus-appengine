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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
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
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasOneToManyArrayJDO implements HasOneToManyJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  private String val;

  @Persistent(mappedBy = "parent", dependentElement = "true")
  @Element(dependent = "true")
  private BidirectionalChildArrayJDO[] bidirChildren = null;

  @Persistent(dependentElement = "true")
  @Element(dependent = "true")
  private Flight[] flights = null;

  @Persistent(dependentElement = "true")
  @Element(dependent = "true")
  private HasKeyPkJDO[] hasKeyPks = null;

  public String getId() {
    return id;
  }

  private List asList(Object[] arr) {
    if (arr == null) {
      return new ArrayList<Object>();
    }
    return Arrays.asList(arr);
  }

  public List<BidirectionalChildJDO> getBidirChildren() {
    return asList(bidirChildren);
  }

  public void addBidirChild(BidirectionalChildJDO bidirChild) {
    List<BidirectionalChildJDO> list = getBidirChildren();
    list.add(bidirChild);
    bidirChildren = list.toArray(new BidirectionalChildArrayJDO[0]);
  }

  public List<Flight> getFlights() {
    return asList(flights);
  }

  public void addFlight(Flight flight) {
    List<Flight> list = getFlights();
    list.add(flight);
    flights = list.toArray(new Flight[0]);
  }

  public List<HasKeyPkJDO> getHasKeyPks() {
    return asList(hasKeyPks);
  }

  public void addHasKeyPk(HasKeyPkJDO hasKeyPk) {
    List<HasKeyPkJDO> list = getHasKeyPks();
    list.add(hasKeyPk);
    hasKeyPks = list.toArray(new HasKeyPkJDO[0]);
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
    if (bidirChildren != null) {
      for (int i = 0; i < bidirChildren.length; i++) {
        bidirChildren[i] = null;
      }
    }
  }

  public void clearFlights() {
    if (flights != null) {
      for (int i = 0; i < flights.length; i++) {
        flights[i] = null;
      }
    }
  }

  public void clearHasKeyPks() {
    if (hasKeyPks != null) {
      for (int i = 0; i < hasKeyPks.length; i++) {
        hasKeyPks[i] = null;
      }
    }
  }

  private Object[] copyAndSetAtPos(Object[] arr, Object obj, int pos) {
    // datanuc can't detect changes to array elements so we need
    // to replace the whole array
    Object[] copy = (Object[]) Array.newInstance(obj.getClass(), arr.length);
    System.arraycopy(arr, 0, copy, 0, arr.length);
    copy[pos] = obj;
    return copy;
  }

  public void addBidirChildAtPosition(BidirectionalChildJDO bidir, int pos) {
    bidirChildren = (BidirectionalChildArrayJDO[]) copyAndSetAtPos(bidirChildren, bidir, pos);
  }

  public void addFlightAtPosition(Flight f, int pos) {
    flights = (Flight[]) copyAndSetAtPos(flights, f, pos);
  }

  public void addHasKeyPkAtPosition(HasKeyPkJDO hasKeyPk, int pos) {
    hasKeyPks = (HasKeyPkJDO[]) copyAndSetAtPos(hasKeyPks, hasKeyPk, pos);
  }

  public void removeBidirChildAtPosition(int i) {
    bidirChildren = (BidirectionalChildArrayJDO[]) copyAndSetAtPos(bidirChildren, null, i);
  }

  public void removeFlightAtPosition(int i) {
    flights = (Flight[]) copyAndSetAtPos(flights, null, i);
  }

  public void removeHasKeyPkAtPosition(int i) {
    hasKeyPks = (HasKeyPkJDO[]) copyAndSetAtPos(hasKeyPks, null, i);
  }

  private Object[] addAtPosition(int i, Object[] arr, Object newElement) {
    List<Object> list =
        new ArrayList<Object>(Arrays.asList(arr));
    list.add(i, newElement);
    return list.toArray((Object[]) Array.newInstance(newElement.getClass(), list.size()));

  }

  public void addAtPosition(int i, BidirectionalChildJDO bidir) {
    bidirChildren = (BidirectionalChildArrayJDO[]) addAtPosition(i, bidirChildren, bidir);
  }

  public void addAtPosition(int i, Flight f) {
    flights = (Flight[]) addAtPosition(i, flights, f);
  }

  public void addAtPosition(int i, HasKeyPkJDO hasKeyPk) {
    hasKeyPks = (HasKeyPkJDO[]) addAtPosition(i, hasKeyPks, hasKeyPk);
  }

  public void removeFlights(Collection<Flight> flights) {
    throw new UnsupportedOperationException();
  }

  public void removeBidirChildren(Collection<BidirectionalChildJDO> bidirChildren) {
    throw new UnsupportedOperationException();
  }

}