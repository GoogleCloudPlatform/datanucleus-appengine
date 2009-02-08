// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.Collection;

/**
 * @author Max Ross <maxr@google.com>
 */
public interface HasOneToManyJDO {
  Collection<BidirectionalChildJDO> getBidirChildren();
  void addBidirChild(BidirectionalChildJDO child);
  void nullBidirChildren();
  void clearBidirChildren();
  Collection<Flight> getFlights();
  void addFlight(Flight flight);
  void nullFlights();
  void clearFlights();
  Collection<HasKeyPkJDO> getHasKeyPks();
  void addHasKeyPk(HasKeyPkJDO hasKeyPk);
  void nullHasKeyPks();
  void clearHasKeyPks();
  String getVal();
  void setVal(String val);
  String getId();
  void addBidirChildAtPosition(BidirectionalChildJDO bidir, int pos);
  void addFlightAtPosition(Flight f, int pos);
  void addHasKeyPkAtPosition(HasKeyPkJDO hasKeyPk, int pos);
  void removeBidirChildAtPosition(int i);
  void removeFlightAtPosition(int i);
  void removeHasKeyPkAtPosition(int i);
  void removeFlights(Collection<Flight> flights);
  void removeBidirChildren(Collection<BidirectionalChildJDO> bidirChildren);

  void addAtPosition(int i, BidirectionalChildJDO bidir);
  void addAtPosition(int i, Flight f);
  void addAtPosition(int i, HasKeyPkJDO hasKeyPk);
}
