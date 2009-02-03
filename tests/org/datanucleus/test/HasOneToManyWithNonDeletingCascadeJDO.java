// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Order;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasOneToManyWithNonDeletingCascadeJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  // Need to have a custom ordering, otherwise we'll have an order mapping that
  // we'll try to update if we remove an element fromt the list and we'll get an
  // exceptoin because these flights are not in the same entity group as their owner.
  @Element(dependent = "false")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id"))
  private List<Flight> flights = new ArrayList<Flight>();

  public String getId() {
    return id;
  }

  public List<Flight> getFlights() {
    return flights;
  }

  public void setFlights(List<Flight> flights) {
    this.flights = flights;
  }
}
