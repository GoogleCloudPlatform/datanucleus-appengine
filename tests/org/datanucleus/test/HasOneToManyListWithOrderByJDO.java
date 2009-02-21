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
public class HasOneToManyListWithOrderByJDO implements HasOneToManyWithOrderByJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Element(dependent = "true")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="origin DESC, dest ASC"))
  private List<Flight> flightsByOrigAndDest = new ArrayList<Flight>();

  @Element(dependent = "true")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id DESC, origin ASC"))
  private List<Flight> flightsByIdAndOrig = new ArrayList<Flight>();

  @Element(dependent = "true")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="origin DESC, id ASC"))
  private List<Flight> flightsByOrigAndId = new ArrayList<Flight>();

  public Long getId() {
    return id;
  }

  public List<Flight> getFlightsByOrigAndDest() {
    return flightsByOrigAndDest;
  }

  public List<Flight> getFlightsByIdAndOrig() {
    return flightsByIdAndOrig;
  }

  public List<Flight> getFlightsByOrigAndId() {
    return flightsByOrigAndId;
  }
}
