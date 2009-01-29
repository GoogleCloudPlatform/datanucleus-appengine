// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Element;
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
public class HasOneToManyWithOrderByJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  private String val;

  @Element(dependent = "true")
  @Order(column="author")
  private List<Flight> flightsByAuthorAndTitle = new ArrayList<Flight>();

  @Element(dependent = "true")
  private List<Flight> flightsByIdAndAuthor = new ArrayList<Flight>();

  @Element(dependent = "true")
  private List<Flight> flightsByAuthorAndId = new ArrayList<Flight>();

  public String getId() {
    return id;
  }

  public List<Flight> getFlightsByAuthorAndTitle() {
    return flightsByAuthorAndTitle;
  }

  public void setFlightsByAuthorAndTitle(List<Flight> flightsByAuthorAndTitle) {
    this.flightsByAuthorAndTitle = flightsByAuthorAndTitle;
  }

  public List<Flight> getFlightsByIdAndAuthor() {
    return flightsByIdAndAuthor;
  }

  public void setFlightsByIdAndAuthor(List<Flight> flightsByIdAndAuthor) {
    this.flightsByIdAndAuthor = flightsByIdAndAuthor;
  }

  public List<Flight> getFlightsByAuthorAndId() {
    return flightsByAuthorAndId;
  }

  public void setFlightsByAuthorAndId(List<Flight> flightsByAuthorAndId) {
    this.flightsByAuthorAndId = flightsByAuthorAndId;
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }

}