// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.ArrayList;
import java.util.Arrays;
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
public class HasOneToManyArrayWithOrderByJDO implements HasOneToManyWithOrderByJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent(dependentElement = "true")
  @Element(dependent = "true")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="origin DESC, dest ASC"))
  private Flight[] flightsByOrigAndDest;

  @Persistent(dependentElement = "true")
  @Element(dependent = "true")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id DESC, origin ASC"))
  private Flight[] flightsByIdAndOrig;

  @Persistent(dependentElement = "true")
  @Element(dependent = "true")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="origin DESC, id ASC"))
  private Flight[] flightsByOrigAndId;

  public Long getId() {
    return id;
  }

  private List asList(Object[] arr) {
    if (arr == null) {
      return new ArrayList<Object>();
    }
    return Arrays.asList(arr);
  }

  public List<Flight> getFlightsByOrigAndDest() {
    return asList(flightsByOrigAndDest);
  }

  public List<Flight> getFlightsByIdAndOrig() {
    return asList(flightsByIdAndOrig);
  }

  public List<Flight> getFlightsByOrigAndId() {
    return asList(flightsByOrigAndId);
  }
}