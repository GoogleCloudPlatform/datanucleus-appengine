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

  @Order(mappedBy="index")
  private List<HasExplicitIndexColumnJDO> hasIndexColumn = new ArrayList<HasExplicitIndexColumnJDO>();

  @Element(dependent = "true")
  @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="key ASC"))
  List<HasPkWithOverriddenColumnJDO> hasOverridenPk = new ArrayList<HasPkWithOverriddenColumnJDO>();

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

  public List<HasExplicitIndexColumnJDO> getHasIndexColumn() {
    return hasIndexColumn;
  }

  public List<HasPkWithOverriddenColumnJDO> getHasPkWithOverridenColumns() {
    return hasOverridenPk;
  }
}
