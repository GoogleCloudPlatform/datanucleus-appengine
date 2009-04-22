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

  public List<HasExplicitIndexColumnJDO> getHasIndexColumn() {
    throw new UnsupportedOperationException();
  }
}