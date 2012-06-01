/**********************************************************************
Copyright (c) 2012 Google Inc.

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

import java.util.HashMap;
import java.util.Map;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;


/**
 * Sample class with Map fields.
 */
@PersistenceCapable(detachable = "true")
public class HasOneToManyMapJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
  private String id;

  @Persistent
  private String val;

  @Persistent
  private Map<String, Flight> flightsByName = new HashMap<String, Flight>();

  @Persistent
  private Map<Integer, String> basicMap = new HashMap<Integer, String>();

  public String getId() {
    return id;
  }

  public Map<String, Flight> getFlightsByName() {
    return flightsByName;
  }

  public void addFlight(String name, Flight flight) {
    flightsByName.put(name, flight);
  }

  public Map<Integer, String> getBasicMap() {
    return basicMap;
  }

  public void addBasicEntry(Integer val, String name) {
    basicMap.put(val, name);
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }
}
