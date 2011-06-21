/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * **********************************************************************/

package com.google.appengine.datanucleus.test;

import java.util.List;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.Embedded;
import javax.jdo.annotations.EmbeddedOnly;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasEmbeddedJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent
  @Embedded(members={
              @Persistent(name="id", columns=@Column(name="flightId"))
              })
  private Flight flight;

  @Persistent
  @Embedded(members={
              @Persistent(name="id", columns=@Column(name="ID")),
              @Persistent(name="origin", columns=@Column(name="ORIGIN")),
              @Persistent(name="dest", columns=@Column(name="DEST")),
              @Persistent(name="name", columns=@Column(name="NAME")),
              @Persistent(name="you", columns=@Column(name="YOU")),
              @Persistent(name="me", columns=@Column(name="ME")),
              @Persistent(name="flightNumber", columns=@Column(name="FLIGHTNUMBER"))
              })
  private Flight anotherFlight;

  @Persistent
  @Embedded
  private Embedded1 embedded1;

  public Long getId() {
    return id;
  }

  public Flight getFlight() {
    return flight;
  }

  public void setFlight(Flight flight) {
    this.flight = flight;
  }

  public Embedded1 getEmbedded1() {
    return embedded1;
  }

  public void setEmbedded1(Embedded1 embedded1) {
    this.embedded1 = embedded1;
  }

  public Flight getAnotherFlight() {
    return anotherFlight;
  }

  public void setAnotherFlight(Flight anotherFlight) {
    this.anotherFlight = anotherFlight;
  }

  @EmbeddedOnly
  @PersistenceCapable
  public static class Embedded1 {
    private String val1;

    @Persistent
    private List<String> multiVal1;
    
    @Persistent
    @Embedded
    private Embedded2 embedded2;

    public String getVal1() {
      return val1;
    }

    public void setVal1(String val1) {
      this.val1 = val1;
    }

    public Embedded2 getEmbedded2() {
      return embedded2;
    }

    public void setEmbedded2(Embedded2 embedded2) {
      this.embedded2 = embedded2;
    }

    public List<String> getMultiVal1() {
      return multiVal1;
    }

    public void setMultiVal1(List<String> multiVal1) {
      this.multiVal1 = multiVal1;
    }
  }

  @EmbeddedOnly
  @PersistenceCapable
  public static class Embedded2 {
    private String val2;
    private List<String> multiVal2;

    public String getVal2() {
      return val2;
    }

    public void setVal2(String val2) {
      this.val2 = val2;
    }

    public List<String> getMultiVal2() {
      return multiVal2;
    }

    public void setMultiVal2(List<String> multiVal2) {
      this.multiVal2 = multiVal2;
    }
  }
}
