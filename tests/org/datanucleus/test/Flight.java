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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

import java.io.Serializable;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Version;
import javax.jdo.annotations.VersionStrategy;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION, detachable = "true")
@Version(strategy = VersionStrategy.VERSION_NUMBER)
public class Flight implements Serializable {
  private String id;
  @Persistent
  private String origin;
  @Persistent
  private String dest;
  @Persistent
  private String name;
  @Persistent
  int you;
  @Persistent
  int me;
  @Persistent
  @Column(name="flight_number")
  int flightNumber;

  public Flight() {
  }

  public Flight(String origin, String dest, String name, int you, int me) {
    this.origin = origin;
    this.dest = dest;
    this.name = name;
    this.you = you;
    this.me = me;
  }

  public String getOrigin() {
    return origin;
  }

  public void setOrigin(String origin) {
    this.origin = origin;
  }

  public String getDest() {
    return dest;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getYou() {
    return you;
  }

  public void setYou(int you) {
    this.you = you;
  }

  public int getMe() {
    return me;
  }

  public void setMe(int me) {
    this.me = me;
  }

  @PrimaryKey
  @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  public String getId() {
    return id;
  }

  /**
   * You really shouldn't call this unless you're looking for trouble.
   * Useful for tests that verify the trouble.
   */
  public void setId(String id) {
    this.id = id;
  }

  public int getFlightNumber() {
    return flightNumber;
  }

  public void setFlightNumber(int flightNumber) {
    this.flightNumber = flightNumber;
  }

  @Override
  public String toString() {
    return "\n\nid: " + id + "\norigin: " + origin + "\ndest: " + dest
        + "\nname: " + name + "\nyou: " + you + "\nme: " + me;
  }

  public static Entity newFlightEntity(
      String keyName, String name, String origin, String dest, int you, int me) {
    return newFlightEntity(keyName, name, origin, dest, you, me, 300);
  }

  public static void addData(Entity e, String name, String origin, String dest, int you, int me, int flightNumber) {
    e.setProperty("name", name);
    e.setProperty("origin", origin);
    e.setProperty("dest", dest);
    e.setProperty("you", you);
    e.setProperty("me", me);
    e.setProperty("OPT_VERSION", 1L);
    e.setProperty("flight_number", flightNumber);
  }

  public static Entity newFlightEntity(Key parent, String keyName, String name, String origin,
                                       String dest, int you, int me, int flightNumber) {
    Entity e;
    if (keyName == null) {
      if (parent == null) {
        e = new Entity(Flight.class.getSimpleName());
      } else {
        e = new Entity(Flight.class.getSimpleName(), parent);
      }
    } else {
      if (parent == null) {
        e = new Entity(Flight.class.getSimpleName(), keyName);
      } else {
        e = new Entity(Flight.class.getSimpleName(), keyName, parent);
      }
    }
    addData(e, name, origin, dest, you, me, flightNumber);
    return e;
  }

  public static Entity newFlightEntity(
      String keyName, String name, String origin, String dest, int you, int me, int flightNumber) {
    return newFlightEntity(null, keyName, name, origin, dest, you, me, flightNumber);
  }

  public static Entity newFlightEntity(
      String name, String origin, String dest, int you, int me, int flightNumber) {
    return newFlightEntity(null, name, origin, dest, you, me, flightNumber);
  }

  public static Entity newFlightEntity(String name, String origin, String dest,
      int you, int me) {
    return newFlightEntity(null, name, origin, dest, you, me);
  }

  /**
   * We get weird test failures if we give Flight an equals() method due to
   * attempts to read fields of deleted objects.
   * TODO(maxr) Straighten this out. 
   */
  public boolean customEquals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Flight flight = (Flight) o;

    if (flightNumber != flight.flightNumber) {
      return false;
    }
    if (me != flight.me) {
      return false;
    }
    if (you != flight.you) {
      return false;
    }
    if (dest != null ? !dest.equals(flight.dest) : flight.dest != null) {
      return false;
    }
    if (id != null ? !id.equals(flight.id) : flight.id != null) {
      return false;
    }
    if (name != null ? !name.equals(flight.name) : flight.name != null) {
      return false;
    }
    if (origin != null ? !origin.equals(flight.origin) : flight.origin != null) {
      return false;
    }

    return true;
  }
}
