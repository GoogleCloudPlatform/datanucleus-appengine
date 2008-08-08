// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.apphosting.api.datastore.Entity;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.IdGeneratorStrategy;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class Flight {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
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

  public String getId() {
    return id;
  }

  public String toString() {
    return "\n\nid: " + id + "\norigin: " + origin + "\ndest: " + dest
        + "\nname: " + name + "\nyou: " + you + "\nme: " + me;
  }

  public static Entity newFlightEntity(String name, String origin, String dest,
      int you, int me) {
    Entity e = new Entity(Flight.class.getName());
    e.setProperty("name", name);
    e.setProperty("origin", origin);
    e.setProperty("dest", dest);
    e.setProperty("you", you);
    e.setProperty("me", me);
    return e;
  }

}
