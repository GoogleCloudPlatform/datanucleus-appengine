// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.IdentityType;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.DATASTORE)
public class Flight {
  @PrimaryKey
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

  public String toString() {
    return "\n\nid: " + id + "\norigin: " + origin + "\ndest: " + dest
        + "\nname: " + name + "\nyou: " + you + "\nme: " + me;
  }
}
