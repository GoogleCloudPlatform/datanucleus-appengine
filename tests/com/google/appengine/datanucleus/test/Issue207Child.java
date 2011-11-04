package com.google.appengine.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue207Child implements Issue207ChildInterface {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Key id;

  String name;

  @Persistent(types=Issue207Parent.class)
  Issue207ParentInterface parent;

  public Issue207Child(String name) {
    this.name = name;
  }

  public void setParent(Issue207ParentInterface owner) {
    this.parent = owner;
  }

  public Issue207ParentInterface getParent() {
    return parent;
  }

  public String getName() {
    return name;
  }

  public Key getId() {
    return id;
  }
}
