package com.google.appengine.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue73Parent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;

  String name;

  @Persistent
  Issue73Child child1 = null;

  @Persistent
  Issue73Child child2 = null;

  public String getName() {
    return name;
  }

  public void setName(String str) {
    this.name = str;
  }

  public void setChild1(Issue73Child child) {
    this.child1 = child;
  }

  public void setChild2(Issue73Child child) {
    this.child2 = child;
  }

  public Issue73Child getChild1() {
    return child1;
  }

  public Issue73Child getChild2() {
    return child2;
  }
}
