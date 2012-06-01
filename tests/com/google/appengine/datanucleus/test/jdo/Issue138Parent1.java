package com.google.appengine.datanucleus.test.jdo;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue138Parent1 {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;

  String name;

  @Persistent
  Issue138Child child = null;

  public String getName() {
    return name;
  }

  public void setName(String str) {
    this.name = str;
  }

  public void setChild(Issue138Child child) {
    this.child = child;
  }

  public Issue138Child getChild() {
    return child;
  }

}
