package com.google.appengine.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable(detachable="true")
public class Issue271Child {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Key key;

  @Persistent
  private Issue271Parent parent;

  public Key getKey() {
    return key;
  }

  public Issue271Parent getParent() {
    return parent;
  }

  public void setParent(Issue271Parent parent) {
    this.parent = parent;
  }
}
