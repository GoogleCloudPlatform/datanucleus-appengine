package com.google.appengine.datanucleus.test.jdo;

import java.util.*;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.annotations.Unowned;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class Issue290Parent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Key key;

  @Persistent
  private String name;

  @Persistent
  @Unowned
  private List<Issue290Child> children = new ArrayList();

  public Issue290Parent(String name) {
    super();
    this.name = name;
  }

  public Key getKey() {
    return key;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Issue290Child> getChildren() {
    return children;
  }
}