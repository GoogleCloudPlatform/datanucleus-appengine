package com.google.appengine.datanucleus.bugs.test;

import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue228Parent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;

  String name;

  @Persistent(mappedBy="parent")
  Set<Issue228Child> children = new HashSet<Issue228Child>();

  public String getName() {
    return name;
  }

  public void setName(String str) {
    this.name = str;
  }

  public Set<Issue228Child> getChildren() {
    return children;
  }
}
