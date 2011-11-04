package com.google.appengine.datanucleus.test;

import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue207Parent implements Issue207ParentInterface {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Key id;

  String name;

  @Persistent(mappedBy="parent") 
  @Extension(vendorName = "datanucleus", key = "implementation-classes", 
      value = "com.google.appengine.datanucleus.test.Issue207Child")
  private Set<Issue207ChildInterface> children = new HashSet<Issue207ChildInterface>();

  public Issue207Parent(String name) {
    this.name = name;
  }

  public Set<Issue207ChildInterface> getChildren() {
    return children;
  }

  public String getName() {
    return name;
  }

  public Key getId() {
    return id;
  }
}
