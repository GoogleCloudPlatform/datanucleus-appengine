package com.google.appengine.datanucleus.test;

import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue90Parent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;

  String name;

  @Persistent
  List<Issue90Child> children;

  public String getName() {
    return name;
  }

  public void setName(String str) {
    this.name = str;
  }

  public void setChildren(List<Issue90Child> children) {
    this.children = children;
  }

  public List<Issue90Child> getChildren() {
    return children;
  }
}
