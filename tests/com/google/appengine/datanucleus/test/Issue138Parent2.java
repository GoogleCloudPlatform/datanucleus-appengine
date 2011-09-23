package com.google.appengine.datanucleus.test;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue138Parent2 {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;

  String name;

  @Persistent
  List<Issue138Child> children = new ArrayList<Issue138Child>();

  public String getName() {
    return name;
  }

  public void setName(String str) {
    this.name = str;
  }

  public List<Issue138Child> getChildren() {
    return children;
  }
}
