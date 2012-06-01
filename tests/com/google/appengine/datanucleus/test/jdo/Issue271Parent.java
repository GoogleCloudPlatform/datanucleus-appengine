package com.google.appengine.datanucleus.test.jdo;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable(detachable="true")
public class Issue271Parent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Key key;

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private List<Issue271Child> children = new ArrayList<Issue271Child>();

  public Key getKey() {
    return key;
  }

  public List<Issue271Child> getChildren() {
    return children;
  }
}
