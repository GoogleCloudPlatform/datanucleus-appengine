package com.google.appengine.datanucleus.test.jdo;

import java.util.TreeSet;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class Issue165Parent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
  private String id;
  
  @Persistent
  String aString;
  
  @Persistent
  Issue165Child child;

  @Persistent(mappedBy="parent")
  TreeSet<Issue165Child2> children2 = new TreeSet<Issue165Child2>();

  public void setChild(Issue165Child c) { 
    child = c; 
    child.parent = this;
  }

  public void addChild2(Issue165Child2 child) {
    children2.add(child);
  }

  public String getId() {
    return id;
  }

  public void setAString(String str) { aString = str;}

  public Issue165Child getChild() {return child;}

  public TreeSet<Issue165Child2> getChildren2() {return children2;}
}
