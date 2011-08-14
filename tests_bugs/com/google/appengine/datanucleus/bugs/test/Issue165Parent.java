package com.google.appengine.datanucleus.bugs.test;

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
  
  @Persistent(mappedBy = "parent")
  Issue165Child child;
  
  public void setChild(Issue165Child c) { 
    child = c; 
    child.parent = this;
  }

  public String getId() {
    return id;
  }

  public void setAString(String str) { aString = str;}

  public Issue165Child getChild() {return child;}
}
