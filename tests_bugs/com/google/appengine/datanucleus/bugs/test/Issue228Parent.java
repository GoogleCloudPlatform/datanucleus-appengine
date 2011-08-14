package com.google.appengine.datanucleus.bugs.test;

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
  
  @Persistent
  String aString;
  
  @Persistent(mappedBy = "parent")
  Issue228Child child;
  
  public void setChild(Issue228Child c) { 
    child = c; 
    child.parent = this;
  }

  public void setAString(String str) { aString = str;}

  public Issue228Child getChild() {return child;}
}
