package com.google.appengine.datanucleus.bugs.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue228Owner {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;
  
  @Persistent
  String aString;
  
  @Persistent(mappedBy = "parent")
  Issue228Related child;
  
  public void setChild(Issue228Related c) { 
    child = c; 
    child.parent = this;
  }

  public void setAString(String str) { aString = str;}

  public Issue228Related getChild() {return child;}
}
