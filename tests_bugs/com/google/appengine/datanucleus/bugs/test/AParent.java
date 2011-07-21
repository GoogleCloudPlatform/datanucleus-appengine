package com.google.appengine.datanucleus.bugs.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class AParent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;
  
  @Persistent
  String aString;
  
  @Persistent(mappedBy = "parent")
  AChild child;
  
  public void setChild(AChild c) { child = c; child.parent = this; }

  public void setAString(String str) { aString = str;}

  public AChild getChild() {return child;}
}
