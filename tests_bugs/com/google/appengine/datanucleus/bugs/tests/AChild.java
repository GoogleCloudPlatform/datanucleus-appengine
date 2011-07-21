package com.google.appengine.datanucleus.bugs.tests;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class AChild {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;

  @Persistent
  AParent parent;
  
  @Persistent
  String aString;

  public void setAString(String str) { aString = str;}

  public AParent getParent() {return parent;}
}
