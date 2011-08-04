package com.google.appengine.datanucleus.bugs.test;

import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class Issue125Entity {
   @PrimaryKey
   @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
   private Key key;

   @Persistent
   private List<Issue125Entity> children;

   public Key getKey() {
       return key;
   }

   public void setKey(Key key) {
       this.key = key;
   }

   public List<Issue125Entity> getChildren() {
       return children;
   }

   public void setChildren(List<Issue125Entity> children) {
       this.children = children;
   }
}