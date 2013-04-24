package com.google.appengine.datanucleus.test.jpa;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class HasEmbeddableCollection {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private Long id;

  @Embedded
  @ElementCollection
  Set<EmbeddableJPA> theSet = new HashSet();

  public Long getId() {
    return id;
  }

  public Set<EmbeddableJPA> getTheSet() {
    return theSet;
  }
}