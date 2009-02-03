// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToManyWithNonDeletingCascadeSetJPA
    implements HasOneToManyWithNonDeletingCascadeJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;

  @OneToMany(cascade = {CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH})
  private Set<Book> books = new HashSet<Book>();

  public String getId() {
    return id;
  }

  public Set<Book> getBooks() {
    return books;
  }

  public void nullBooks() {
    this.books = null;
  }
}