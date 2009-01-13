// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.ArrayList;
import java.util.List;

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
public class HasOneToManyWithNonDeletingCascadeJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;

  @OneToMany(cascade = {CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH})
  private List<Book> books = new ArrayList<Book>();

  public String getId() {
    return id;
  }

  public List<Book> getBooks() {
    return books;
  }

  public void setBooks(List<Book> books) {
    this.books = books;
  }
}