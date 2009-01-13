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
public class HasOneToManyJPA {

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private String id;

  private String val;

  @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
  private List<BidirectionalChildJPA> bidirChildren = new ArrayList<BidirectionalChildJPA>();

  @OneToMany(cascade = CascadeType.ALL)
  private List<Book> books = new ArrayList<Book>();

  @OneToMany(cascade = CascadeType.ALL)
  private List<HasKeyPkJPA> hasKeyPks = new ArrayList<HasKeyPkJPA>();

  public String getId() {
    return id;
  }

  public List<BidirectionalChildJPA> getBidirChildren() {
    return bidirChildren;
  }

  public void setBidirChildren(List<BidirectionalChildJPA> bidirChildren) {
    this.bidirChildren = bidirChildren;
  }

  public List<Book> getBooks() {
    return books;
  }

  public void setBooks(List<Book> books) {
    this.books = books;
  }

  public List<HasKeyPkJPA> getHasKeyPks() {
    return hasKeyPks;
  }

  public void setHasKeyPks(List<HasKeyPkJPA> hasKeyPks) {
    this.hasKeyPks = hasKeyPks;
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }
}