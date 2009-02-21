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
public class HasOneToManyListLongPkJPA {

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private Long id;

  private String val;

  @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
  private List<BidirectionalChildListLongPkJPA> bidirChildren = new ArrayList<BidirectionalChildListLongPkJPA>();

  @OneToMany(cascade = CascadeType.ALL)
  private List<Book> books = new ArrayList<Book>();

  @OneToMany(cascade = CascadeType.ALL)
  private List<HasKeyPkJPA> hasKeyPks = new ArrayList<HasKeyPkJPA>();

  public Long getId() {
    return id;
  }

  public List<BidirectionalChildListLongPkJPA> getBidirChildren() {
    return (List) bidirChildren;
  }

  public void nullBidirChildren() {
    this.bidirChildren = null;
  }

  public List<Book> getBooks() {
    return books;
  }

  public void nullBooks() {
    this.books = null;
  }

  public List<HasKeyPkJPA> getHasKeyPks() {
    return hasKeyPks;
  }

  public void nullHasKeyPks() {
    this.hasKeyPks = null;
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }
}