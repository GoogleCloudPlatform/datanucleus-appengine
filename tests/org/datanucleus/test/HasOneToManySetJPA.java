// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.jpa.annotations.Extension;

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
public class HasOneToManySetJPA implements HasOneToManyJPA {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  private String val;

  @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
  private Set<BidirectionalChildSetJPA> bidirChildren = new HashSet<BidirectionalChildSetJPA>();

  @OneToMany(cascade = CascadeType.ALL)
  private Set<Book> books = new HashSet<Book>();

  @OneToMany(cascade = CascadeType.ALL)
  private Set<HasKeyPkJPA> hasKeyPks = new HashSet<HasKeyPkJPA>();

  public String getId() {
    return id;
  }

  public Set<BidirectionalChildJPA> getBidirChildren() {
    return (Set) bidirChildren;
  }

  public void nullBidirChildren() {
    this.bidirChildren = null;
  }

  public Set<Book> getBooks() {
    return books;
  }

  public void nullBooks() {
    this.books = null;
  }

  public Set<HasKeyPkJPA> getHasKeyPks() {
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