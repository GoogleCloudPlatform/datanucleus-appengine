// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToOneStringPkJPA {

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private String id;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "book_id")
  private Book book;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "haskeypk_id")
  private HasKeyPkJPA hasKeyPK;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "hasparent_id")
  private HasOneToOneStringPkParentJPA hasParent;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "hasparentkeypk_id")
  private HasOneToOneStringPkParentKeyPkJPA hasParentKeyPK;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Book getBook() {
    return book;
  }

  public void setBook(Book book) {
    this.book = book;
  }

  public HasKeyPkJPA getHasKeyPK() {
    return hasKeyPK;
  }

  public void setHasKeyPK(HasKeyPkJPA hasKeyPK) {
    this.hasKeyPK = hasKeyPK;
  }

  public HasOneToOneStringPkParentJPA getHasParent() {
    return hasParent;
  }

  public void setHasParent(HasOneToOneStringPkParentJPA hasParent) {
    this.hasParent = hasParent;
  }

  public HasOneToOneStringPkParentKeyPkJPA getHasParentKeyPK() {
    return hasParentKeyPK;
  }

  public void setHasParentKeyPK(HasOneToOneStringPkParentKeyPkJPA hasParentKeyPK) {
    this.hasParentKeyPK = hasParentKeyPK;
  }
}