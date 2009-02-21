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
import javax.persistence.OrderBy;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToManyWithOrderByJPA {

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private Long id;

  private String val;

  @OneToMany(cascade = CascadeType.ALL)
  @OrderBy("author DESC, title ASC")
  private List<Book> booksByAuthorAndTitle = new ArrayList<Book>();

  @OneToMany(cascade = CascadeType.ALL)
  @OrderBy("id DESC, author ASC")
  private List<Book> booksByIdAndAuthor = new ArrayList<Book>();

  @OneToMany(cascade = CascadeType.ALL)
  @OrderBy("author DESC, id ASC")
  private List<Book> booksByAuthorAndId = new ArrayList<Book>();

  public Long getId() {
    return id;
  }

  public List<Book> getBooksByAuthorAndTitle() {
    return booksByAuthorAndTitle;
  }

  public void setBooksByAuthorAndTitle(List<Book> booksByAuthorAndTitle) {
    this.booksByAuthorAndTitle = booksByAuthorAndTitle;
  }

  public List<Book> getBooksByIdAndAuthor() {
    return booksByIdAndAuthor;
  }

  public void setBooksByIdAndAuthor(List<Book> booksByIdAndAuthor) {
    this.booksByIdAndAuthor = booksByIdAndAuthor;
  }

  public List<Book> getBooksByAuthorAndId() {
    return booksByAuthorAndId;
  }

  public void setBooksByAuthorAndId(List<Book> booksByAuthorAndId) {
    this.booksByAuthorAndId = booksByAuthorAndId;
  }

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }

}