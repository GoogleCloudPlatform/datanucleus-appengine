/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package com.google.appengine.datanucleus.test.jpa;

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