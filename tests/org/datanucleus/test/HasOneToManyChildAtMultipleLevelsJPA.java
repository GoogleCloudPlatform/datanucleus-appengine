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
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToManyChildAtMultipleLevelsJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Key id;

  @OneToMany(cascade = CascadeType.ALL)
  private List<Book> books;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "child_id")
  private HasOneToManyChildAtMultipleLevelsJPA child;

  public Key getId() {
    return id;
  }

  public List<Book> getBooks() {
    return books;
  }

  public void setBooks(List<Book> books) {
    this.books = books;
  }

  public HasOneToManyChildAtMultipleLevelsJPA getChild() {
    return child;
  }

  public void setChild(HasOneToManyChildAtMultipleLevelsJPA child) {
    this.child = child;
  }
}