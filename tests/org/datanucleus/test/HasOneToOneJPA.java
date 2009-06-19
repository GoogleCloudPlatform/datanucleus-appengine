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

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToOneJPA {

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  @JoinColumn(name = "book_id")
  private Book book;

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  @JoinColumn(name = "haskeypk_id")
  private HasKeyPkJPA hasKeyPK;

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  @JoinColumn(name = "hasparent_id")
  private HasOneToOneParentJPA hasParent;

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  @JoinColumn(name = "hasparentkeypk_id")
  private HasOneToOneParentKeyPkJPA hasParentKeyPK;

  public String getId() {
    return id;
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

  public HasOneToOneParentJPA getHasParent() {
    return hasParent;
  }

  public void setHasParent(HasOneToOneParentJPA hasParent) {
    this.hasParent = hasParent;
  }

  public HasOneToOneParentKeyPkJPA getHasParentKeyPK() {
    return hasParentKeyPK;
  }

  public void setHasParentKeyPK(HasOneToOneParentKeyPkJPA hasParentKeyPK) {
    this.hasParentKeyPK = hasParentKeyPK;
  }
}
