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

import com.google.appengine.api.datastore.Key;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasUnencodedStringPkOneToManyBidirChildrenJPA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
  private List<ChildA> childAList = new ArrayList<ChildA>();

  public List<ChildA> getChildAList() {
    return childAList;
  }

  public void setChildAList(List<ChildA> childAList) {
    this.childAList = childAList;
  }

  @Entity
  public static class ChildA {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Key key;

    @ManyToOne(fetch = FetchType.LAZY)
    private HasUnencodedStringPkOneToManyBidirChildrenJPA parent;

    public HasUnencodedStringPkOneToManyBidirChildrenJPA getParent() {
      return parent;
    }
  }
}