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
package com.google.appengine.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToOneParentKeyPkJPA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private Key id;

  @OneToOne(mappedBy = "hasParentKeyPK", fetch = FetchType.LAZY)
  private HasOneToOneJPA parent;

  private String str;

  public Key getId() {
    return id;
  }

  public HasOneToOneJPA getParent() {
    return parent;
  }

  public void setParent(HasOneToOneJPA parent) {
    this.parent = parent;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }
}
