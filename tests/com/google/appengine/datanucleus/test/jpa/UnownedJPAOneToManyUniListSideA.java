/**********************************************************************
Copyright (c) 2012 Google Inc.

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

import com.google.appengine.datanucleus.annotations.Unowned;

/**
 * "owner" of a 1-N unidirectional relation in JPA, using unowned (List) relations.
 */
@Entity
public class UnownedJPAOneToManyUniListSideA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  long id;

  String name;

  @Unowned
  @OneToMany(cascade={CascadeType.PERSIST, CascadeType.MERGE})
  List<UnownedJPAOneToManyUniSideB> bs = new ArrayList<UnownedJPAOneToManyUniSideB>();

  public Long getId() {
    return id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void addB(UnownedJPAOneToManyUniSideB other) {
    this.bs.add(other);
  }

  public List<UnownedJPAOneToManyUniSideB> getBs() {
    return bs;
  }
}
