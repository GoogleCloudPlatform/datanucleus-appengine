/**********************************************************************
Copyright (c) 2011 Google Inc.

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
package com.google.appengine.datanucleus.test.jdo;

import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.datanucleus.annotations.Unowned;

/**
 * "Owner" of a 1-N unidirectional relation in JDO, using unowned relations.
 */
@PersistenceCapable(detachable="true")
public class UnownedJDOOneToManyUniSideA {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Long id;

  @Persistent
  @Unowned
  Set<UnownedJDOOneToManyUniSideB> others = new HashSet<UnownedJDOOneToManyUniSideB>();

  String name;

  public Long getId() {
    return id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void addOther(UnownedJDOOneToManyUniSideB other) {
    this.others.add(other);
  }

  public Set<UnownedJDOOneToManyUniSideB> getOthers() {
    return others;
  }
}
