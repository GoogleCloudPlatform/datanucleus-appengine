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
package com.google.appengine.datanucleus.test.jdo;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.Utils;


import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(detachable = "true")
public class HasMultipleBidirChildrenJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private List<BidirChild1> child1 = Utils.newArrayList();

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private List<BidirChild2> child2 = Utils.newArrayList();

  public Long getId() {
    return id;
  }

  public List<BidirChild1> getChild1() {
    return child1;
  }

  public void setChild1(List<BidirChild1> child1) {
    this.child1 = child1;
  }

  public List<BidirChild2> getChild2() {
    return child2;
  }

  public void setChild2(List<BidirChild2> child2) {
    this.child2 = child2;
  }

  @PersistenceCapable(detachable = "true")
  public static class BidirChild1 {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    private HasMultipleBidirChildrenJDO parent;

    public Key getId() {
      return id;
    }

    public void setId(Key id) {
      this.id = id;
    }
  }

  @PersistenceCapable(detachable = "true")
  public static class BidirChild2 {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Persistent
    private HasMultipleBidirChildrenJDO parent;

    public Key getId() {
      return id;
    }

    public void setId(Key id) {
      this.id = id;
    }
  }
}