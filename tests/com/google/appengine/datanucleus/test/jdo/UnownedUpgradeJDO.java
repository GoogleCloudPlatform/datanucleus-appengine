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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.annotations.Unowned;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * Model classes used to validate that we can upgrade a PC class with Keys to
 * an upgrade class with an unowned relationship.
 */
public class UnownedUpgradeJDO {

  @PersistenceCapable
  public static class SideB {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    private String name;

    public Long getId() {
      return id;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  @PersistenceCapable(table = "HasOneToMany")
  public static class HasOneToManyWithKey {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    Long id;

    @Persistent
    List<Key> others;

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

    public void addOther(Key other) {
      if (this.others == null) {
        this.others = new ArrayList<Key>();
      }
      this.others.add(other);
    }

    public List<Key> getOthers() {
      return others;
    }
  }

  @PersistenceCapable(table = "HasOneToMany")
  public static class HasOneToManyWithUnowned {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    Long id;

    @Persistent
    @Unowned
    List<SideB> others;

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

    public void addOther(SideB other) {
      if (this.others == null) {
        this.others = new ArrayList<SideB>();
      }
      this.others.add(other);
    }

    public List<SideB> getOthers() {
      return others;
    }
  }

  @PersistenceCapable(table = "HasOneToOne")
  public static class HasOneToOneWithKey {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    Long id;

    @Persistent
    Key other;

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

    public void setOther(Key other) {
      this.other = other;
    }

    public Key getOther() {
      return other;
    }
  }

  @PersistenceCapable(table = "HasOneToOne")
  public static class HasOneToOneWithUnowned {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    Long id;

    @Persistent(column = "other")
    @Unowned
    SideB other;

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

    public void setOther(SideB other) {
      this.other = other;
    }

    public SideB getOther() {
      return other;
    }
  }
}
