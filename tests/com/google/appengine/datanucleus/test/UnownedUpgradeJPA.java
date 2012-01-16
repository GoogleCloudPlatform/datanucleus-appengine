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
package com.google.appengine.datanucleus.test;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.annotations.Unowned;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 * Model classes used to validate that we can upgrade a PC class with Keys to
 * an upgrade class with an unowned relationship.
 */
public class UnownedUpgradeJPA {

  @Entity
  public static class SideB {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
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

  @Entity
  @Table(name = "HasOneToMany")
  public static class HasOneToManyWithKey {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    Long id;

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

  @Entity
  @Table(name = "HasOneToMany")
  public static class HasOneToManyWithUnowned {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    Long id;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
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

  @Entity
  @Table(name = "HasOneToOne")
  public static class HasOneToOneWithKey {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    Long id;

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

  @Entity
  @Table(name = "HasOneToOne")
  public static class HasOneToOneWithUnowned {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    Long id;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Column(name = "other")
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
