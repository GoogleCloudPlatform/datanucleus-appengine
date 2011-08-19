/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;

/**
 * Example 1 (A-X):
 * A has List<X>
 * B extends A
 * X has back-pointer to A
 * Y extends X
 *
 * Example 2 (B-Y):
 * A
 * B extends A has List<Y extends X>
 * X
 * Y extends X, has a back-pointer to A
 *
 * Example 3 (B-X):
 * A
 * B extends A has List<X>
 * X has back-pointer to B
 * Y extends X
 *
 * Example 4 (A-Y):
 * A has List<Y extends X>
 * B extends A
 * X has a back-pointer to A
 * Y extends X
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public class BidirectionalOneToManySubclassesJPA {

  public static class Example1 {
    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class A {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Long id;

      private String aString;

      @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
      @OrderBy(value = "xString desc")
      private List<X> children = new ArrayList<X>();

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
        this.id = id;
      }

      public String getAString() {
        return aString;
      }

      public void setAString(String aString) {
        this.aString = aString;
      }

      public List<X> getChildren() {
        return children;
      }

      public void setChildren(List<X> children) {
        this.children = children;
      }
    }

    @Entity
    public static class B extends A {
      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }
    }

    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class X {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Key id;

      @SuppressWarnings("unused")
      @ManyToOne(fetch = FetchType.LAZY)
      private A parent;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @Entity
    public static class Y extends X {
    }
  }

  public static class Example2 {
    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class A {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Long id;

      private String aString;

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
        this.id = id;
      }

      public String getAString() {
        return aString;
      }

      public void setAString(String aString) {
        this.aString = aString;
      }
    }

    @Entity
    public static class B extends A {
      @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
      @OrderBy(value = "xString desc")
      private List<Y> children = new ArrayList<Y>();

      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }

      public List<Y> getChildren() {
        return children;
      }

      public void setChildren(List<Y> children) {
        this.children = children;
      }
    }

    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class X {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Key id;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @Entity
    public static class Y extends X {
      @ManyToOne(fetch = FetchType.LAZY)
      private B parent;

      private String yString;

      public B getParent() {
        return parent;
      }

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }
    }
  }

  public static class Example3 {
    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class A {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Long id;

      private String aString;

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
        this.id = id;
      }

      public String getAString() {
        return aString;
      }

      public void setAString(String aString) {
        this.aString = aString;
      }
    }

    @Entity
    public static class B extends A {
      // TODO This ought to be generic of X not Y!
      @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
      @OrderBy(value = "xString desc")
      private List<Y> children = new ArrayList<Y>();

      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }

      public List<Y> getChildren() {
        return children;
      }

      public void setChildren(List<Y> children) {
        this.children = children;
      }
    }

    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class X {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Key id;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @Entity
    public static class Y extends X{
      // TODO This ought to be on X not Y!
      @ManyToOne(fetch = FetchType.LAZY)
      private B parent;

      public B getParent() {
        return parent;
      }

      public void setParent(B parent) {
        this.parent = parent;
      }

      private String yString;

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }
    }
  }

  public static class Example4 {
    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class A {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Long id;

      private String aString;

      @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
      @OrderBy(value = "xString desc")
      private List<Y> children = new ArrayList<Y>();

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
        this.id = id;
      }

      public String getAString() {
        return aString;
      }

      public void setAString(String aString) {
        this.aString = aString;
      }

      public List<Y> getChildren() {
        return children;
      }

      public void setChildren(List<Y> children) {
        this.children = children;
      }
    }

    @Entity
    public static class B extends A {
      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }
    }

    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class X {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Key id;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @Entity
    public static class Y extends X {
      @ManyToOne(fetch = FetchType.LAZY)
      private A parent;

      private String yString;

      public A getParent() {
        return parent;
      }

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }
    }
  }
}