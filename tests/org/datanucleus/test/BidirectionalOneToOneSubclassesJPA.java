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
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.OneToOne;

/**
 * Example 1:
 * A has X
 * B extends A has X
 * X has a back-pointer to A
 *
 * Example 2:
 * A
 * B extends A has Y extends X
 * Y has a back-pointer to B
 *
 * Example 3:
 * A has Y extends X
 * B extends A
 * X has a back-pointer to A
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public class BidirectionalOneToOneSubclassesJPA {

  public static class Example1 {
    @Entity
    @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
    public static class A {
      @Id
      @GeneratedValue(strategy= GenerationType.IDENTITY)
      private Long id;

      private String aString;

      @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
      private X child;

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

      public X getChild() {
        return child;
      }

      public void setChild(X child) {
        this.child = child;
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

      @OneToOne(fetch = FetchType.LAZY, mappedBy = "child")
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

      @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
      private Y child;

      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }

      public Y getChild() {
        return child;
      }

      public void setChild(Y child) {
        this.child = child;
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
      private String yString;

      @OneToOne(fetch = FetchType.LAZY, mappedBy = "child")
      private B parent;

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }

      public B getParent() {
        return parent;
      }

      public void setParent(B parent) {
        this.parent = parent;
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

      @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
      private Y child;

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

      public Y getChild() {
        return child;
      }

      public void setChild(Y child) {
        this.child = child;
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

      @OneToOne(fetch = FetchType.LAZY, mappedBy = "child")
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

      public A getParent() {
        return parent;
      }
    }

    @Entity
    public static class Y extends X {
      private String yString;

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }
    }
  }
}