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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class HasNotNullConstraintsJPA {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;
  
  @Column(nullable = false)
  private Boolean bool;

  @Column(nullable = false)
  private Character c;

  @Column(nullable = false)
  private Byte b;

  @Column(nullable = false)
  private Short s;

  @Column(nullable = false)
  private Integer i;
  
  @Column(nullable = false)
  private Long l;

  @Column(nullable = false)
  private Float f;

  @Column(nullable = false)
  private Double d;
  
  @Column(nullable = false)
  private String str;
  
  public Boolean getBool() {
    return bool;
  }

  public void setBool(Boolean bool) {
    this.bool = bool;
  }

  public Character getC() {
    return c;
  }

  public void setC(Character c) {
    this.c = c;
  }

  public Byte getB() {
    return b;
  }

  public void setB(Byte b) {
    this.b = b;
  }

  public Short getS() {
    return s;
  }

  public void setS(Short s) {
    this.s = s;
  }

  public Integer getI() {
    return i;
  }

  public void setI(Integer i) {
    this.i = i;
  }

  public Long getL() {
    return l;
  }

  public void setL(Long l) {
    this.l = l;
  }

  public Float getF() {
    return f;
  }

  public void setF(Float f) {
    this.f = f;
  }

  public Double getD() {
    return d;
  }

  public void setD(Double d) {
    this.d = d;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }

}
