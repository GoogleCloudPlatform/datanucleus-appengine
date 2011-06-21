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

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.NullValue;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class HasNotNullConstraintsJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;
  
  @Persistent(nullValue = NullValue.EXCEPTION)
  private Boolean bool;

  @Persistent(nullValue = NullValue.EXCEPTION)
  private Character c;

  @Persistent(nullValue = NullValue.EXCEPTION)
  private Byte b;

  @Persistent(nullValue = NullValue.EXCEPTION)
  private Short s;

  @Persistent(nullValue = NullValue.EXCEPTION)
  private Integer i;
  
  @Persistent(nullValue = NullValue.EXCEPTION)
  private Long l;

  @Persistent(nullValue = NullValue.EXCEPTION)
  private Float f;

  @Persistent(nullValue = NullValue.EXCEPTION)
  private Double d;
  
  @Persistent(nullValue = NullValue.EXCEPTION)
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
