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
 package org.datanucleus.test;

import com.google.appengine.api.datastore.Text;

import org.datanucleus.jpa.annotations.Extension;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;

public class SingleTableInheritanceJPA {

  @Entity
  @DiscriminatorValue(value = "P")
  @Inheritance(strategy = InheritanceType.SINGLE_TABLE)
  public static class Parent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String parentStr;

    public Long getId() {
      return id;
    }

    public String getParentStr() {
      return parentStr;
    }

    public void setParentStr(String parentStr) {
      this.parentStr = parentStr;
    }
  }
  
  @Embeddable
  public static class Embedded1 {
    private String str;
    
    public Embedded1(String str) {
      this.str = str;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }    
  }
  
  @Entity
  public static class Child11Many {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;
    
    private String str;

    private Child11 child11;
    
    public Child11Many(String str) {
      this.str = str;
    }
    
    public String getId() {
      return id;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    public Child11 getChild11() {
      return child11;
    }
    
  }

  @Entity
  @DiscriminatorValue(value = "C11")
  public static class Child11 extends Parent {
    private Integer child11Integer;
    
    @OneToMany(mappedBy = "child11", cascade = CascadeType.ALL)
    private List<Child11Many> child11Manys = new ArrayList<Child11Many>();

    public Integer getChild11Integer() {
      return child11Integer;
    }

    public void setChild11Integer(Integer child11Integer) {
      this.child11Integer = child11Integer;
    }

    public List<Child11Many> getChild11Manys() {
      return child11Manys;
    }

  }

  @Entity
  @DiscriminatorValue(value = "C12")
  public static class Child12 extends Child11 {
    private int child12Int;
    
    private float value;
    
    @Embedded
    @AttributeOverrides({@AttributeOverride(name="str", column=@Column(name="string"))})
    private Embedded1 embedded1;
    
    public int getChild12Int() {
      return child12Int;
    }

    public void setChild12Int(int child12Int) {
      this.child12Int = child12Int;
    }
    
    public float getValue() {
      return value;
    }

    public void setValue(float value) {
      this.value = value;
    }

    public Embedded1 getEmbedded1() {
      return embedded1;
    }

    public void setEmbedded1(Embedded1 embedded1) {
      this.embedded1 = embedded1;
    }

  }

  @Embeddable
  public static class Embedded2 {
    private Text str;
    private Double dbl;
    
    public Embedded2(String str, Double dbl) {
      setStr(str);
      this.dbl = dbl;
    }

    public String getStr() {
      return str != null ? str.getValue() : null;
    }

    public void setStr(String str) {
      this.str = str != null ? new Text(str) : null;
    }

    public Double getDbl() {
      return dbl;
    }

    public void setDbl(Double dbl) {
      this.dbl = dbl;
    }    
  }

  @Entity
  @DiscriminatorValue(value = "C21")
  public static class Child21 extends Parent {
    private long child21Long;
    
    @Embedded
    private Embedded2 embedded2;
    
    public long getChild21Long() {
      return child21Long;
    }

    public void setChild21Long(long child21Long) {
      this.child21Long = child21Long;
    }

    public Embedded2 getEmbedded2() {
      return embedded2;
    }

    public void setEmbedded2(Embedded2 embedded2) {
      this.embedded2 = embedded2;
    }
    
  }

  @Entity
  @DiscriminatorValue(value = "C22")
  public static class Child22 extends Child21 {
    private String child22Str;
    
    private Boolean value;

    public String getChild22Str() {
      return child22Str;
    }

    public void setChild22Str(String child22Str) {
      this.child22Str = child22Str;
    }

    public Boolean getValue() {
      return value;
    }

    public void setValue(Boolean value) {
      this.value = value;
    }
    
  }
  
  @Entity
  @DiscriminatorValue(value = "1")
  @DiscriminatorColumn(name = "type", discriminatorType = DiscriminatorType.INTEGER)
  @NamedQueries(value = {
      @NamedQuery(name = "AllParentIntDiscriminatorObjects", 
	  	  query = "select p from org.datanucleus.test.SingleTableInheritanceJPA$ParentIntDiscriminator p order by parentStr")
  })
  public static class ParentIntDiscriminator {    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName = "datanucleus", key="gae.encoded-pk", value="true")
    private String id;
    private String parentStr;

    public String getId() {
      return id;
    }

    public String getParentStr() {
      return parentStr;
    }

    public void setParentStr(String parentStr) {
      this.parentStr = parentStr;
    }
  }
  
  @Entity
  @DiscriminatorValue(value = "2")
  public static class ChildDateIntDiscriminator extends ParentIntDiscriminator {
    @Column(name = "date", nullable = false)
    private Date value;

    public Date getValue() {
      return value;
    }

    public void setValue(Date value) {
      this.value = value;
    }
  }

  @Entity
  @DiscriminatorValue(value = "3")
  public static class ChildBoolIntDiscriminator extends ParentIntDiscriminator {
    @Column(name = "bool")
    private boolean value;

    public boolean isValue() {
      return value;
    }

    public void setValue(boolean value) {
      this.value = value;
    }
  }
  
  @Entity
  public static class ParentWithoutDiscriminator {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    public Long getId() {
      return id;
    }
  }
  
  @Entity
  public static class ChildToParentWithoutDiscriminator extends ParentWithoutDiscriminator {
  }
}
