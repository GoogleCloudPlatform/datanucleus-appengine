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
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildListJPA.BidirTopList;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildListJPA.BidirTopLongPkList;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildListJPA.BidirTopStringPkList;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildListJPA.BidirTopUnencodedStringPkList;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyKeyPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyLongPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyUnencodedStringPkJPA;
import com.google.appengine.datanucleus.test.SubclassesJPA.MappedSuperclassParent;
import com.google.appengine.datanucleus.test.SubclassesJPA.TablePerClass;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirTop;

import org.datanucleus.jpa.annotations.Extension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.OrderBy;

public class HasPolymorphicRelationsListJPA {
  @Entity
  public static class HasOneToManyListJPA implements HasOneToManyJPA {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    private String val;

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<BidirTopList> bidirChildren = new ArrayList<BidirTopList>();

    @OneToMany(cascade = CascadeType.ALL)
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();

    @OneToMany(cascade = CascadeType.ALL)
    private List<HasKeyPkJPA> hasKeyPks = new ArrayList<HasKeyPkJPA>();

    public String getId() {
      return id;
    }

    public List<BidirectionalSingleTableChildJPA.BidirTop> getBidirChildren() {
      return (List) bidirChildren;
    }

    public void nullBidirChildren() {
      this.bidirChildren = null;
    }

    public List<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void nullUnidirChildren() {
      this.unidirChildren = null;
    }

    public List<HasKeyPkJPA> getHasKeyPks() {
      return hasKeyPks;
    }

    public void nullHasKeyPks() {
      this.hasKeyPks = null;
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
  }
  
  @Entity
  public static class HasOneToManyLongPkListJPA implements HasOneToManyLongPkJPA {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    private String val;

    @OneToMany(cascade = CascadeType.ALL)
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();

    @OneToMany(cascade = CascadeType.ALL)
    private List<HasKeyPkJPA> hasKeyPks = new ArrayList<HasKeyPkJPA>();

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<BidirTopLongPkList> bidirChildren =
        new ArrayList<BidirTopLongPkList>();

    public Long getId() {
      return id;
    }

    public List<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void nullUnidirChildren() {
      this.unidirChildren = null;
    }

    public Collection<BidirectionalSingleTableChildJPA.BidirTopLongPk> getBidirChildren() {
      return (List) bidirChildren;
    }

    public List<HasKeyPkJPA> getHasKeyPks() {
      return hasKeyPks;
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
  }
  
  @Entity
  public static class HasOneToManyStringPkListJPA {

    @Id
    private String id;

    private String val;

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<BidirTopStringPkList> bidirChildren =
        new ArrayList<BidirTopStringPkList>();

    @OneToMany(cascade = CascadeType.ALL)
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();

    @OneToMany(cascade = CascadeType.ALL)
    private List<HasKeyPkJPA> hasKeyPks = new ArrayList<HasKeyPkJPA>();

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public List<BidirTopStringPkList> getBidirChildren() {
      return (List) bidirChildren;
    }

    public void nullBidirChildren() {
      this.bidirChildren = null;
    }

    public List<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void nullUnidirChildren() {
      this.unidirChildren = null;
    }

    public List<HasKeyPkJPA> getHasKeyPks() {
      return hasKeyPks;
    }

    public void nullHasKeyPks() {
      this.hasKeyPks = null;
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
    
  }

  @Entity
  public static class HasOneToManyKeyPkListJPA implements HasOneToManyKeyPkJPA {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Key id;

    @OneToMany(cascade = CascadeType.ALL)
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();

    public Key getId() {
      return id;
    }

    public List<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void nullUnidirChildren() {
      this.unidirChildren = null;
    }

  }
  
  @Entity
  public static class HasOneToManyUnencodedStringPkListJPA implements HasOneToManyUnencodedStringPkJPA {

    @Id
    private String id;

    @OneToMany(cascade = CascadeType.ALL)
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<BidirTopUnencodedStringPkList> bidirChildren =
        new ArrayList<BidirTopUnencodedStringPkList>();

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public List<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void nullUnidirChildren() {
      this.unidirChildren = null;
    }

    public Collection<BidirectionalSingleTableChildJPA.BidirTopUnencodedStringPk> getBidirChildren() {
      return (Collection) bidirChildren; 
    }
  }

  @Entity
  public static class HasOneToManyWithOrderByJPA {
  
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;
  
    private String val;
  
    @OneToMany(cascade = CascadeType.ALL)
    @OrderBy("name DESC, str ASC")
    private List<UnidirTop> unidirByNameAndStr = new ArrayList<UnidirTop>();
  
    @OneToMany(cascade = CascadeType.ALL)
    @OrderBy("id DESC, name ASC")
    private List<UnidirTop> unidirByIdAndName = new ArrayList<UnidirTop>();
  
    @OneToMany(cascade = CascadeType.ALL)
    @OrderBy("name DESC, id ASC")
    private List<UnidirTop> unidirByNameAndId = new ArrayList<UnidirTop>();
  
    public Long getId() {
      return id;
    }
  
    public List<UnidirTop> getUnidirByNameAndStr() {
      return unidirByNameAndStr;
    }
  
    public void setUnidirByNameAndStr(List<UnidirTop> unidirByNameAndStr) {
      this.unidirByNameAndStr = unidirByNameAndStr;
    }
  
    public List<UnidirTop> getUnidirByIdAndName() {
      return unidirByIdAndName;
    }
  
    public void setUnidirByIdAndName(List<UnidirTop> unidirByIdAndName) {
      this.unidirByIdAndName = unidirByIdAndName;
    }
  
    public List<UnidirTop> getUnidirByNameAndId() {
      return unidirByNameAndId;
    }
  
    public void setUnidirByNameAndId(List<UnidirTop> unidirByNameAndId) {
      this.unidirByNameAndId = unidirByNameAndId;
    }
  
    public String getVal() {
      return val;
    }
  
    public void setVal(String val) {
      this.val = val;
    }
  }

  @Entity
  public static class HasOneToManyChildAtMultipleLevelsJPA {
  
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Key id;
  
    @OneToMany(cascade = CascadeType.ALL)
    private List<UnidirTop> unidirChildren;
  
    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "child_id")
    private HasOneToManyChildAtMultipleLevelsJPA child;
  
    public Key getId() {
      return id;
    }
  
    public List<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }
  
    public void setUnidirChildren(List<UnidirTop> unidirChildren) {
      this.unidirChildren = unidirChildren;
    }
  
    public HasOneToManyChildAtMultipleLevelsJPA getChild() {
      return child;
    }
  
    public void setChild(HasOneToManyChildAtMultipleLevelsJPA child) {
      this.child = child;
    }
  }
  
  @Entity
  public static class HasOneToManyWithUnsupportedInheritanceList {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Key id;
    
    @OneToMany(cascade = CascadeType.ALL)
    private List<TablePerClass> children1 = new ArrayList<TablePerClass>();

    @OneToMany(cascade = CascadeType.ALL)
    private List<MappedSuperclassParent> children2 = new ArrayList<MappedSuperclassParent>();

    public Key getId() {
      return id;
    }

    public List<TablePerClass> getChildren1() {
      return children1;
    }

    public List<MappedSuperclassParent> getChildren2() {
      return children2;
    }
    
  }
}
