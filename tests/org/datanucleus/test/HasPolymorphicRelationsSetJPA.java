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

import com.google.appengine.api.datastore.Key;

import org.datanucleus.jpa.annotations.Extension;
import org.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTop;
import org.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopLongPk;
import org.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopUnencodedStringPk;
import org.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirTopLongPkSet;
import org.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirTopSet;
import org.datanucleus.test.BidirectionalSingleTableChildSetJPA.BidirTopUnencodedStringPkSet;
import org.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyJPA;
import org.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyKeyPkJPA;
import org.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyLongPkJPA;
import org.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyUnencodedStringPkJPA;
import org.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirTop;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;

public class HasPolymorphicRelationsSetJPA {
  @Entity
  public static class HasOneToManySetJPA implements HasOneToManyJPA {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    private String val;

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private Set<BidirTopSet> bidirChildren = new HashSet<BidirTopSet>();

    @OneToMany(cascade = CascadeType.ALL)
    private Set<UnidirTop> unidirChildren = new HashSet<UnidirTop>();

    @OneToMany(cascade = CascadeType.ALL)
    private Set<HasKeyPkJPA> hasKeyPks = new HashSet<HasKeyPkJPA>();

    public String getId() {
      return id;
    }

    public Set<BidirTop> getBidirChildren() {
      return (Set) bidirChildren;
    }

    public void nullBidirChildren() {
      this.bidirChildren = null;
    }

    public Set<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void nullUnidirChildren() {
      this.unidirChildren = null;
    }

    public Set<HasKeyPkJPA> getHasKeyPks() {
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
  public static class HasOneToManyKeyPkSetJPA implements HasOneToManyKeyPkJPA {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Key id;

    @OneToMany(cascade = CascadeType.ALL)
    private Set<UnidirTop> unidirChildren = new HashSet<UnidirTop>();

    public Key getId() {
      return id;
    }

    public Set<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void nullBooks() {
      this.unidirChildren = null;
    }

  }
  
  @Entity
  public static class HasOneToManyLongPkSetJPA implements HasOneToManyLongPkJPA {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @OneToMany(cascade = CascadeType.ALL)
    private Set<UnidirTop> unidirChildren = new HashSet<UnidirTop>();

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private Set<BidirTopLongPkSet> bidirChildren =
        new HashSet<BidirTopLongPkSet>();

    public Long getId() {
      return id;
    }

    public Set<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void unidirChildren() {
      this.unidirChildren = null;
    }

    public Collection<BidirTopLongPk> getBidirChildren() {
      return (Set) bidirChildren;
    }
  }
  
  @Entity
  public static class HasOneToManyUnencodedStringPkSetJPA implements HasOneToManyUnencodedStringPkJPA {

    @Id
    private String id;

    @OneToMany(cascade = CascadeType.ALL)
    private Set<UnidirTop> books = new HashSet<UnidirTop>();

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private Set<BidirTopUnencodedStringPkSet> bidirChildren =
        new HashSet<BidirTopUnencodedStringPkSet>();

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public Set<UnidirTop> getUnidirChildren() {
      return books;
    }

    public void nullBooks() {
      this.books = null;
    }

    public Collection<BidirTopUnencodedStringPk> getBidirChildren() {
      return (Set) bidirChildren;
    }
  }
}
