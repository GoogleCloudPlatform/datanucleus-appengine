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
package org.datanucleus.test;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION, detachable = "true")
public class BidirectionalChildListJDO implements BidirectionalChildJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
  private String id;

  @Persistent
  private HasOneToManyListJDO parent;

  @Persistent(mappedBy = "parent")
  @Element(dependent = "true")
  private List<BidirectionalGrandchildListJDO> bidirGrandChildren =
      new ArrayList<BidirectionalGrandchildListJDO>();

  private String childVal;

  public String getId() {
    return id;
  }

  public HasOneToManyListJDO getParent() {
    return parent;
  }

  public void setParent(HasOneToManyJDO parent) {
    this.parent = (HasOneToManyListJDO) parent;
  }

  public String getChildVal() {
    return childVal;
  }

  public void setChildVal(String childVal) {
    this.childVal = childVal;
  }

  public List<BidirectionalGrandchildListJDO> getBidirGrandChildren() {
    return bidirGrandChildren;
  }

  public void setBidirGrandChildren(List<BidirectionalGrandchildListJDO> bidirGrandChildren) {
    this.bidirGrandChildren = bidirGrandChildren;
  }
}
