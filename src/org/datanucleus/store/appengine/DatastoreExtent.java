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
package org.datanucleus.store.appengine;

import org.datanucleus.FetchPlan;
import org.datanucleus.ObjectManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractExtent;
import org.datanucleus.store.query.Query;

import java.util.Collection;
import java.util.Iterator;

/**
 * Models a datastore extent - all entities of a certain kind.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreExtent extends AbstractExtent {

  /** FetchPlan for use with this Extent. */
  private final FetchPlan fetchPlan;

  /** Underlying query for getting the Extent. */
  private final Query query;


  /**
   * Construct the extent.
   *
   * @param om The ObjectManager.
   * @param cls The class of the objects in the extent.
   * @param subclasses True if the extent should include subclasses
   * @param cmd The meta data for the class.
   */
  public DatastoreExtent(
      ObjectManager om, Class cls, boolean subclasses, AbstractClassMetaData cmd) {
    super(om, cls, subclasses, cmd);
    // Can we actually support returning subclasses?  I guess we'd have to look at the
    // metadata to see what subclasses are maped and then just issue multiple queries.
    this.fetchPlan = om.getFetchPlan().getCopy();

    query = om.newQuery();
    query.setClass(cls);
    query.setSubclasses(subclasses);
  }

  public Iterator iterator() {
    return ((Collection)query.execute()).iterator();
  }

  public void closeAll() {
    query.closeAll();
  }

  public void close(Iterator iterator) {
    query.close(iterator);
  }

  public FetchPlan getFetchPlan() {
    return fetchPlan;
  }
}
