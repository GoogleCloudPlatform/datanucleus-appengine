// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.store.AbstractExtent;
import org.datanucleus.store.query.Query;
import org.datanucleus.FetchPlan;
import org.datanucleus.ObjectManager;
import org.datanucleus.metadata.AbstractClassMetaData;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreExtent extends AbstractExtent {

  /** FetchPlan for use with this Extent. */
  private FetchPlan fetchPlan = null;

  /** Underlying query for getting the Extent. */
  private Query query;


  public DatastoreExtent(ObjectManager om, Class cls, boolean subclasses, AbstractClassMetaData cmd) {
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
