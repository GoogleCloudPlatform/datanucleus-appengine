package com.google.appengine.datanucleus.bugs.jdo;

import javax.jdo.listener.InstanceLifecycleEvent;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.jdo.listener.StoreLifecycleListener;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.test.Issue129Entity;

public class Issue129LifecycleListener implements InstanceLifecycleListener, StoreLifecycleListener {

  boolean postStoreHadIdSet = false;
  public void postStore(final InstanceLifecycleEvent event) {
    postStoreHadIdSet = (((Issue129Entity)event.getSource()).getId() != null);
      NucleusLogger.GENERAL.info(">> postStore entity: " + event.getSource(), new Exception());
  }

  public void preStore(InstanceLifecycleEvent event) {}

  public boolean idWasSetBeforePostStore() {
    return postStoreHadIdSet;
  }
}