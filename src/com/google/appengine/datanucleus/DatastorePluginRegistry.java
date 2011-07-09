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
package com.google.appengine.datanucleus;

import org.datanucleus.plugin.Bundle;
import org.datanucleus.plugin.ConfigurationElement;
import org.datanucleus.plugin.Extension;
import org.datanucleus.plugin.ExtensionPoint;
import org.datanucleus.plugin.PluginRegistry;

import com.google.appengine.datanucleus.jdo.DatastoreJDOMetaDataManager;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

/**
 * Custom {@link PluginRegistry} that delegates to another
 * {@link PluginRegistry} provided at time of construction for all operations.
 * 
 * Overrides the MetaDataManager for JDO to change the DFG. Note : this is basically wrong.
 * TODO Ditch this class
 *
 * @author Max Ross <maxr@google.com>
 */
final class DatastorePluginRegistry implements PluginRegistry {

  private final PluginRegistry delegate;

  DatastorePluginRegistry(PluginRegistry delegate) {
    this.delegate = delegate;
  }

  public ExtensionPoint getExtensionPoint(String id) {
    ExtensionPoint ep = delegate.getExtensionPoint(id);

    if (id.equals("org.datanucleus.metadata_manager")) {
      boolean replaced = false;
      for (Extension ext : ep.getExtensions()) {
        for (ConfigurationElement cfg : ext.getConfigurationElements()) {
          if (cfg.getAttribute("name").equals("JDO")) {
            // override with our own metadata manager
            // See DatastoreMetaDataManager for the reason why we do this.
            threadsafePutAttribute(cfg, "class", DatastoreJDOMetaDataManager.class.getName());
            replaced = true;
          }
        }
      }

      if (!replaced) {
        throw new RuntimeException("Unable to replace JPACallbackHandler.");
      }
    }
    return ep;
  }

  private void threadsafePutAttribute(ConfigurationElement cfg, String attrName, String val) {
    // we make a fixed set of changes and we're the only ones making them so
    // it's ok for this check to be outside of the synchronized block
    if (!val.equals(cfg.getAttribute(attrName))) {
      // These config elements are typically instantiated and initialized when
      // the pmf/emf is initialized so they were never designed to be threadsafe.
      // However, in order to inject our own config attributes we need to modify
      // them during pm/em creation.  To make this modification safe we synchronize
      // on the config element.  This should be sufficient since we're the only
      // one mutating them after system startup.
      synchronized (cfg) {
        cfg.putAttribute(attrName, val);
      }
    }
  }

  public ExtensionPoint[] getExtensionPoints() {
    return delegate.getExtensionPoints();
  }

  public void registerExtensionPoints() {
    delegate.registerExtensionPoints();
  }

  public void registerExtensions() {
    delegate.registerExtensions();
  }

  public Object createExecutableExtension(ConfigurationElement confElm, String name,
                                          Class[] argsClass, Object[] args)
      throws ClassNotFoundException, SecurityException, NoSuchMethodException,
             IllegalArgumentException, InstantiationException, IllegalAccessException,
             InvocationTargetException {
    return delegate.createExecutableExtension(confElm, name, argsClass, args);
  }

  public Class loadClass(String pluginId, String className) throws ClassNotFoundException {
    return delegate.loadClass(pluginId, className);
  }

  public URL resolveURLAsFileURL(URL url) throws IOException {
    return delegate.resolveURLAsFileURL(url);
  }

  public void resolveConstraints() {
    delegate.resolveConstraints();
  }

  public Bundle[] getBundles() {
    return delegate.getBundles();
  }
}
