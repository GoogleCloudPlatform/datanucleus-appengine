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

import org.datanucleus.identity.IdentityKeyTranslator;
import org.datanucleus.ExecutionContext;

/**
 * Translator of the "key" passed in to pm.getObjectById(cls, key), or em.find(cls, key) into
 * a valid JDO/JPA key.
 */
public class DatastoreIdentityKeyTranslator implements IdentityKeyTranslator
{
    /* (non-Javadoc)
     * @see org.datanucleus.identity.IdentityKeyTranslator#getKey(org.datanucleus.store.ExecutionContext, java.lang.Class, java.lang.Object)
     */
    public Object getKey(ExecutionContext ec, Class cls, Object key)
    {
        return EntityUtils.idToInternalKey(ec, cls, key, false);
    }
}