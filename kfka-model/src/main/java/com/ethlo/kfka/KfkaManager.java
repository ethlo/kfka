package com.ethlo.kfka;

/*-
 * #%L
 * kfka
 * %%
 * Copyright (C) 2017 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

public interface KfkaManager
{
    long add(KfkaMessage msg);

    void cleanExpired();
    
    void clearAll();

    long findfirst();

    long findLatest();
    
    long size();

    void addListener(KfkaMessageListener l);

    KfkaMessageListener addListener(KfkaMessageListener l, KfkaPredicate kfkaPredicate);

    void clearCache();

    long loadAll();

    void removeListener(KfkaMessageListener listener);

    void delete(long messageId);
}
