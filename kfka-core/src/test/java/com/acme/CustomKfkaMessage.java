package com.acme;

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

import java.util.Collection;
import java.util.Collections;

import com.ethlo.kfka.KfkaMessage;

public class CustomKfkaMessage extends KfkaMessage
{
    private Integer userId;

    private CustomKfkaMessage()
    {
        super(null);
    }

    public CustomKfkaMessage(CustomKfkaMessageBuilder builder)
    {
        super(builder);
        this.userId = builder.userId;
    }

    public Integer getUserId()
    {
        return userId;
    }

    public CustomKfkaMessage setUserId(Integer userId)
    {
        this.userId = userId;
        return this;
    }

    @Override
    public Collection<String> getQueryableProperties()
    {
        return Collections.singletonList("userId");
    }

    public static class CustomKfkaMessageBuilder extends Builder
    {
        private int userId;

        @Override
        public CustomKfkaMessage build()
        {
            return new CustomKfkaMessage(this);
        }

        public CustomKfkaMessageBuilder userId(int userId)
        {
            this.userId = userId;
            return this;
        }
    }
}
