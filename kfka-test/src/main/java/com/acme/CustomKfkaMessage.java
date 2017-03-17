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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import com.ethlo.kfka.KfkaMessage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class CustomKfkaMessage extends KfkaMessage
{
    private static final long serialVersionUID = 443035395722534117L;
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
    
    public static class CustomKfkaMessageBuilder extends KfkaMessage.Builder
    {
        private int userId;

        @SuppressWarnings("unchecked")
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
    
    public CustomKfkaMessage setUserId(Integer userId)
    {
        this.userId = userId;
        return this;
    }

    public Integer getUserId()
    {
        return userId;
    }

    @Override
    public Collection<String> getQueryableProperties()
    {
        return Arrays.asList("userId");
    }

    @Override
    protected void doWriteData(ObjectDataOutput out) throws IOException
    {
        out.writeInt(userId != null ? userId : 0);
    }

    @Override
    public void doReadData(ObjectDataInput in) throws IOException
    {
        final int i = in.readInt();
        this.userId = i > 0 ? i : null;  
    }
}
