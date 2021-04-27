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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Objects;

import com.ethlo.kfka.util.Hex;
import com.ethlo.kfka.util.RandomUtil;

public abstract class KfkaMessage implements Serializable, Comparable<KfkaMessage>
{
    public static final int MESSAGE_ID_LENGTH = 12;
    private String topic;
    private long timestamp;
    private byte[] payload;
    private String type;
    private Long id;
    private String messageId;

    protected KfkaMessage(Builder builder)
    {
        if (builder == null)
        {
            return;
        }

        Assert.notNull(builder.topic, "topic may not be null");
        Assert.notNull(builder.type, "type may not be null");
        Assert.notNull(builder.timestamp, "timestamp may not be null");
        Assert.notNull(builder.payload, "payload may not be null");

        this.topic = builder.topic;
        this.timestamp = builder.timestamp;
        this.payload = builder.payload;
        this.type = builder.type;
        this.id = builder.id;
        this.messageId = builder.messageId;
    }

    public String getTopic()
    {
        return topic;
    }

    public Long getTimestamp()
    {
        return timestamp;
    }

    public byte[] getPayload()
    {
        return payload;
    }

    public String getType()
    {
        return type;
    }

    @Override
    public int compareTo(KfkaMessage o)
    {
        return Long.compare(o.id, this.id);
    }

    @Override
    public String toString()
    {
        return "KfkaMessage [id=" + id
                + ", messageId=" + messageId
                + ", topic=" + topic
                + ", timestamp=" + timestamp
                + ", payload=" + Hex.bytesToHex(payload)
                + ", type=" + type;
    }

    public Long getId()
    {
        return this.id;
    }

    public void setId(long newId)
    {
        this.id = newId;
    }

    public abstract Collection<String> getQueryableProperties();

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof KfkaMessage)
        {
            return Objects.equals(id, ((KfkaMessage) obj).id);
        }
        return false;
    }

    protected void timestamp(Long timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getMessageId()
    {
        return messageId;
    }

    public void setMessageId(final String messageId)
    {
        this.messageId = messageId;
    }

    public abstract static class Builder
    {
        private String topic;
        private Long timestamp = System.currentTimeMillis();
        private byte[] payload;
        private String type;
        private Long id;
        private String messageId;

        public Builder topic(String topic)
        {
            this.topic = topic;
            return this;
        }

        public Builder timestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        public Builder payload(String message)
        {
            this.payload = message.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        public Builder payload(byte[] payload)
        {
            this.payload = payload;
            return this;
        }

        public Builder type(String type)
        {
            this.type = type;
            return this;
        }

        public Builder id(Long id)
        {
            this.id = id;
            return this;
        }

        public Builder messageId(final String messageId)
        {
            this.messageId = messageId;
            return this;
        }

        public abstract <T extends KfkaMessage> T build();
    }
}
