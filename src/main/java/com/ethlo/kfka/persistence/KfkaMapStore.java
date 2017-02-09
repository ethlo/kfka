package com.ethlo.kfka.persistence;

import com.ethlo.kfka.KfkaMessage;
import com.hazelcast.core.MapStore;

public interface KfkaMapStore<T extends KfkaMessage> extends MapStore<Long, T>
{

}