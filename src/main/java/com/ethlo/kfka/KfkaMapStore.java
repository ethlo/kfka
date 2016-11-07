package com.ethlo.kfka;

import com.hazelcast.core.MapStore;

public interface KfkaMapStore<T extends KfkaMessage> extends MapStore<Long, T>
{

}
