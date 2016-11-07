package com.ethlo.kfka;

import java.util.LinkedList;
import java.util.List;

public class CollectingListener implements KfkaMessageListener
{
    private final List<KfkaMessage> received = new LinkedList<>(); 
    
    @Override
    public void onMessage(KfkaMessage msg)
    {
        received.add(msg);
    }

    public List<KfkaMessage> getReceived()
    {
        return received;
    }
}
