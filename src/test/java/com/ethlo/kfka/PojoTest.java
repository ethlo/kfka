package com.ethlo.kfka;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

import com.acme.CustomKfkaMessage;


public class PojoTest
{
    @Test
    public void testHashcode()
    {
        new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build().hashCode();
    }
    
    @Test
    public void testEquals()
    {
        final CustomKfkaMessage a = new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build();
        final CustomKfkaMessage b = new CustomKfkaMessage.CustomKfkaMessageBuilder().id(12131L).topic("bar").type("foo").payload("payload").build();
        final CustomKfkaMessage c = new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build();
                        
        assertThat(a).isNotEqualTo(b);
        assertThat(a).isEqualTo(c);
    }
    
    @Test
    public void testToString()
    {
        new CustomKfkaMessage.CustomKfkaMessageBuilder().id(1213123L).topic("bar").type("foo").payload("payload").build().toString();
    }
}
