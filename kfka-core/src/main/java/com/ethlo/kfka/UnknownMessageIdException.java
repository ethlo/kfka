package com.ethlo.kfka;

public class UnknownMessageIdException extends RuntimeException
{
    private final String messageId;

    public UnknownMessageIdException(final String messageId)
    {
        super("Unknown message id: " + messageId);
        this.messageId = messageId;
    }

    public String getMessageId()
    {
        return messageId;
    }
}