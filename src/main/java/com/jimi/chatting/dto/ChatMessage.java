package com.jimi.chatting.dto;

public record ChatMessage (
    String roomId,
    String sender,
    String content,
    long timestamp
) {}
