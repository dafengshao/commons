// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space
// Source File Name: ZKStartListener.java

package com.km.commons.config.api;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;

public interface ZKStartListener {

  public abstract void executor(CuratorFramework curatorframework, ConnectionState newState);
}
