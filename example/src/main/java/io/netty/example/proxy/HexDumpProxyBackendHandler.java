/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.example.proxy;

import io.netty.channel.*;

public class HexDumpProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private final HexDumpProxyFrontendHandler frontendHandler;
    private final Channel inboundChannel;
    private ChannelHandlerContext ctx;

    public HexDumpProxyBackendHandler(HexDumpProxyFrontendHandler frontendHandler, Channel inboundChannel) {
        this.frontendHandler = frontendHandler;
        this.inboundChannel = inboundChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        if (!inboundChannel.isActive()) {
            HexDumpProxyFrontendHandler.closeOnFlush(ctx.channel());
        }
        resume();
        frontendHandler.resume();
    }

    void pause() {
        System.out.println("Backend paused");
        ctx.channel().config().setAutoRead(false);
    }

    void resume() {
        System.out.println("Backend resumed");
        ctx.channel().config().setAutoRead(true);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        inboundChannel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (!future.isSuccess()) {
                    future.channel().close();
                }
            }
        });
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            frontendHandler.resume();
        } else {
            frontendHandler.pause();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        HexDumpProxyFrontendHandler.closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        HexDumpProxyFrontendHandler.closeOnFlush(ctx.channel());
    }
}
