/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.ouc.tcp.client.TCP_Receiver_ADT;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

public class TCP_Receiver extends TCP_Receiver_ADT {

    private TCP_PACKET ackPack;	//回复的ACK报文段
    private int expectedSeq = 1;    // 期望收到的的序号

    // SR: 接收窗口缓存失序的包
    private int recvWindowN = 5;    // 接收窗口大小
    private ConcurrentHashMap<Integer, int[]> recvBuffer = new ConcurrentHashMap<>();   // 缓存失序包

    /*构造函数*/
    public TCP_Receiver() {
        super();	//调用超类构造函数，创建底层 Client、初始化 dataQueue，并把 recvData.txt 清空
        super.initTCP_Receiver(this);	//启动 ListenPacket 监听线程，初始化TCP接收端
    }

    @Override
    //接收到数据报：检查校验和，设置回复的ACK报文段
    public void rdt_recv(TCP_PACKET recvPack) {
        int recvSeq = recvPack.getTcpH().getTh_seq();

        //检查校验码，生成ACK
        if(CheckSum.computeChkSum(recvPack) == recvPack.getTcpH().getTh_sum())
        {   // 校验通过
            // SR：检查是否在接收窗口内 [expectedSeq, expectedSeq + recvWindowSize*100）
            if (recvSeq >=  expectedSeq && recvSeq < expectedSeq + recvWindowN * 100)
            {
                if (recvSeq == expectedSeq)
                {   // 收到期望包
                    System.out.println("[SR] Receive expected packet, seq: " + recvSeq);
                    // 交付
                    dataQueue.add(recvPack.getTcpS().getData());
                    expectedSeq += 100;

                    // SR: 检查缓存中是否有可以连续交付的数据包(下一个包是否在缓存中）
                    while (recvBuffer.containsKey(expectedSeq))
                    {
                        System.out.println("[SR] Deliver buffered packet, seq: " + expectedSeq);
                        dataQueue.add(recvPack.getTcpS().getData());
                        recvBuffer.remove(expectedSeq);
                        expectedSeq += 100;
                    }
                }
                else
                {   // SR：缓存失序包(recvSeq > expectedSeq)
                    if (!recvBuffer.containsKey(recvSeq))
                    {
                        System.out.println("[SR] Buffer out-of-order packet, seq: " + recvSeq);
                        recvBuffer.put(recvSeq, recvPack.getTcpS().getData());
                    }
                    else // 不在窗口内
                        System.out.println("[SR] Receive duplicate packet, seq: " + recvSeq);
                }

                // SR：发送该包ACK，只要在窗口内就确认
                tcpH.setTh_ack(recvSeq);
                ackPack = new TCP_PACKET(tcpH,tcpS,recvPack.getSourceAddr());
                tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
                reply(ackPack);
            }
            else if (recvSeq < expectedSeq)
            {   // 窗口外延迟到达
                System.out.println("[SR] Receive old packet (outside window), seq: " + recvSeq + ", still ACK it");
                // SR:仍要发送ACK，防止放松段一直超时重传
                tcpH.setTh_ack(recvSeq);
                ackPack = new  TCP_PACKET(tcpH, tcpS,recvPack.getSourceAddr());
                tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
                reply(ackPack);
            }
            else
            {   // SR：窗口外未来的包，直接丢弃不缓存
                System.out.println("[SR] Receive packet outside window, seq: " + recvSeq+ ", ignored, expected: " + expectedSeq);
            }
        }
        else{  // 校验失败，数据损坏，直接丢弃等待超时重传
            System.out.println("[SR] Receive corrupted packet!  " );
        }

        System.out.println();

        //交付数据（每20组数据交付一次）
        if(dataQueue.size() == 20)
            deliver_data();
    }

    @Override
    //交付数据（将数据写入文件）；不需要修改
    //把 dataQueue 里的 int 数组按行追加写入 recvData.txt
    public void deliver_data() {
        //检查dataQueue，将数据写入文件
        File fw = new File("recvData.txt");
        BufferedWriter writer;

        try {
            writer = new BufferedWriter(new FileWriter(fw, true));

            //循环检查data队列中是否有新交付数据
            while(!dataQueue.isEmpty()) {
                int[] data = dataQueue.poll();

                //将数据写入文件
                for(int i = 0; i < data.length; i++) {
                    writer.write(data[i] + "\n");
                }

                writer.flush();		//清空输出缓存
            }
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    //回复ACK报文段
    public void reply(TCP_PACKET replyPack) {
        //设置错误控制标志
        replyPack.getTcpH().setTh_eflag((byte)7);	//eFlag = 0，信道无错误，接收方向发送方发送ACK或NACK信息时不会出现错误
        //发送数据报
        client.send(replyPack);
    }

}
