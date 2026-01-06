/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.ouc.tcp.client.TCP_Receiver_ADT;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

public class TCP_Receiver extends TCP_Receiver_ADT {

    private TCP_PACKET ackPack;	//回复的ACK报文段

    private int expectedSeq = 1;    // 期望收到的的序号
    private int lastACK = 0;        // 上一次发送的ACK序号

    int sequence=1;//用于记录当前待接收的包序号，注意包序号不完全是

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
            if (recvSeq ==  expectedSeq)
            {   // 新包
                System.out.println("[GBN] Receive expected packet, seq: " + recvSeq);
                // 交付数据
                dataQueue.add(recvPack.getTcpS().getData());
                // 更新期望序号
                expectedSeq += 100;
                // 发送 ACK
                lastACK = recvSeq;
                tcpH.setTh_ack(recvSeq);
            }
            else
            {   // 失序包
                System.out.println("[GBN] Receive out-of-order packet, seq: " + recvSeq);
                //GBN： 丢弃不缓存，发送重复ACK
                tcpH.setTh_ack(lastACK);
            }
            //生成ACK报文段（设置确认号）
            ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
            tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
            //回复ACK报文段
            reply(ackPack);
        }
        else{  // 校验失败，数据损坏
            System.out.println("[GBN] Receive corrupted packet!  " );

            // GBN：发送重复ACK
            tcpH.setTh_ack(lastACK);
            ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
            tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
            //回复ACK报文段
            reply(ackPack);
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
        tcpH.setTh_eflag((byte)7);	//eFlag = 0，信道无错误，接收方向发送方发送ACK或NACK信息时不会出现错误
        //发送数据报
        client.send(replyPack);
    }

}
