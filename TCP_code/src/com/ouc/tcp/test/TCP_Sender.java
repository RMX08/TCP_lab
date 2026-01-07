/***************************2.1: ACK/NACK
 **************************** Feng Hong; 2015-12-09*/

package com.ouc.tcp.test;

import com.ouc.tcp.client.TCP_Sender_ADT;
import com.ouc.tcp.client.UDT_RetransTask;
import com.ouc.tcp.client.UDT_Timer;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class TCP_Sender extends TCP_Sender_ADT {

    private TCP_PACKET tcpPack;	//当前“正在等待 ACK 的那一个数据报文”

    // 创建 SR 滑动窗口变量
    private volatile int base = 1;      // 最早未确认的包序号
    private volatile int nextSeq = 1;   // 下一个待发送的包序号
    private int N = 5;                  // 窗口大小

    // SR ： 每个包独立的状态和计时器
    private ConcurrentHashMap<Integer,TCP_PACKET> sentPackets = new ConcurrentHashMap<>();  // 已发送但没确认的包
    private ConcurrentHashMap<Integer,Boolean> ackedPackets = new ConcurrentHashMap<>();    // 记录每个包是否已确认
    private ConcurrentHashMap<Integer,UDT_Timer> packetTimers = new ConcurrentHashMap<>();  // 每个包独立的计时器

    /*构造函数*/
    public TCP_Sender() {
        super();	//调用超类构造函数,创建底层 Client 并初始化 ackQueue 等；打印 Sender socket 地址
        super.initTCP_Sender(this);	//启动 ListenACK 监听线程,初始化TCP发送端
    }

    @Override
    //可靠发送（应用层调用）：封装应用层数据，产生TCP数据报；需要修改
    public void rdt_send(int dataIndex, int[] appData) {
        // 计算当前包的序号
        int currentSeq = dataIndex * appData.length + 1;

        // GBN:检查窗口是否已满 （窗口范围 [base, base + N*100)
        while (nextSeq >= base + N * 100)
        {
            try {
                Thread.sleep(10);   // 防止CPU空转
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 创建新的TCP_HEADER 和 TCP_SEGMENT
        TCP_HEADER newTcpH =  new TCP_HEADER();
        newTcpH.setTh_sport(tcpH.getTh_sport());
        newTcpH.setTh_dport(tcpH.getTh_dport());
        newTcpH.setTh_seq(currentSeq);//包序号设置为字节流号：

        TCP_SEGMENT newTcpS =  new TCP_SEGMENT();
        newTcpS.setData(appData);

        //生成 TCP 数据报
        tcpPack = new TCP_PACKET(newTcpH, newTcpS, destinAddr);//组装报文
        newTcpH.setTh_sum(CheckSum.computeChkSum(tcpPack));//更新带有checksum的TCP 报文头
        tcpPack.setTcpH(newTcpH);

        // SR: 除了缓存包，还要标记为"未确认"
        sentPackets.put(currentSeq,tcpPack);
        ackedPackets.put(currentSeq,false);     // 标记为未确认

        //发送 TCP 数据报
        udt_send(tcpPack);
        System.out.println("[SR] Sent packet seq: " + currentSeq + ", Window: [" + base + ", " + (base + N * 100) + ")");

        // SR: 每个包都启动独立计时器
        startTimer(currentSeq);

        // 更新下一个发送序号
        nextSeq += appData.length;
    }

    @Override
    //不可靠发送：将打包好的TCP数据报通过不可靠传输信道发送；仅需修改错误标志
    public void udt_send(TCP_PACKET stcpPack) {
        //设置错误控制标志
        stcpPack.getTcpH().setTh_eflag((byte)7);  //eFlag = 0，信道无错误，发送方像接收方发送数据时不会产生位错
        //System.out.println("to send: "+stcpPack.getTcpH().getTh_seq());
        //发送数据报
        client.send(stcpPack);
    }

    @Override
    //需要修改
    public synchronized void waitACK() {
        //从 ackQueue 取一个 ACK 号，判断它是不是当前 tcpPack 的确认
        //循环检查确认号对列中是否有新收到的ACK
        while (!ackQueue.isEmpty()){
            int currentAck=ackQueue.poll();
            System.out.println("[SR] Received Ack : " + currentAck);

            //SR: 选择确认，每个包都确认
            if (currentAck >= base & currentAck < base + N * 100)
            {
                // 判断该包是否在窗口内且未确认
                if (ackedPackets.containsKey(currentAck) && !ackedPackets.get(currentAck))
                {
                    // 标记为已确认
                    ackedPackets.put(currentAck, true);
                    System.out.println("[SR] Packet" + currentAck + " Acked : ");

                    // 停止该包的计时器
                    stopTimer(currentAck);

                    // 若是base，滑动窗口
                    if (currentAck == base)
                    {
                        // 批量划过
                        while (ackedPackets.containsKey(base) && ackedPackets.get(base))
                        {
                            sentPackets.remove(base);
                            ackedPackets.remove(base);
                            packetTimers.remove(base);

                            base += 100;
                            System.out.println("[SR] Window slides to base: " + base);
                        }
                    }
                }
            }
            else if (currentAck < base)
                System.out.println("[SR] Ignored old ACK: " + currentAck );
        }
    }

    @Override
    //接收到ACK报文：检查校验和，将确认号插入ack队列;NACK的确认号为－1；不需要修改
    public void recv(TCP_PACKET recvPack) {
        // 检查ACK校验和
        if (CheckSum.computeChkSum(recvPack) == recvPack.getTcpH().getTh_sum())
        {   // 校验和正确
            int ackNum = recvPack.getTcpH().getTh_ack();
            System.out.println("[SR] Receive ACK Number： "+ ackNum);
            ackQueue.add(ackNum);
        }
        else
        {   // 校验和错误，ACK损坏,忽略
            System.out.println("[SR] Receive Corrupted ACK , treat as duplicate ");
        }

        //处理ACK报文
        waitACK();
    }

    // SR: 启动独立计时器
    private synchronized void startTimer(final int seq) {
        UDT_Timer timer = new UDT_Timer();
        packetTimers.put(seq,timer);

        timer.schedule(new java.util.TimerTask() {
            @Override
            public void run()
            {   // SR: 只重传超时的那个包
                if (ackedPackets.containsKey(seq) && !ackedPackets.get(seq)) {
                    System.out.println("[SR] Timeout! Retransmitting packet seq: " + seq);
                    TCP_PACKET pkt = sentPackets.get(seq);
                    if (pkt != null) {
                        udt_send(pkt);
                        // 重新启动该包的计时器
                        startTimer(seq);
                    }
                }
            }
        },1000);
    }

    //SR： 停止计时器
    private synchronized void stopTimer(int seq) {
        UDT_Timer timer = packetTimers.get(seq);
        if (timer != null) {
            timer.cancel();
            packetTimers.remove(seq);
        }
    }

}