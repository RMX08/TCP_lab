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

    // 创建计时器及重传任务
    private UDT_Timer timer;

    // 创建GBN 滑动窗口变量
    private volatile int base = 1;      // 最早未确认的包序号
    private volatile int nextSeq = 1;   // 下一个待发送的包序号
    private int N = 5;                  // 窗口大小
    // 已发送但没确认的包
    private ConcurrentHashMap<Integer,TCP_PACKET> sentPackets = new ConcurrentHashMap<>();

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
            waitACK();  // 窗口满了，等待ACK释放窗口空间

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

        // 缓存已发送的包（用于重传）
        sentPackets.put(currentSeq,tcpPack);

        //发送 TCP 数据报
        udt_send(tcpPack);
        System.out.println("[GBN] Sent packet seq: " + currentSeq + ", Window: [" + base + ", " + (base + N * 100) + ")");

        // 如果是窗口内的第一个包，启动计时器
        if (base == nextSeq)
            startTimer();

        // 更新下一个发送序号
        nextSeq += appData.length;
    }

    @Override
    //不可靠发送：将打包好的TCP数据报通过不可靠传输信道发送；仅需修改错误标志
    public void udt_send(TCP_PACKET stcpPack) {
        //设置错误控制标志
        tcpH.setTh_eflag((byte)7);  //eFlag = 0，信道无错误，发送方像接收方发送数据时不会产生位错
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
            System.out.println("[GBN] Received Ack : " + currentAck);

            //GBN: 累计确认
            if (currentAck >= base){
                // 收到一个ACK, 窗口右移一个包
                base = currentAck + 100;    // 更新base
                System.out.println("[GBN] Window slides to base: " + base);

                // 停止当前计时器
                stopTimer();

                // 如果窜口内还有未确认的包，重启计时器
                if (base < nextSeq)
                    startTimer();
            }else{
                // 收到重复/错误 ACK -> 忽略，继续等超时
                System.out.println("[GBN] Ignored old ACK: " + currentAck );
                this.timer = null;
            }
        }
    }

    @Override
    //接收到ACK报文：检查校验和，将确认号插入ack队列;NACK的确认号为－1；不需要修改
    public void recv(TCP_PACKET recvPack) {
        // 检查ACK校验和
        if (CheckSum.computeChkSum(recvPack) == recvPack.getTcpH().getTh_sum())
        {   // 校验和正确
            int ackNum = recvPack.getTcpH().getTh_ack();
            System.out.println("[RDT-2.2] Receive ACK Number： "+ ackNum);
            ackQueue.add(ackNum);
        }
        else
        {   // 校验和错误，ACK损坏,视为重复ACK
            System.out.println("[RDT-2.2] Receive Corrupted ACK , treat as duplicate ");

            if (!ackQueue.isEmpty())
            { // 使用上一次的ACK值（如果队列不为空）
                ackQueue.add(ackQueue.peek());
            }
            else
            { // 还没有收到任何ACK
                ackQueue.add(-1);
            }

        }

        //处理ACK报文
        waitACK();
    }

    // GBN: 启动计时器
    private synchronized void startTimer() {
        if (timer == null) {
            timer = new UDT_Timer();
            timer.schedule(new java.util.TimerTask(){
                @Override
                public void run() {
                    System.out.println("[GBN] Timeout! Retransmitting from base: " + base);
                    // GBN : 超时重传窗口内所有未确认的包
                    for (int i = base; i < nextSeq; i += 100)
                    {
                        TCP_PACKET pkt = sentPackets.get(i);
                        if (pkt != null)
                        {
                            udt_send(pkt);
                            System.out.println("[GBN] Retransmit packet seq: " + i);
                        }
                    }
                }
            },2000);
        }
    }

    //GBN： 停止计时器
    private synchronized void stopTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

}