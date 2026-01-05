/***************************2.1: ACK/NACK
 **************************** Feng Hong; 2015-12-09*/

package com.ouc.tcp.test;

import com.ouc.tcp.client.TCP_Sender_ADT;
import com.ouc.tcp.client.UDT_RetransTask;
import com.ouc.tcp.client.UDT_Timer;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

public class TCP_Sender extends TCP_Sender_ADT {

    private TCP_PACKET tcpPack;	//当前“正在等待 ACK 的那一个数据报文”
    private volatile int flag = 0;
    //0：还没收到期望 ACK（rdt_send 会一直等）
    //1：已收到期望 ACK（rdt_send 返回）

    // 创建计时器及重传任务
    private UDT_Timer timer;
    private UDT_RetransTask reTrans;

    /*构造函数*/
    public TCP_Sender() {
        super();	//调用超类构造函数,创建底层 Client 并初始化 ackQueue 等；打印 Sender socket 地址
        super.initTCP_Sender(this);	//启动 ListenACK 监听线程,初始化TCP发送端
    }

    @Override
    //可靠发送（应用层调用）：封装应用层数据，产生TCP数据报；需要修改
    public void rdt_send(int dataIndex, int[] appData) {

        //生成TCP数据报（设置序号和数据字段/校验和),注意打包的顺序
        tcpH.setTh_seq(dataIndex * appData.length + 1);//包序号设置为字节流号：
        tcpS.setData(appData);
        tcpPack = new TCP_PACKET(tcpH, tcpS, destinAddr);//组装报文
        //更新带有checksum的TCP 报文头
        tcpH.setTh_sum(CheckSum.computeChkSum(tcpPack));
        tcpPack.setTcpH(tcpH);

        //发送TCP数据报
        udt_send(tcpPack);

        // 启动计时器：3s后执行重传任务
        timer = new UDT_Timer();
        reTrans = new UDT_RetransTask(client, tcpPack);
        timer.schedule(reTrans, 3000, 3000);

        flag = 0;

        //等待ACK报文
        //waitACK();

        //“停等”的等 ACK 阶段：只有当后台收到 ACK 并把 flag 改成 1，这里才会返回
        while (flag==0); // 可能死循环

        // 收到正确 ACK，关闭计时器
        if(timer != null)
            timer.cancel();
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
    public void waitACK() {
        //从 ackQueue 取一个 ACK 号，判断它是不是当前 tcpPack 的确认
        //循环检查确认号对列中是否有新收到的ACK
        if(!ackQueue.isEmpty()){
            int currentAck=ackQueue.poll();
            int expectedACK = tcpPack.getTcpH().getTh_seq();    // 期望收到的ACK

            if (currentAck == expectedACK){
                // 收到正确ACK，设置flag=1，让rdt_send继续
                System.out.println("[RDT-3.0] ACK matched, confirmed: "+expectedACK);
                flag = 1; // 让rdt_send继续
            }else{
                // 收到重复/错误 ACK -> 忽略，继续等超时
                System.out.println("[RDT-3.0] Ignored ACK: " + currentAck + " (Expected: " + expectedACK + ")");
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

}