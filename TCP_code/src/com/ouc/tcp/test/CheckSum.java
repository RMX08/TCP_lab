package com.ouc.tcp.test;

import java.util.zip.CRC32;

import com.ouc.tcp.message.TCP_HEADER;
import com.ouc.tcp.message.TCP_PACKET;

public class CheckSum {

    /*计算TCP报文段校验和：只需校验TCP首部中的seq、ack和sum，以及TCP数据字段*/
    public static short computeChkSum(TCP_PACKET tcpPack) {
        int checkSum = 0;
        TCP_HEADER header = tcpPack.getTcpH();

        // 校验seq 4bytes → 32bits → 两个16bits
        int seq = header.getTh_seq();
        checkSum += (seq >> 16) & 0xFFFF;    // 高16位
        checkSum += seq & 0xFFFF;            // 低16位

        // 校验ack 4bytes
        int ack = header.getTh_ack();
        checkSum += (ack >> 16) & 0xFFFF;
        checkSum += ack & 0xFFFF;

        // 校验数据段
        int[] data = tcpPack.getTcpS().getData();
        if (data != null)
        {
            // int 4字节
            for (int value : data) {
                checkSum += (value >> 16) & 0xFFFF;
                checkSum += value & 0xFFFF;
            }
        }

        // 处理进位
        while ((checkSum >> 16) != 0)
        {
            checkSum = (checkSum & 0xFFFF) + (checkSum >> 16);
        }

        // 取反得到校验和
        checkSum = ~checkSum;

        return (short) checkSum;
    }

}