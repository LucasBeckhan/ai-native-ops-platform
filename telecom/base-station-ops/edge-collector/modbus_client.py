#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/base-station-ops/edge-collector/modbus_client.py
# 所属模块: 场景三 —— Modbus TCP 协议解析器（纯 Python 从零实现，零依赖）
# 运行方式:
#   终端1（模拟从站设备）: python3 modbus_simulator.py
#   终端2（采集器）:       python3 modbus_client.py --read-holding 0 4
# 说明:
#   - Modbus TCP 帧结构（MBAP 头 + PDU），教学实现覆盖三个功能码：
#       0x03 Read Holding Registers（读保持寄存器：温度/功率等模拟量）
#       0x04 Read Input Registers（读输入寄存器：只读传感器）
#       0x06 Write Single Register（写单寄存器：下发阈值，工业控制）
#   - 为什么手写：面试"你了解工业协议吗"需要协议级回答；
#     生产可换 pymodbus（本实现语义与其对齐）。
#   - MBAP 头 7 字节：事务标识(2) + 协议标识(2,=0) + 长度(2) + 单元标识(1)
#     PDU：功能码(1) + 数据
# =====================================================================
import argparse
import socket
import struct


class ModbusTCPClient:
    """最小 Modbus TCP 客户端（同步阻塞，采集循环场景够用；
    高并发采集用 pymodbus 的异步版）。"""

    def __init__(self, host: str = "127.0.0.1", port: int = 5020, unit_id: int = 1,
                 timeout: float = 3.0):
        self.host, self.port, self.unit_id = host, port, unit_id
        self.timeout = timeout
        self._sock: socket.socket | None = None
        self._txn = 0

    def connect(self) -> None:
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)

    def close(self) -> None:
        if self._sock:
            self._sock.close()
            self._sock = None

    def _recv_exact(self, n: int) -> bytes:
        """精确接收 n 字节（TCP 是字节流，必须循环收满）。"""
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise IOError("连接被对端关闭")
            buf += chunk
        return buf

    def _transact(self, pdu: bytes) -> bytes:
        """发送请求 PDU，返回响应 PDU（自动处理 MBAP 头与事务号校验）。"""
        self._txn = (self._txn + 1) & 0xFFFF
        mbap = struct.pack(">HHHB", self._txn, 0, len(pdu) + 1, self.unit_id)
        self._sock.sendall(mbap + pdu)
        txn, proto, length, unit = struct.unpack(">HHHB", self._recv_exact(7))
        if txn != self._txn:
            raise IOError(f"事务号不匹配: 发 {self._txn} 收 {txn}")
        if proto != 0:
            raise IOError(f"协议标识异常: {proto}")
        body = self._recv_exact(length - 1)
        if unit != self.unit_id:
            raise IOError("单元标识不匹配")
        return body

    # ---- 功能码 0x03/0x04：读寄存器 ----
    def read_registers(self, address: int, count: int, func: int = 0x03) -> list[int]:
        """读 count 个 16 位寄存器，返回原始值列表。
        异常响应：功能码|0x80 + 异常码（0x02 非法地址 / 0x03 非法数量）。"""
        if not 1 <= count <= 125:
            raise ValueError("count 范围 1~125（协议约束）")
        pdu = struct.pack(">BHH", func, address, count)
        resp = self._transact(pdu)
        if resp[0] == func + 0x80:
            raise IOError(f"Modbus 异常码 {resp[1]}（1=非法功能 2=非法地址 3=非法数量）")
        byte_count = resp[1]
        return list(struct.unpack(f">{byte_count // 2}H", resp[2:2 + byte_count]))

    # ---- 功能码 0x06：写单寄存器 ----
    def write_register(self, address: int, value: int) -> bool:
        """写单寄存器。正常响应 = 请求回显。"""
        pdu = struct.pack(">BHH", 0x06, address, value)
        resp = self._transact(pdu)
        return resp == pdu


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Modbus TCP 采集器")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5020)
    parser.add_argument("--read-holding", nargs=2, type=int, metavar=("ADDR", "COUNT"))
    parser.add_argument("--read-input", nargs=2, type=int, metavar=("ADDR", "COUNT"))
    parser.add_argument("--write", nargs=2, type=int, metavar=("ADDR", "VALUE"))
    args = parser.parse_args()

    client = ModbusTCPClient(args.host, args.port)
    client.connect()
    try:
        if args.read_holding:
            addr, cnt = args.read_holding
            print(f"保持寄存器[{addr}..{addr+cnt-1}]:",
                  client.read_registers(addr, cnt, 0x03))
        if args.read_input:
            addr, cnt = args.read_input
            print(f"输入寄存器[{addr}..{addr+cnt-1}]:",
                  client.read_registers(addr, cnt, 0x04))
        if args.write:
            addr, val = args.write
            print(f"写寄存器[{addr}]={val}:",
                  "成功" if client.write_register(addr, val) else "失败")
        if not (args.read_holding or args.read_input or args.write):
            parser.print_help()
    finally:
        client.close()
