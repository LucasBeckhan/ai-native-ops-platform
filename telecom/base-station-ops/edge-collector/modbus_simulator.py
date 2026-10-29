#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/base-station-ops/edge-collector/modbus_simulator.py
# 所属模块: 场景三 —— Modbus 从站模拟器（基站环境传感器：温度/湿度/功率/阈值）
# 运行方式: python3 modbus_simulator.py   （监听 5020 端口）
# 说明:
#   - 寄存器地图（与真实基站动环监控的点位规划对齐）：
#       保持寄存器（0x03 可读写）: 0=温度告警阈值, 1=功率告警阈值
#       输入寄存器（0x04 只读）:   100=温度(0.1℃), 101=湿度(%), 102=功率(W),
#                                  103=电池电压(0.01V)
#   - 温度按正弦波动模拟昼夜变化；阈值寄存器被采集器写后生效。
#   - 生产对照：真实从站是 RTU/网关，本模拟器让协议实验零硬件依赖。
# =====================================================================
import socket
import struct
import threading
import math
import time

HOST, PORT = "127.0.0.1", 5020
# 寄存器存储（简化：保持与输入寄存器同一空间，地址独立规划）
holding = {0: 450, 1: 3000}          # 阈值：温度 45.0℃ / 功率 3000W
inputs = {100: 0, 101: 0, 102: 0, 103: 0}
lock = threading.Lock()


def sensor_loop() -> None:
    """每秒刷新一次输入寄存器（模拟传感器采样）。"""
    t0 = time.time()
    while True:
        with lock:
            # 温度昼夜正弦波动 25~42℃（0.1℃ 精度）
            inputs[100] = int((33.5 + 8.5 * math.sin((time.time() - t0) / 60)) * 10)
            inputs[101] = random_humidity()
            inputs[102] = 2500 + int(500 * math.sin((time.time() - t0) / 90))
            inputs[103] = 5310   # 53.10V 直流供电
        time.sleep(1)


def random_humidity() -> int:
    import random
    return random.randint(30, 80)


def handle(conn: socket.socket) -> None:
    try:
        while True:
            header = recv_exact(conn, 7)
            txn, proto, length, unit = struct.unpack(">HHHB", header)
            pdu = recv_exact(conn, length - 1)
            resp_pdu = process(pdu)
            resp_mbap = struct.pack(">HHHB", txn, 0, len(resp_pdu) + 1, unit)
            conn.sendall(resp_mbap + resp_pdu)
    except (ConnectionResetError, struct.error):
        pass
    finally:
        conn.close()


def recv_exact(conn: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionResetError
        buf += chunk
    return buf


def process(pdu: bytes) -> bytes:
    func = pdu[0]
    if func == 0x03:                       # 读保持寄存器
        addr, count = struct.unpack(">HH", pdu[1:5])
        with lock:
            vals = [holding.get(addr + i, 0) for i in range(count)]
    elif func == 0x04:                     # 读输入寄存器
        addr, count = struct.unpack(">HH", pdu[1:5])
        with lock:
            vals = [inputs.get(addr + i, 0) for i in range(count)]
    elif func == 0x06:                     # 写单寄存器
        addr, value = struct.unpack(">HH", pdu[1:5])
        with lock:
            holding[addr] = value
        return pdu                          # 正常响应=请求回显
    else:
        return bytes([func | 0x80, 0x01])   # 非法功能
    return bytes([func, len(vals) * 2]) + struct.pack(f">{len(vals)}H", *vals)


if __name__ == "__main__":
    threading.Thread(target=sensor_loop, daemon=True).start()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"Modbus 从站模拟器监听 {HOST}:{PORT}（温度/湿度/功率/电压寄存器就绪）")
    while True:
        conn, addr = server.accept()
        print(f"采集器接入: {addr}")
        threading.Thread(target=handle, args=(conn,), daemon=True).start()
