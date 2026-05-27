from flask import Flask, request, jsonify, Response
from flask_socketio import SocketIO, emit
from collections import deque

import ssl
import time
import json
import paho.mqtt.client as mqtt


class DeviceManager:
    def __init__(self, host="liust.local", port=443, timeout_limit=5000):
        self.host = host
        self.port = port
        self.timeout_limit = timeout_limit

        self.devices = {}
        self.match_results = {}

        self.client = mqtt.Client(transport="websockets")
        self.client.tls_set(cert_reqs=ssl.CERT_NONE)
        self.client.ws_set_options(path="/mqtt")

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.running = True

    def start(self):
        try:
            print(f"🔄 Connecting to {self.host}:{self.port} via WSS...")
            self.client.connect(self.host, self.port, 60)
            self.client.loop_start()
            print("🚀 Device Manager started successfully.")
        except Exception as e:
            print(f"❌ Connection failed: {e}")

    def stop(self):
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()
        print("🛑 Device Manager stopped.")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"✅ Successfully connected to {self.host} via WebSockets")
            self.client.subscribe("mgr/register")
            self.client.subscribe("mgr/unregister")
            self.client.subscribe("mgr/request")
            self.client.subscribe("mgr/unrequest")
            # 🎯 核心修改 1：使用通配符 '+'，允许接收任意机器人ID发来的 ping 请求
            self.client.subscribe("mgr/time/ping/+")
        else:
            print(f"❌ Connection failed with code {rc}")

    def on_message(self, client, userdata, msg):
        str_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        try:
            payload_str = msg.payload.decode()
            data = json.loads(payload_str)
        except Exception:
            print(f"⚠️ Receive non-JSON or invalid data on {msg.topic}")
            return

        if isinstance(data, list):
            if len(data) > 0:
                data = data[0]
            else:
                print(f"⚠️ Receive an empty list on {msg.topic}")
                return

        # 🎯 核心修改 2：拦截时间同步 Topic，直接分发（因为时间数据格式与标准设备数据不同）
        if msg.topic.startswith("mgr/time/ping/"):
            self._handle_time_ping(msg.topic, data)
            return

        dev_type = data.get("devType", "unknown")
        dev_id = data.get("devId", "unknown")
        dev_model = data.get("type", "unknown")
        dev_status = data.get("optStr", "unknown")

        if msg.topic == "mgr/register":
            self._handle_register(str_time, dev_type, dev_id, dev_model, dev_status)
        elif msg.topic == "mgr/unregister":
            self._handle_unregister(dev_id)

        elif msg.topic == "mgr/request":
            user_id = data.get("devId")
            robot_model = data.get("type")
            self._handle_request(user_id, robot_model)
        elif msg.topic == "mgr/unrequest":
            user_id = data.get("devId")
            self._handle_unrequest(user_id)

    def _handle_register(self, time_receive, dev_type, dev_id, dev_model, dev_status):
        """设备注册逻辑：统一接管状态控制"""
        if dev_id in self.devices:
            current_status = self.devices[dev_id].get("devStatus", "available")
        else:
            current_status = "available"

        self.devices[dev_id] = {
            "time": time_receive,
            "devType": dev_type,
            "devModel": dev_model,
            "devStatus": current_status,
        }
        print(f"➕ [REGISTER] {dev_type.upper()} '{dev_id}' 成功上线. 当前系统内状态: {current_status}")
        self.print_device_table()

    def _handle_unregister(self, dev_id):
        if dev_id not in self.devices:
            return

        dev_type = self.devices[dev_id].get("devType")
        print(f"➖ [UNREGISTER] {dev_type.upper()} '{dev_id}' 请求注销。")

        if dev_type == "browser" and dev_id in self.match_results:
            robot_id = self.match_results[dev_id]

            if robot_id in self.devices:
                self.devices[robot_id]["devStatus"] = "available"
                print(f"🔄 [RELEASE] 由于控制端断开，机器人 '{robot_id}' 已自动释放，恢复空闲状态(available)。")

            del self.match_results[dev_id]

        elif dev_type == "robot":
            controlling_user = None
            for u_id, r_id in self.match_results.items():
                if r_id == dev_id:
                    controlling_user = u_id
                    break

            if controlling_user:
                if controlling_user in self.devices:
                    self.devices[controlling_user]["devStatus"] = "available"
                    print(f"🔄 [RELEASE] 由于机器人断线，用户端 '{controlling_user}' 的状态已重置为空闲(available)。")
                del self.match_results[controlling_user]

        del self.devices[dev_id]
        self.print_device_table()

    def _handle_request(self, dev_id, dev_model):
        print(f"📨 [REQUEST] 用户 '{dev_id}' 请求机器人型号: {dev_model}")

        if dev_id not in self.devices:
            print(f"❌ 请求失败: 用户 '{dev_id}' 不在线")
            return

        if dev_id in self.match_results:
            robot_id = self.match_results[dev_id]
            print(f"⚠️ 用户 '{dev_id}' 已经绑定机器人 '{robot_id}'")
            return

        target_robot_id = None

        for r_id, info in self.devices.items():
            if info.get("devType") != "robot":
                continue
            if info.get("devModel") != dev_model:
                continue
            if info.get("devStatus") != "available":
                continue

            target_robot_id = r_id
            break

        if target_robot_id is None:
            print(f"⚠️ 当前没有空闲机器人可用: {dev_model}")
            socketio.emit(
                'match_failed',
                {"reason": "No available robot", "model": dev_model},
                namespace='/ws',
                to=user_sid_mapping.get(dev_id)
            )
            return

        self.match_results[dev_id] = target_robot_id
        self.devices[dev_id]["devStatus"] = "busy"
        self.devices[target_robot_id]["devStatus"] = "busy"

        print(f"✅ 匹配成功: 用户 '{dev_id}' <--> 机器人 '{target_robot_id}'")

        self.client.publish(
            f"dev/{dev_id}",
            json.dumps({
                "type": self.devices[target_robot_id]["devModel"],
                "devId": target_robot_id
            })
        )

        # self.client.publish(
        #     f"robot/{target_robot_id}/assign",
        #     json.dumps({"userId": dev_id})
        # )

        self.print_device_table()

    def _handle_unrequest(self, dev_id):
        print(f"📴 [UNREQUEST] 用户 '{dev_id}' 请求释放机器人")

        if dev_id not in self.match_results:
            print(f"⚠️ 用户 '{dev_id}' 当前没有绑定机器人")
            return

        robot_id = self.match_results[dev_id]

        if dev_id in self.devices:
            self.devices[dev_id]["devStatus"] = "available"

        if robot_id in self.devices:
            self.devices[robot_id]["devStatus"] = "available"

        del self.match_results[dev_id]
        print(f"🔓 已释放: 用户 '{dev_id}' 与机器人 '{robot_id}'")

        socketio.emit(
            'robot_released',
            {"robotId": robot_id},
            namespace='/ws',
            to=user_sid_mapping.get(dev_id)
        )

        self.client.publish(
            f"robot/{robot_id}/release",
            json.dumps({"userId": dev_id})
        )

        self.print_device_table()

    # 🎯 核心修改 3：收纳入类内部的高精度时间同步响应函数
    def _handle_time_ping(self, topic, data):
        try:
            # 1. 从 Topic 中解析出是哪台机器人发起的请求 (mgr/time/ping/{robot_uuid})
            topic_parts = topic.split('/')
            robot_uuid = topic_parts[-1]

            # 2. 拿到机器人的本地原始发送时间戳 t0
            robot_t0 = data.get("robot_t0")
            if robot_t0 is None:
                return

            # 3. 抓取管理中心服务器的高精度毫秒级时间戳
            server_time_ms = int(time.time() * 1000)

            # 4. 精准定向发布给该机器人的专属接收 Topic
            response_topic = f"mgr/time/pong/{robot_uuid}"
            response_data = {
                "robot_t0": robot_t0,
                "server_time": server_time_ms
            }

            # 使用 qos=1 确保时间包在不稳定的网络下更稳妥地送达
            self.client.publish(response_topic, json.dumps(response_data), qos=1)
            # print(f"⏱️ [MQTT TIME] Pong to {robot_uuid}: t0={robot_t0}, server={server_time_ms}")

        except Exception as e:
            print(f"❌ Error handling time ping: {e}")

    def get_device_json(self):
        device_list = []
        for dev_id, info in self.devices.items():
            device_node = {
                "time": info.get("time"),
                "id": dev_id,
                "type": info.get("devType"),
                "model": info.get("devModel"),
                "status": info.get("devStatus")
            }
            device_list.append(device_node)
        return json.dumps(device_list, ensure_ascii=False, indent=2)

    def print_device_table(self):
        print("\n=== Current Active Devices ===")
        if not self.devices:
            print("No devices connected.")
        else:
            print(f"{'Time':<25} | {'ID':<45} | {'Device Type':<15} | {'Model':<25} | {'Status':<10}")
            print("-" * 130)

            for dev_id, info in self.devices.items():
                dev_time_recv = info.get("time", "unknown")
                d_type = info.get("devType", "unknown")
                d_model = info.get("devModel", "unknown")
                d_status = info.get("devStatus", "unknown")
                print(f"{dev_time_recv:<25} | {dev_id:<45} | {d_type:<15} | {d_model:<25} | {d_status:<10}")
        print("==================================================\n")

        robot_dev_json = self.get_device_json()
        with open("active_devices.json", "w", encoding="utf-8") as f:
            f.write(robot_dev_json)


# --- 后续的 Flask / SocketIO 保持原样 ---
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

latest_messages = deque(maxlen=10)
manager = DeviceManager(host="liust.local", port=443, timeout_limit=5000)

user_sid_mapping = {}


@app.route('/offer', methods=['POST'])
def handle_offer():
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "No JSON payload"}), 400

    target_user_id = data.get("userID")
    if not target_user_id:
        return jsonify({"status": "error", "message": "Missing 'targetUserId' in payload"}), 400

    print(f"📩 Get Message from BTP，Send to user: {target_user_id}")
    user_sid = user_sid_mapping.get(target_user_id)

    if user_sid:
        socketio.emit('btp_action', data, to=user_sid, namespace='/ws')
        return jsonify({"status": "dispatched", "msg": f"Successfully sent to user {target_user_id}"}), 200
    else:
        print(f"⚠️ 推送失败: 用户 {target_user_id} 当前不在线")
        return jsonify({"status": "failed", "message": f"User {target_user_id} is not connected"}), 404


@app.route('/device', methods=['GET'])
def handle_device():
    robot_dev_json = manager.get_device_json()
    return Response(robot_dev_json, mimetype='application/json'), 200


@socketio.on('connect', namespace='/ws')
def test_connect():
    print("🤖 Robot Connected by WebSocket.")
    emit('response', {'data': 'Connected'})


@socketio.on('register_user', namespace='/ws')
def handle_user_register(data):
    user_id = data.get("userId") or data.get("devId")
    if user_id:
        user_sid_mapping[user_id] = request.sid
        print(f"🔑 身份绑定成功: 用户 '{user_id}' -> SID '{request.sid}'")
        emit('response', {'status': 'registered', 'userId': user_id})


@socketio.on('sync_time_ping', namespace='/ws')
def handle_sync_time(data):
    server_time_ms = int(time.time() * 1000)
    client_t0 = data.get("client_t0")
    emit('sync_time_pong', {"client_t0": client_t0, "server_time": server_time_ms})


@app.route('/time', methods=['GET'])
def get_server_time():
    server_time_ms = int(time.time() * 1000)
    return jsonify({"status": "success", "server_time": server_time_ms}), 200


if __name__ == '__main__':
    manager.start()
    try:
        print("🌐 Starting Flask-SocketIO server on port 8080...")
        socketio.run(app, host='0.0.0.0', port=8080)
    except KeyboardInterrupt:
        pass
    finally:
        manager.stop()