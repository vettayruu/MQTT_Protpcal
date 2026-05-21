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

        # 核心修改 1：如果设备已经是老面孔，保留它当前的 busy/available 状态，不要覆盖
        # 如果是新设备注册，才硬性初始化为 "available"
        if dev_id in self.devices:
            current_status = self.devices[dev_id].get("devStatus", "available")
        else:
            current_status = "available"  # 🎯 默认注册时都是空闲状态

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
            robot_id = self.match_results[dev_id]  # 找到它之前绑定的机器人

            # 释放机器人：将其状态恢复为可用状态
            if robot_id in self.devices:
                self.devices[robot_id]["devStatus"] = "available"
                print(f"🔄 [RELEASE] 由于控制端断开，机器人 '{robot_id}' 已自动释放，恢复空闲状态(available)。")

            del self.match_results[dev_id]  # 从配对映射表中移除

        # 情况 B: 退出的是一个正在被控制的机器人 (robot)
        elif dev_type == "robot":
            # 反向查找是谁在控制这个机器人
            controlling_user = None
            for u_id, r_id in self.match_results.items():
                if r_id == dev_id:
                    controlling_user = u_id
                    break

            if controlling_user:
                # 释放浏览器：让浏览器也变回空闲状态，可以去匹配别的机器人
                if controlling_user in self.devices:
                    self.devices[controlling_user]["devStatus"] = "available"
                    print(f"🔄 [RELEASE] 由于机器人断线，用户端 '{controlling_user}' 的状态已重置为空闲(available)。")
                del self.match_results[controlling_user]

        # 最后将设备从在线列表中彻底移除
        del self.devices[dev_id]
        self.print_device_table()

    def _handle_request(self, dev_id, dev_model):
        """
        用户请求机器人
        dev_id: 请求方 browser 的 id
        dev_model: 用户需要的机器人型号
        """
        print(f"📨 [REQUEST] 用户 '{dev_id}' 请求机器人型号: {dev_model}")

        # 1. 检查请求方是否在线
        if dev_id not in self.devices:
            print(f"❌ 请求失败: 用户 '{dev_id}' 不在线")
            return

        # 2. 如果用户已经匹配过机器人
        if dev_id in self.match_results:
            robot_id = self.match_results[dev_id]
            print(f"⚠️ 用户 '{dev_id}' 已经绑定机器人 '{robot_id}'")
            return

        # 3. 查找空闲机器人
        target_robot_id = None

        for r_id, info in self.devices.items():

            # 只查找 robot
            if info.get("devType") != "robot":
                continue

            # 型号过滤
            if info.get("devModel") != dev_model:
                continue

            # 必须是空闲
            if info.get("devStatus") != "available":
                continue

            target_robot_id = r_id
            break

        # 4. 没找到机器人
        if target_robot_id is None:
            print(f"⚠️ 当前没有空闲机器人可用: {dev_model}")

            socketio.emit(
                'match_failed',
                {
                    "reason": "No available robot",
                    "model": dev_model
                },
                namespace='/ws',
                to=user_sid_mapping.get(dev_id)
            )

            return

        # 5. 建立映射
        self.match_results[dev_id] = target_robot_id

        # 6. 更新状态
        self.devices[dev_id]["devStatus"] = "busy"
        self.devices[target_robot_id]["devStatus"] = "busy"

        print(f"✅ 匹配成功: 用户 '{dev_id}' <--> 机器人 '{target_robot_id}'")

        # 7. 回传给用户
        # socketio.emit(
        #     'match_success',
        #     {
        #         "robotId": target_robot_id,
        #         "robotModel": self.devices[target_robot_id]["devModel"]
        #     },
        #     namespace='/ws',
        #     to=user_sid_mapping.get(dev_id)
        # )

        self.client.publish(
            f"dev/{dev_id}",
            json.dumps({
                "type": self.devices[target_robot_id]["devModel"],
                "devId": target_robot_id
            })
        )

        # 8. （可选）通知机器人
        self.client.publish(
            f"robot/{target_robot_id}/assign",
            json.dumps({
                "userId": dev_id
            })
        )

        self.print_device_table()

    def _handle_unrequest(self, dev_id):
        """
        用户主动释放机器人
        """

        print(f"📴 [UNREQUEST] 用户 '{dev_id}' 请求释放机器人")

        if dev_id not in self.match_results:
            print(f"⚠️ 用户 '{dev_id}' 当前没有绑定机器人")
            return

        robot_id = self.match_results[dev_id]

        # 恢复状态
        if dev_id in self.devices:
            self.devices[dev_id]["devStatus"] = "available"

        if robot_id in self.devices:
            self.devices[robot_id]["devStatus"] = "available"

        # 删除映射
        del self.match_results[dev_id]

        print(f"🔓 已释放: 用户 '{dev_id}' 与机器人 '{robot_id}'")

        # 通知用户
        socketio.emit(
            'robot_released',
            {
                "robotId": robot_id
            },
            namespace='/ws',
            to=user_sid_mapping.get(dev_id)
        )

        # （可选）通知机器人
        self.client.publish(
            f"robot/{robot_id}/release",
            json.dumps({
                "userId": dev_id
            })
        )

        self.print_device_table()

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
        # ================= 1. 打印全量设备列表 =================
        print("\n=== Current Active Devices ===")
        if not self.devices:
            print("No devices connected.")
        else:
            print(f"{'Time':<25} | {'ID':<45} | {'Device Type':<15} | {' Model':<25} | {'Status':<10}")
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



# --- Initialize Flask & SocketIO ---
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

latest_messages = deque(maxlen=10)

manager = DeviceManager(host="liust.local", port=443, timeout_limit=5000)

# @app.route('/offer', methods=['POST'])
# def handle_offer():
#     data = request.json
#     if not data:
#         return jsonify({"status": "error"}), 400
#
#     print(f"📩 BTP Message: {data}")
#     socketio.emit(
#         'btp_action',
#         data,
#         namespace='/ws'
#     )
#
#     return jsonify({
#         "status": "dispatched",
#         "data": data
#     }), 200

user_sid_mapping = {}
@app.route('/offer', methods=['POST'])
def handle_offer():
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "No JSON payload"}), 400

    # 规定 BTP 发过来的数据里必须包含目标用户的 targetUserId
    # 例如: { "targetUserId": "user_abc_123", "action": "move_forward" }
    target_user_id = data.get("userID")

    if not target_user_id:
        return jsonify({"status": "error", "message": "Missing 'targetUserId' in payload"}), 400

    print(f"📩 Get Message from BTP，Send to user: {target_user_id}")

    # 从映射表中找出该用户的专属 sid
    user_sid = user_sid_mapping.get(target_user_id)

    if user_sid:
        # ⭐ 核心修改：使用 to=参数 限制只发给这个特定的房间（即该用户的 sid）
        socketio.emit(
            'btp_action',
            data,
            to=user_sid,
            namespace='/ws'
        )
        return jsonify({
            "status": "dispatched",
            "msg": f"Successfully sent to user {target_user_id}"
        }), 200
    else:
        # 如果用户根本不在线
        print(f"⚠️ 推送失败: 用户 {target_user_id} 当前不在线 (找不到对应的 WebSocket 连接)")
        return jsonify({
            "status": "failed",
            "message": f"User {target_user_id} is not connected via WebSocket"
        }), 404


@app.route('/device', methods=['GET'])
def handle_device():
    robot_dev_json = manager.get_device_json()
    return Response(robot_dev_json, mimetype='application/json'), 200


# --- WebSocket Connect Test ---
@socketio.on('connect', namespace='/ws')
def test_connect():
    print("🤖 Robot Connected by WebSocket.")
    emit('response', {'data': 'Connected'})

# --- User Register ---
@socketio.on('register_user', namespace='/ws')
def handle_user_register(data):
    """User Register for Websoclet"""
    user_id = data.get("userId") or data.get("devId")
    if user_id:
        # request.sid 是 Flask-SocketIO 自动为当前连接生成的唯一标识
        user_sid_mapping[user_id] = request.sid
        print(f"🔑 身份绑定成功: 用户 '{user_id}' -> SID '{request.sid}'")
        emit('response', {'status': 'registered', 'userId': user_id})


if __name__ == '__main__':
    # Start MQTT Firstly
    manager.start()

    try:
        print("🌐 Starting Flask-SocketIO server on port 8080...")
        socketio.run(app, host='0.0.0.0', port=8080)
    except KeyboardInterrupt:
        pass
    finally:
        manager.stop()