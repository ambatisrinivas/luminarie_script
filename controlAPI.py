import asyncio
import websockets
import json
import os
import socket
import threading
import logging
import time
import re
from concurrent.futures import ThreadPoolExecutor
import csv
import datetime
import psutil
from socket import SO_REUSEADDR

# Load configuration
with open('config.json') as f:
    CONFIG = json.load(f)

# Configure Logging with unique filename based on timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
log_filename = f"{timestamp}.log"
logging.basicConfig(
    level=logging.DEBUG,  # Enable DEBUG level for detailed logging
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()]
)

# Initial state for WebSocket clients
state = {
    "auto_mode": False,
    "available_scenes": [],
    "current_scene": None,
    "loaded_scene": None,
    "cw": 50.0,
    "ww": 50.0,
    "scheduler": {
        "current_cct": 3500,
        "current_interval": 0,
        "total_intervals": 8640,
        "interval_seconds": 5,
        "status": "idle",
        "interval_progress": 0
    },
    "connected_devices": {},
    "basicLogs": [],
    "advancedLogs": [],
    "scene_data": {"cct": [], "intensity": []},
    "current_cct": 3500,
    "current_intensity": 250,
    "is_manual_override": False,
    "cpu_percent": 0.0,
    "mem_percent": 0.0,
    "activationTime": None,
    "currentPhase": "Night",
    "isSystemOn": True,
}

scene_data = {}
clients = set()

class LuminaireOperations:
    def __init__(self):
        self._devices_lock = threading.RLock()
        self._send_lock = threading.Lock()
        self.min_cct = 3500
        self.max_cct = 6500
        self.min_intensity = 0
        self.max_intensity = 500
        self.INACTIVITY_THRESHOLD = CONFIG["inactivity_threshold"]
        self.devices = {}
        self.current_interval_index = 0
        self.total_intervals = 0
        self.start_time = None
        self.stop_event = threading.Event()
        self.paused = False
        logging.debug("LuminaireOperations initialized")

    def get_system_stats(self):
        logging.debug("Fetching system stats")
        cpu_percent, mem_percent = psutil.cpu_percent(interval=1), psutil.virtual_memory().percent
        logging.debug(f"System stats - CPU: {cpu_percent}%, Mem: {mem_percent}%")
        return cpu_percent, mem_percent

    def add(self, ip: str, connection: socket.socket):
        with self._devices_lock:
            self.devices[ip] = {"connection": connection, "last_seen": time.time(), "cw": 50.0, "ww": 50.0}
            if ip not in state["connected_devices"]:
                state["connected_devices"][ip] = {"cw": 50.0, "ww": 50.0}
            self.log_advanced(f"Luminaire connected: {ip}")
            logging.info(f"Added luminaire {ip}")
            logging.debug(f"Device list updated: {list(self.devices.keys())}")

    def disconnect(self, ip: str):
        with self._devices_lock:
            if ip in self.devices:
                conn = self.devices[ip].get("connection")
                if conn: conn.close()
                del self.devices[ip]
                if ip in state["connected_devices"]: del state["connected_devices"][ip]
                self.log_advanced(f"Luminaire disconnected: {ip}")
            logging.info(f"Disconnected {ip}")
            logging.debug(f"Device list after disconnect: {list(self.devices.keys())}")

    def clearALL(self):
        with self._devices_lock:
            for ip in list(self.devices.keys()): self.disconnect(ip)
            state["connected_devices"] = {}
        self.log_advanced("All luminaires disconnected.")
        logging.info("All luminaires disconnected.")
        logging.debug("Device list cleared")

    def processACK(self, ip: str, response: str) -> bool:
        logging.debug(f"Processing ACK from {ip}: {response}")
        try:
            if isinstance(response, bytes): response = response.decode('utf-8', errors='ignore')
            match = re.match(r"\*(\d{3})(\d{3})100ACK(\d{3})(\d{3})#", response)
            if not match: 
                logging.warning(f"Invalid ACK format from {ip}: {response}")
                return False
            cw_raw, ww_raw = match.group(3), match.group(4)
            cw, ww = int(cw_raw) / 10, int(ww_raw) / 10
            with self._devices_lock:
                if ip in self.devices:
                    self.update_cw_ww_intensity(ip, cw, ww)
                    self.log_advanced(f"Received [{ip}]: {response}")
                    state["cw"] = cw
                    state["ww"] = ww
                    state["current_cct"] = self.calculate_cct_from_cw_ww(cw, ww)
                    logging.debug(f"Updated state - CW: {cw}%, WW: {ww}%, CCT: {state['current_cct']}K")
                    return True
            return False
        except Exception as e:
            self.log_advanced(f"Error processing ACK for {ip}: {e}")
            logging.error(f"Error processing ACK for {ip}: {e}", exc_info=True)
            return False

    def log_basic(self, message: str):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        state["basicLogs"].append(f"[{timestamp}] {message}")
        state["basicLogs"] = state["basicLogs"][-50:]
        logging.info(f"Basic Log: {message}")

    def log_advanced(self, message: str):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        state["advancedLogs"].append(f"[{timestamp}] {message}")
        state["advancedLogs"] = state["advancedLogs"][-100:]
        logging.debug(f"Advanced Log: {message}")

    def list(self) -> dict:
        logging.debug("Listing connected devices")
        with self._devices_lock:
            now = time.time()
            devices = {ip: {"cw": data.get("cw"), "ww": data.get("ww")} for ip, data in self.devices.items() if now - data["last_seen"] < self.INACTIVITY_THRESHOLD}
            logging.debug(f"Active devices: {list(devices.keys())}")
            return devices

    def update_cw_ww_intensity(self, ip: str, cw: float, ww: float):
        with self._devices_lock:
            if ip in self.devices:
                self.devices[ip].update({"cw": cw, "ww": ww, "last_seen": time.time()})
                state["connected_devices"][ip] = {"cw": cw, "ww": ww}
                logging.debug(f"Updated {ip} - CW: {cw}%, WW: {ww}%")

    def send(self, ip: str, cw: float, ww: float, max_retries: int = 3, timeout: float = 2.0) -> bool:
        logging.debug(f"Sending to {ip} - CW: {cw}%, WW: {ww}%")
        retries = 0
        with self._devices_lock:
            if ip not in self.devices: 
                logging.warning(f"Device {ip} not found for sending")
                return False
        while retries < max_retries:
            try:
                command = self.buildCommand(ip, cw, ww)
                self.devices[ip]["connection"].settimeout(timeout)
                self.devices[ip]["connection"].sendall(command.encode())
                self.log_advanced(f"Sent [{ip}]: {command}")
                logging.debug(f"Successfully sent to {ip}")
                return True
            except socket.timeout:
                retries += 1
                self.log_advanced(f"Timeout sending to {ip} (retry {retries}/{max_retries})")
                logging.warning(f"Timeout sending to {ip} (retry {retries}/{max_retries})")
            except socket.error as e:
                retries += 1
                self.log_advanced(f"Error sending to {ip} (retry {retries}/{max_retries}): {e}")
                logging.warning(f"Error sending to {ip} (retry {retries}/{max_retries}): {e}")
                if retries >= max_retries: self.disconnect(ip)
            time.sleep(0.5)
        logging.error(f"Failed to send to {ip} after {max_retries} retries")
        return False

    def sendAll(self, cw: float, ww: float) -> tuple[bool, list]:
        logging.debug(f"Sending to all devices - CW: {cw}%, WW: {ww}%")
        with self._devices_lock:
            ips = list(self.devices.keys())
        if not ips: 
            logging.warning("No devices available to send to")
            return False, []
        failed_ips = []
        with ThreadPoolExecutor(max_workers=min(7, len(ips))) as executor:
            futures = {executor.submit(self.send, ip, cw, ww): ip for ip in ips}
            for future in futures: future.result() or failed_ips.append(futures[future])
        success = len(failed_ips) == 0
        if not success:
            self.log_advanced(f"Failed to send to luminaires: {', '.join(failed_ips)}")
            state["alert"] = f"Failed to send to luminaires: {', '.join(failed_ips)}"
        state["current_cct"] = self.calculate_cct_from_cw_ww(cw, ww)
        logging.debug(f"SendAll completed - Success: {success}, Failed IPs: {failed_ips}")
        return success, failed_ips

    def calculate_cw_ww_from_cct_intensity(self, cct: float, intensity: float) -> tuple[float, float]:
        logging.debug(f"Calculating CW/WW from CCT: {cct}, Intensity: {intensity}")
        cct = max(self.min_cct, min(self.max_cct, cct))
        intensity = max(self.min_intensity, min(self.max_intensity, intensity))
        intensity_percent = intensity / 500.0
        cw_base = (cct - self.min_cct) / ((self.max_cct - self.min_cct) / 100.0)
        ww_base = 100.0 - cw_base
        cw = max(0.0, min(99.99, cw_base * intensity_percent))
        ww = max(0.0, min(99.99, ww_base * intensity_percent))
        logging.debug(f"Calculated - CW: {cw}%, WW: {ww}%")
        return cw, ww

    def calculate_cct_from_cw_ww(self, cw: float, ww: float) -> float:
        logging.debug(f"Calculating CCT from CW: {cw}%, WW: {ww}%")
        total = cw + ww
        cct = 3500 if total == 0 else self.min_cct + ((cw / total) * 100 * ((self.max_cct - self.min_cct) / 100.0))
        logging.debug(f"Calculated CCT: {cct}K")
        return cct

    def buildCommand(self, ip: str, cw: float, ww: float) -> str:
        logging.debug(f"Building command for {ip} - CW: {cw}%, WW: {ww}%")
        try:
            ip_parts = ip.split(".")
            ip3, ip4 = f"{int(ip_parts[2]):03}", f"{int(ip_parts[3]):03}"
            command = f"*{ip3}{ip4}{int(cw*10):03}{int(ww*10):03}##"
            logging.debug(f"Built command: {command}")
            return command
        except (ValueError, IndexError) as e:
            self.log_advanced(f"Error building command for {ip}: {e}")
            logging.error(f"Error building command for {ip}: {e}", exc_info=True)
            raise ValueError(f"Invalid IP: {ip}")

    def get_nearest_interval(self, current_time):
        logging.debug(f"Getting nearest interval for time: {current_time}")
        seconds_since_midnight = (current_time.hour * 3600) + (current_time.minute * 60) + current_time.second
        logging.debug(f"Nearest interval: {seconds_since_midnight}")
        return seconds_since_midnight

    def determine_phase(self, interval):
        logging.debug(f"Determining phase for interval: {interval}")
        if 0 <= interval <= 17: phase = "Morning"
        elif 18 <= interval <= 29: phase = "Midday"
        elif 30 <= interval <= 41: phase = "Evening"
        else: phase = "Night"
        logging.debug(f"Determined phase: {phase}")
        return phase

    def run_smooth_scheduler(self, csv_path: str):
        logging.debug(f"Starting scheduler for {csv_path}")
        self.stop_event.clear()
        self.paused = False
        state["scheduler"]["status"] = "running"
        now = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_basic(f"Activated scene: {os.path.basename(csv_path)}")
        try:
            with open(csv_path, newline='') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                scene_data_list = []
                for row in reader:
                    time_str, cct, intensity = row[0].strip(), float(row[1]), float(row[2])
                    hour, minute = map(int, time_str.split(':'))
                    total_minutes = hour * 60 + minute
                    scene_data_list.append((total_minutes, cct, intensity))
                self.total_intervals = len(scene_data_list)
                state["scheduler"]["total_intervals"] = 8640

                full_cct_data = []
                full_intensity_data = []
                for i in range(self.total_intervals):
                    start_min, start_cct, start_intensity = scene_data_list[i]
                    next_idx = (i + 1) % self.total_intervals
                    end_min, end_cct, end_intensity = scene_data_list[next_idx]
                    time_diff = ((end_min - start_min + 1440) % 1440) * 60
                    cct_diff = end_cct - start_cct
                    intensity_diff = end_intensity - start_intensity
                    for j in range(1800):
                        t = j / 1799
                        interpolated_cct = start_cct + (cct_diff * t)
                        interpolated_intensity = start_intensity + (intensity_diff * t)
                        full_cct_data.append(interpolated_cct)
                        full_intensity_data.append(interpolated_intensity)

                state["scene_data"]["cct"] = full_cct_data
                state["scene_data"]["intensity"] = full_intensity_data

                self.current_interval_index = self.get_nearest_interval(datetime.datetime.now())
                self.start_time = time.time()
                logging.debug(f"Scheduler started at index: {self.current_interval_index}, start_time: {self.start_time}")

                last_update_time = self.start_time
                last_interval_update = self.current_interval_index

                while not self.stop_event.is_set() and self.current_interval_index < 86400:
                    current_time = time.time()
                    elapsed = current_time - self.start_time

                    if self.paused:
                        time.sleep(0.1)
                        logging.debug("Scheduler paused")
                        continue

                    if elapsed >= 1.0:
                        current_idx = int(self.current_interval_index)
                        current_interval = (current_idx // 1800) % self.total_intervals
                        next_interval = (current_interval + 1) % self.total_intervals
                        interval_progress = (current_idx % 1800) / 1799

                        start_min, start_cct, start_intensity = scene_data_list[current_interval]
                        end_min, end_cct, end_intensity = scene_data_list[next_interval]
                        time_diff = ((end_min - start_min + 1440) % 1440) * 60
                        cct_diff = end_cct - start_cct
                        intensity_diff = end_intensity - start_intensity

                        state["current_cct"] = start_cct + (cct_diff * interval_progress)
                        state["current_intensity"] = start_intensity + (intensity_diff * interval_progress)
                        cw, ww = self.calculate_cw_ww_from_cct_intensity(state["current_cct"], state["current_intensity"])
                        state["cw"], state["ww"] = cw, ww

                        success, failed_ips = self.sendAll(cw, ww)
                        if not success:
                            state["alert"] = f"Failed to send to luminaires: {', '.join(failed_ips)}"
                            logging.warning(f"SendAll failed for IPs: {failed_ips}")

                        if current_idx % 10 == 0 and current_idx != last_interval_update:
                            state["scheduler"]["current_interval"] = current_idx // 10
                            state["scheduler"]["interval_progress"] = (current_idx / 86400) * 100
                            interval = current_idx // (180 * 10)
                            state["currentPhase"] = self.determine_phase(interval)
                            last_interval_update = current_idx
                            logging.info(f"Interval update - Index: {current_idx}, Progress: {state['scheduler']['interval_progress']}%, Phase: {state['currentPhase']}")

                        logging.info(f"Elapsed: {elapsed:.1f}, Index: {current_idx}, Interval: {current_interval}, "
                                   f"Progress: {interval_progress:.2f}, CCT: {state['current_cct']:.1f}K, "
                                   f"Intensity: {state['current_intensity']:.1f}lux, CW: {cw:.1f}%, WW: {ww:.1f}%")
                        self.current_interval_index = (self.current_interval_index + 1) % 86400
                        self.start_time += 1.0
                        last_update_time = current_time

                if not self.stop_event.is_set():
                    state["scheduler"]["status"] = "completed"
                    now = datetime.datetime.now().strftime("%H:%M:%S")
                    self.log_basic(f"Scene completed: {os.path.basename(csv_path)}")
                    logging.info("Scene execution completed successfully!")
                    logging.debug("Scheduler loop completed")
        except FileNotFoundError:
            self.log_advanced(f"CSV file not found: {csv_path}")
            logging.error(f"CSV file not found: {csv_path}", exc_info=True)
            state["scheduler"]["status"] = "failed"
        except Exception as e:
            self.log_advanced(f"Error running scheduler: {e}")
            logging.error(f"Error running scheduler: {e}", exc_info=True)
            state["scheduler"]["status"] = "failed"
        finally:
            logging.debug(f"Scheduler for {csv_path} terminated")

    def pause_scheduler(self):
        self.paused = True
        self.log_basic("Scheduler paused")
        logging.info("Scheduler paused")

    def resume_scheduler(self):
        self.paused = False
        self.start_time = time.time() - (self.current_interval_index % 86400)
        self.log_basic("Scheduler resumed")
        logging.info("Scheduler resumed")
        logging.debug(f"Resumed at index: {self.current_interval_index}, start_time: {self.start_time}")

    def adjust_cw(self, delta: float, ip: str = None) -> bool:
        logging.debug(f"Adjusting CW by {delta} for IP: {ip}")
        if ip:
            with self._devices_lock:
                if ip not in self.devices or self.devices[ip].get("cw") is None: 
                    logging.warning(f"Device {ip} not found or no CW data")
                    return False
                current_cw = self.devices[ip]["cw"]
                new_cw = max(0.0, min(100.0, current_cw + delta))
                self.log_basic(f"Adjusted CW to {new_cw}%")
                return self.send(ip, new_cw, 100 - new_cw)
        else:
            with self._devices_lock:
                device_with_cw = next((ip for ip, data in self.devices.items() if data.get("cw") is None), None)
                if not device_with_cw: 
                    logging.warning("No device with CW data found")
                    return False
                current_cw = self.devices[device_with_cw]["cw"]
            new_cw = max(0.0, min(100.0, current_cw + delta))
            self.log_basic(f"Adjusted CW to {new_cw}%")
            return self.sendAll(new_cw, 100 - new_cw)[0]

class LuminaireServer:
    def __init__(self, host=CONFIG["host"], port=CONFIG["tcp_port"], luminaire_ops=None):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.luminaire_ops = luminaire_ops
        self._lock = threading.Lock()
        logging.debug("LuminaireServer initialized")

    async def emit_status_update(self, websocket):
        logging.debug("Emitting status update")
        await websocket.send(json.dumps(state))

    async def stream_logs(self, websocket):
        logging.debug("Starting log streaming")
        while True:
            if websocket.open and (state["basicLogs"] or state["advancedLogs"]):
                await websocket.send(json.dumps({
                    "type": "log_update",
                    "basicLogs": state["basicLogs"],
                    "advancedLogs": state["advancedLogs"]
                }))
                logging.debug("Logs streamed to client")
            await asyncio.sleep(1.0)

    async def broadcast_system_stats(self):
        logging.debug("Starting system stats broadcast")
        while True:
            cpu_percent, mem_percent = self.luminaire_ops.get_system_stats()
            state["cpu_percent"] = cpu_percent
            state["mem_percent"] = mem_percent
            update = {
                "type": "system_stats",
                "cpu_percent": cpu_percent,
                "mem_percent": mem_percent,
            }
            for client in list(clients):
                if not client.close:
                    await client.send(json.dumps(update))
                    logging.debug(f"Broadcasted stats to {client.remote_address}")
            await asyncio.sleep(1.0)

    async def broadcast_live_updates(self):
        logging.debug("Starting live updates broadcast")
        while True:
            if state["scheduler"]["status"] == "running":
                live_update = {
                    "type": "live_update",
                    "current_cct": state["current_cct"],
                    "current_intensity": state["current_intensity"],
                    "cw": state["cw"],
                    "ww": state["ww"],
                    "currentPhase": state["currentPhase"],
                    "cpu_percent": state["cpu_percent"],
                    "mem_percent": state["mem_percent"],
                    "interval_progress": state["scheduler"]["interval_progress"]
                }
                if clients:
                    for client in list(clients):
                        if not client.close:
                            await client.send(json.dumps(live_update))
                            logging.debug(f"Sent live update to {client.remote_address}")
            await asyncio.sleep(0.2)

    def start(self):
        logging.debug(f"Starting server on {self.host}:{self.port}")
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            threading.Thread(target=self.accept_connections, daemon=True).start()
            logging.info(f"Luminaire Server started on {self.host}:{self.port}")
        except Exception as e:
            logging.error(f"Failed to start server: {e}", exc_info=True)
            self._cleanup_socket()
            raise

    def accept_connections(self):
        logging.debug("Accepting connections")
        while self.running:
            try:
                client_socket, (client_ip, _) = self.server_socket.accept()
                logging.info(f"New luminaire connected: {client_ip}")
                with self._lock:
                    if self.luminaire_ops: self.luminaire_ops.add(client_ip, client_socket)
                threading.Thread(target=self.handle_client, args=(client_ip, client_socket), daemon=True).start()
            except socket.timeout: 
                logging.debug("Socket timeout during accept")
                continue
            except Exception as e:
                if self.running: logging.error(f"Critical error accepting connection: {e}", exc_info=True)

    def handle_client(self, ip: str, client_socket: socket.socket):
        logging.debug(f"Handling client {ip}")
        try:
            while self.running:
                data = client_socket.recv(1024)
                if not data: break
                logging.info(f"Received from {ip}: {data.decode()}")
                with self._lock:
                    if self.luminaire_ops: self.luminaire_ops.processACK(ip, data)
        except (ConnectionResetError, OSError) as e:
            logging.warning(f"Luminaire {ip} disconnected unexpectedly: {e}")
        except Exception as e:
            logging.error(f"Unexpected error handling {ip}: {e}", exc_info=True)
        finally:
            with self._lock:
                if self.luminaire_ops: self.luminaire_ops.disconnect(ip)
            logging.debug(f"Client {ip} handling terminated")

    def shutdown(self):
        logging.debug("Shutting down server")
        self.running = False
        with self._lock:
            if self.luminaire_ops: self.luminaire_ops.clearALL()
        self._cleanup_socket()
        logging.info("Luminaire Server shut down.")

    def _cleanup_socket(self):
        if self.server_socket: self.server_socket.close()
        logging.debug("Socket cleaned up")

    async def status_loop(self):
        logging.debug("Starting status loop")
        while True:
            state["connected_devices"] = self.luminaire_ops.list()
            logging.debug(f"Updated connected devices: {list(state['connected_devices'].keys())}")
            await asyncio.sleep(10.0)

def load_scenes(luminaire_ops):
    logging.debug("Loading scenes")
    scene_dir = "scenes"
    if not os.path.exists(scene_dir): os.makedirs(scene_dir)
    state["available_scenes"] = [f for f in os.listdir(scene_dir) if f.endswith('.csv')]
    for scene in state["available_scenes"]:
        try:
            with open(os.path.join(scene_dir, scene), newline='') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                scene_data_list = [(int(row[0].split(':')[0]) * 60 + int(row[0].split(':')[1]), float(row[1]), float(row[2])) for row in reader]
                full_cct_data = []
                full_intensity_data = []
                for i in range(len(scene_data_list)):
                    start_min, start_cct, start_intensity = scene_data_list[i]
                    end_min, end_cct, end_intensity = scene_data_list[(i + 1) % len(scene_data_list)]
                    time_diff = ((end_min - start_min + 1440) % 1440) * 60
                    cct_diff = end_cct - start_cct
                    intensity_diff = end_intensity - start_intensity
                    for j in range(1800):
                        t = j / 1799
                        interpolated_cct = start_cct + (cct_diff * t)
                        interpolated_intensity = start_intensity + (intensity_diff * t)
                        full_cct_data.append(interpolated_cct)
                        full_intensity_data.append(interpolated_intensity)
                scene_data[scene] = {
                    "cct": full_cct_data,
                    "intensity": full_intensity_data
                }
            logging.info(f"Loaded scene {scene}.")
            logging.debug(f"Scene data for {scene}: {len(full_cct_data)} CCT points, {len(full_intensity_data)} intensity points")
        except Exception as e:
            logging.error(f"Error loading scene {scene}: {e}", exc_info=True)

async def websocket_handler(websocket, path=None):
    logging.debug(f"New WebSocket connection from {websocket.remote_address}")
    clients.add(websocket)
    logging.info(f"New WebSocket client connected from {websocket.remote_address}")
    ops.log_advanced("WebSocket connected")
    try:
        await emit_status_update(websocket)
        async for message in websocket:
            data = json.loads(message)
            logging.debug(f"Received message: {data}")
            action = data.get("type")
            now = datetime.datetime.now().strftime("%H:%M:%S")
            if action == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
                logging.debug("Sent pong response")
            elif action == "set_mode":
                state["auto_mode"] = data["auto"]
                ops.stop_event.set() if not data["auto"] else ops.stop_event.clear()
                ops.log_basic(f"Switched to {'Auto' if data['auto'] else 'Manual'} mode")
                if not data["auto"]:
                    state["scene_data"] = {"cct": [], "intensity": []}
                    state["current_scene"] = None
                    state["loaded_scene"] = None
                    state["scheduler"]["status"] = "idle"
                load_scenes(ops)
                logging.debug(f"Mode set to {'auto' if data['auto'] else 'manual'}")
            elif action == "load_scene":
                state["loaded_scene"] = data["scene"]
                if data["scene"] in scene_data:
                    state["scene_data"] = scene_data[data["scene"]]
                ops.log_basic(f"Loaded scene: {data['scene']}")
                logging.debug(f"Loaded scene data: {state['scene_data'].keys()}")
            elif action == "activate_scene":
                state["current_scene"] = data["scene"]
                if state["auto_mode"] and data["scene"] in scene_data:
                    state["scene_data"] = scene_data[data["scene"]]
                    state["activationTime"] = now
                    threading.Thread(target=ops.run_smooth_scheduler, args=(os.path.join("scenes", data["scene"]),), daemon=True).start()
                logging.debug(f"Activated scene: {data['scene']}")
            elif action == "stop_scheduler":
                ops.stop_event.set()
                ops.log_basic("Scheduler stopped")
                state["scene_data"] = {"cct": [], "intensity": []}
                state["current_scene"] = None
                state["loaded_scene"] = None
                state["scheduler"]["status"] = "idle"
                logging.debug("Scheduler stopped")
            elif action == "manual_override":
                state["is_manual_override"] = data.get("override", False)
                if not state["is_manual_override"] and state["auto_mode"] and state["current_scene"]:
                    ops.start_time = None
                    threading.Thread(target=ops.run_smooth_scheduler, args=(os.path.join("scenes", state["current_scene"]),), daemon=True).start()
                ops.log_basic(f"Manual override {'enabled' if data.get('override', False) else 'disabled'}")
                logging.debug(f"Manual override set to {state['is_manual_override']}")
            elif action == "pause_resume":
                if data.get("pause", False):
                    ops.pause_scheduler()
                else:
                    ops.resume_scheduler()
                logging.debug(f"Scheduler {'paused' if data.get('pause', False) else 'resumed'}")
            elif action == "adjust_light":
                light_type, delta = data["light_type"], data["delta"]
                if light_type == "cw":
                    ops.adjust_cw(delta * 1.0)
                    state["cw"] = min(100, max(0, (state["cw"] or 50) + delta))
                    state["ww"] = 100 - state["cw"]
                elif light_type == "ww":
                    ops.adjust_cw(-delta * 1.0)
                    state["ww"] = min(100, max(0, (state["ww"] or 50) + delta))
                    state["cw"] = 100 - state["ww"]
                state["current_cct"] = ops.calculate_cct_from_cw_ww(state["cw"], state["ww"])
                ops.log_basic(f"Adjusted {light_type.upper()} by {delta}%")
                logging.debug(f"Adjusted {light_type} by {delta}%, New CW: {state['cw']}, WW: {state['ww']}")
            elif action == "sendAll":
                cw, ww, intensity = data["cw"], data["ww"], data["intensity"]
                state["cw"], state["ww"], state["current_intensity"] = cw, ww, intensity
                state["current_cct"] = ops.calculate_cct_from_cw_ww(cw, ww)
                success, failed_ips = ops.sendAll(cw, ww)
                if not success: state["alert"] = f"Failed to send to luminaires: {', '.join(failed_ips)}"
                logging.debug(f"SendAll - Success: {success}, Failed IPs: {failed_ips}")
            elif action == "set_cct":
                state["current_cct"] = data["cct"]
                cw, ww = ops.calculate_cw_ww_from_cct_intensity(state["current_cct"], state["current_intensity"])
                state["cw"], state["ww"] = cw, ww
                success, failed_ips = ops.sendAll(cw, ww)
                if not success: state["alert"] = f"Failed to send to luminaires: {', '.join(failed_ips)}"
                ops.log_basic(f"Set CCT to {data['cct']}K")
                logging.debug(f"Set CCT to {data['cct']}K, CW: {cw}, WW: {ww}")
            elif action == "toggle_system":
                state["isSystemOn"] = data["isSystemOn"]
                ops.log_basic(f"System turned {'ON' if data['isSystemOn'] else 'OFF'}")
                logging.debug(f"System toggled to {'ON' if data['isSystemOn'] else 'OFF'}")
            state["basicLogs"] = state["basicLogs"][-50:]
            state["advancedLogs"] = state["advancedLogs"][-100:]
            for client in clients: await emit_status_update(client)
            logging.debug("State updated and broadcasted to clients")
        asyncio.create_task(server.stream_logs(websocket))
    except Exception as e:
        ops.log_advanced(f"WebSocket handler error: {e}")
        logging.error(f"WebSocket handler error: {e}", exc_info=True)
    finally:
        clients.remove(websocket)
        ops.log_advanced("WebSocket disconnected")
        logging.info(f"WebSocket client disconnected from {websocket.remote_address}")
        logging.debug("Client removed from clients set")

async def emit_status_update(websocket):
    await websocket.send(json.dumps(state))
    logging.debug("Status update emitted")

async def main():
    global ops, server
    ops = LuminaireOperations()
    server = LuminaireServer(luminaire_ops=ops)
    server.start()
    load_scenes(ops)
    websocket_server = await websockets.serve(websocket_handler, "127.0.0.1", 5000)
    print(f"WebSocket server running on ws://localhost:5000")
    logging.info("WebSocket server started")
    asyncio.create_task(server.broadcast_system_stats())
    asyncio.create_task(server.broadcast_live_updates())
    await server.status_loop()

if __name__ == "__main__":
    asyncio.run(main())
