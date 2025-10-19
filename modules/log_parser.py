import re
from time import sleep
from os import stat
from threading import Thread
from fuzzywuzzy import fuzz

class LogParser():
    """Parses the game.log file for Star Citizen."""
    def __init__(self, gui_module, api_client_module, sound_module, cm_module, local_version, monitoring, discord_id, rsi_handle, player_geid, active_ship, anonymize_state):
        self.log = None
        self.gui = gui_module
        self.api = api_client_module
        self.sounds = sound_module
        self.cm = cm_module
        self.local_version = local_version
        self.monitoring = monitoring
        self.discord_id = discord_id
        self.rsi_handle = rsi_handle
        self.active_ship = active_ship
        if not self.active_ship.get("current"):
            self.active_ship["current"] = "FPS"
        self.active_ship_id = "N/A"
        self.anonymize_state = anonymize_state
        self.game_mode = "Nothing"
        self.active_ship_id = "N/A"
        self.player_geid = player_geid
        self.log_file_location = None
        self.curr_killstreak = 0
        self.max_killstreak = 0
        self.kill_total = 0
        self.death_total = 0
        
        self.global_ship_list = [
            'DRAK', 'ORIG', 'AEGS', 'ANVL', 'CRUS', 'BANU', 'MISC',
            'KRIG', 'XNAA', 'ARGO', 'VNCL', 'ESPR', 'RSI', 'CNOU',
            'GRIN', 'TMBL', 'GAMA'
        ]

    def start_tail_log_thread(self) -> None:
        """Start the log tailing in a separate thread only if it's not already running."""
        thr = Thread(target=self.tail_log, daemon=True)
        thr.start()

    def tail_log(self) -> None:
        """Read the log file and display events in the GUI."""
        try:
            sc_log = open(self.log_file_location, "r")
            if sc_log is None:
                self.log.error(f"No log file found at {self.log_file_location}")
                return
        except Exception as e:
            self.log.error(f"Error opening log file: {e.__class__.__name__} {e}")
        try:
            self.log.warning("");
            self.log.warning("Enter or Load 'SC Kill-Tracker API Key' to establish BWC GrimReaperBot connection...")
            self.log.warning("");
            sleep(1)
            while self.monitoring["active"]:
                # Block loop until API key is valid
                if self.api.api_key["value"]:
                    break
                sleep(1)
            self.log.debug(f"tail_log(): Received key: {self.api.api_key}. Moving on...")
        except Exception as e:
            self.log.error(f"Error waiting for GrimReaperBot connection to be established: {e.__class__.__name__} {e}")

        try:
            # Read all lines to find out what game mode player is currently, in case they booted up late.
            # Don't upload kills, we don't want repeating last session's kills in case they are actually available.
            self.log.info("Loading old log (if available)! Note that old kills shown will not be uploaded.")
            lines = sc_log.readlines()
            for line in lines:
                if not self.api.api_key["value"]:
                    self.log.error("Error: key is invalid. Loading old log stopped.")
                    break
                self.read_log_line(line, False)
            # After loading old log, always default to FPS on the label
            self.active_ship["current"] = "FPS"
            self.active_ship_id = "N/A"
            self.gui.update_vehicle_status("FPS")
        except Exception as e:
            self.log.error(f"Error reading old log file: {e.__class__.__name__} {e}")
        
        try:
            # Main loop to monitor the log
            last_log_file_size = stat(self.log_file_location).st_size
            self.log.debug(f"tail_log(): Last log size: {last_log_file_size}.")
            self.log.success(f"Kill tracking initiated with Discord ID: {self.discord_id['current']}")
        except Exception as e:
            self.log.error(f"Error getting log file size: {e.__class__.__name__} {e}")
        
        while self.monitoring["active"]:
            try:
                if not self.api.api_key["value"]:
                    self.log.error("Error: key is invalid. Kill Tracking is not active...")
                    sleep(5)
                    continue
                # Handle RSI handle first before continuing
                if self.rsi_handle["current"] == "N/A":
                    self.log.error(f"RSI handle name has not been found yet. Retrying ...")
                    self.rsi_handle["current"] = self.find_rsi_handle()
                    if self.rsi_handle["current"] != "N/A":
                        self.log.success(f"Refound RSI handle name: {self.rsi_handle['current']}.")
                where = sc_log.tell()
                line = sc_log.readline()
                if not line:
                    sleep(1)
                    sc_log.seek(where)
                    if last_log_file_size > stat(self.log_file_location).st_size:
                        sc_log.close()
                        sc_log = open(self.log_file_location, "r")
                        last_log_file_size = stat(self.log_file_location).st_size
                else:
                    self.read_log_line(line, True)
            except Exception as e:
                self.log.error(f"Error reading game log file: {e.__class__.__name__} {e}")
        self.log.info("Game log monitoring has stopped.")

    def _extract_ship_info(self, line):
        match = re.search(r"for '([\w]+(?:_[\w]+)+)_(\d+)'", line)
        if match:
            ship_type = match.group(1)
            ship_id = match.group(2)
            return {"ship_type": ship_type, "ship_id": ship_id}
        return None

    def read_log_line(self, line: str, upload_kills: bool) -> None:
        if upload_kills and "<Vehicle Control Flow>" in line:
                if (
                    ("CVehicleMovementBase::SetDriver:" in line and "requesting control token for" in line) or
                    ("CVehicle::Initialize::<lambda_1>::operator ():" in line and "granted control token for" in line)
                ):
                    ship_data = self._extract_ship_info(line)
                    if ship_data:
                        self.active_ship["current"] = ship_data["ship_type"]
                        self.active_ship_id = ship_data["ship_id"]
                        self.log.info(f"Entered ship: {self.active_ship['current']} (ID: {self.active_ship_id})")
                        self.gui.update_vehicle_status(self.active_ship["current"])
                    return
                if (
                    ("CVehicleMovementBase::ClearDriver:" in line and "releasing control token for" in line) or
                    ("losing control token for" in line)
                ):
                    self.active_ship["current"] = "FPS"
                    self.active_ship_id = "N/A"
                    self.log.info("Exited ship: Defaulted to FPS (on-foot)")
                    self.gui.update_vehicle_status("FPS")
                    return
                
        if "<Context Establisher Done>" in line:
            self.set_game_mode(line)
            self.log.debug(f"read_log_line(): set_game_mode with: {line}.")
        elif "CPlayerShipRespawnManager::OnVehicleSpawned" in line and (
                "SC_Default" != self.game_mode) and (self.player_geid["current"] in line):
            self.set_ac_ship(line)
            self.log.debug(f"read_log_line(): set_ac_ship with: {line}.")
        elif ("<Vehicle Destruction>" in line or
            "<local client>: Entering control state dead" in line) and (
                self.active_ship_id in line):
            self.log.debug(f"read_log_line(): destroy_player_zone with: {line}")
            self.destroy_player_zone()
        elif self.rsi_handle["current"] in line:
            if "OnEntityEnterZone" in line:
                self.log.debug(f"read_log_line(): set_player_zone with: {line}.")
                self.set_player_zone(line, False)
            if "CActor::Kill" in line and upload_kills:
                kill_result = self.parse_kill_line(line)
                self.log.debug(f"read_log_line(): kill_result with: {line}.")
                # Do not send
                if kill_result["result"] == "exclusion" or kill_result["result"] == "reset":
                    self.log.debug(f"read_log_line(): Not posting {kill_result['result']} death: {line}.")
                    return
                # Log a message for the current user's death
                elif kill_result["result"] == "killed" or kill_result["result"] == "suicide":
                    self.curr_killstreak = 0
                    self.gui.curr_killstreak_label.config(text=f"Current Killstreak: {self.curr_killstreak}", fg="yellow")
                    self.death_total += 1
                    self.gui.session_deaths_label.config(text=f"Total Session Deaths: {self.death_total}", fg="red")
                    weapon_human_readable = self.convert_string(self.api.sc_data["weapons"], kill_result["data"]["weapon"])
                    if kill_result["result"] == "killed":
                        self.log.info(f'☠ You were killed by {kill_result["data"]["player"]} with {weapon_human_readable}.')
                    elif kill_result["result"] == "suicide":
                        if kill_result["data"]["weapon"] == kill_result["data"]["victim"]:
                            self.log.info('☠ You died via backspace')
                        else:
                            self.log.info(f'☠ You died from {weapon_human_readable}.')
                    # Send death-event to the server via heartbeat
                    #self.cm.post_heartbeat_event(kill_result["data"]["victim"], kill_result["data"]["zone"], None)
                    self.destroy_player_zone()
                    self.update_kd_ratio()
                    self.api.post_kill_event(kill_result)
                # Log a message for the current user's kill
                elif kill_result["result"] == "killer":
                    self.curr_killstreak += 1
                    if self.curr_killstreak > self.max_killstreak:
                        self.max_killstreak = self.curr_killstreak
                    self.kill_total += 1
                    self.gui.curr_killstreak_label.config(text=f"Current Killstreak: {self.curr_killstreak}", fg="#04B431")
                    self.gui.max_killstreak_label.config(text=f"Max Killstreak: {self.max_killstreak}", fg="#04B431")
                    self.gui.session_kills_label.config(text=f"Total Session Kills: {self.kill_total}", fg="#04B431")
                    weapon_human_readable = self.convert_string(self.api.sc_data["weapons"], kill_result["data"]["weapon"])
                    self.log.info(f"🔫 You have killed {kill_result['data']['victim']} with {weapon_human_readable}")
                    self.sounds.play_random_sound()
                    self.update_kd_ratio()
                    self.api.post_kill_event(kill_result)
                else:
                    self.log.error(f"Kill failed to parse with result {kill_result['result']} RAW LINE: {line}.")
        elif "<Jump Drive State Changed>" in line:
            self.log.debug(f"read_log_line(): set_player_zone with: {line}.")
            self.set_player_zone(line, True)

    def set_game_mode(self, line:str) -> None:
        """Parse log for current active game mode."""
        split_line = line.split(' ')
        curr_game_mode = split_line[8].split("=")[1].strip("\"")
        if self.game_mode != curr_game_mode:
            self.game_mode = curr_game_mode
        if "SC_Default" == curr_game_mode:
            self.active_ship["current"] = "FPS"
            self.active_ship_id = "N/A"
            self.gui.update_vehicle_status("FPS")

    def set_ac_ship(self, line:str) -> None:
        """Parse log for current active ship."""
        self.active_ship["current"] = line.split(' ')[5][1:-1]
        self.log.debug(f"Player has entered ship: {self.active_ship['current']}")
        self.gui.update_vehicle_status(self.active_ship["current"])

    def destroy_player_zone(self) -> None:
        self.log.debug(f"Ship Destroyed: {self.active_ship['current']} with ID: {self.active_ship_id}")
        self.active_ship["current"] = "FPS"
        self.active_ship_id = "N/A"
        self.gui.update_vehicle_status("FPS")

    def set_player_zone(self, line: str, use_jd) -> None:
        """Set current active ship zone."""
        if not use_jd:
            line_index = line.index("-> Entity ") + len("-> Entity ")
        else:
            line_index = line.index("adam: ") + len("adam: ")
        if 0 == line_index:
            self.log.debug(f"Active Zone Change: {self.active_ship['current']}")
            self.active_ship["current"] = "FPS"
            self.gui.update_vehicle_status("FPS")
            return
        if not use_jd:
            potential_zone = line[line_index:].split(' ')[0]
            potential_zone = potential_zone[1:-1]
        else:
            potential_zone = line[line_index:].split(' ')[0]
        for x in self.global_ship_list:
            if potential_zone.startswith(x):
                self.active_ship["current"] = potential_zone[:potential_zone.rindex('_')]
                self.active_ship_id = potential_zone[potential_zone.rindex('_') + 1:]
                self.log.debug(f"Active Zone Change: {self.active_ship['current']} with ID: {self.active_ship_id}")
                #self.cm.post_heartbeat_event(None, None, self.active_ship["current"])
                self.gui.update_vehicle_status(self.active_ship["current"])
                return

    def check_exclusion_scenarios(self, line:str) -> bool:
        """Check for kill edgecase scenarios."""
        if self.game_mode == "EA_FreeFlight":
            if "Crash" in line:
                self.log.info("Probably a ship reset, ignoring kill!")
                return False
            if "SelfDestruct" in line:
                self.log.info("Self-destruct detected in Free Flight, ignoring kill!")
                return False

        elif self.game_mode == "EA_SquadronBattle":
            # Add your specific conditions for Squadron Battle mode
            if "Crash" in line:
                self.log.info("Crash detected in Squadron Battle, ignoring kill!")
                return False
            if "SelfDestruct" in line:
                self.log.info("Self-destruct detected in Squadron Battle, ignoring kill!")
                return False
        return True

    def parse_kill_line(self, line:str):
        """Parse kill event."""
        try:
            kill_result = {"result": "", "data": {}}

            if not self.check_exclusion_scenarios(line):
                kill_result["result"] = "exclusion"
                return kill_result
            
            split_line = line.split(' ')

            kill_time = split_line[0].strip('\'')
            killed = split_line[5].strip('\'')

            zone = split_line[9].strip('\'')

            killer = split_line[12].strip('\'')
            
            weapon = split_line[15].strip('\'')
            weapon = weapon.strip()
            weapon = re.sub(r'_\d+$', '', weapon) # Remove trailing _<digits>

            curr_user = self.rsi_handle["current"]

            if killed == killer:
                # Current user killed themselves
                kill_result["result"] = "suicide"
                kill_result["data"] = {
                    'discord_id': self.discord_id["current"],
                    'player': curr_user,
                    'victim': curr_user,
                    'weapon': weapon,
                    'zone': zone,
                    'current_ship': self.active_ship["current"],
                    'game_mode': self.game_mode,
                    'time': kill_time,
                    'client_ver': self.local_version,
                    'anonymize_state': self.anonymize_state
                }
            elif killed == curr_user:
                # Current user died
                kill_result["result"] = "killed"
                kill_result["data"] = {
                    'discord_id': self.discord_id["current"],
                    'player': killer,
                    'victim': curr_user,
                    'weapon': weapon,
                    'zone': self.active_ship["current"],
                    'current_ship': self.active_ship["current"],
                    'game_mode': self.game_mode,
                    'time': kill_time,
                    'client_ver': self.local_version,
                    'anonymize_state': self.anonymize_state
                }
            elif killer.lower() == "unknown":
                # Potential Ship reset
                kill_result["result"] = "reset"
            else:
                # Current user killed something else
                if self.check_ignored_victims(self.api.sc_data["ignoredVictimRules"], line):
                    kill_result["result"] = "exclusion"
                    return kill_result
                kill_result["result"] = "killer"
                kill_result["data"] = {
                    'discord_id': self.discord_id["current"],
                    'player': curr_user,
                    'victim': killed,
                    'weapon': weapon,
                    'zone': zone,
                    'current_ship': self.active_ship["current"],
                    'game_mode': self.game_mode,
                    'time': kill_time,
                    'client_ver': self.local_version,
                    'anonymize_state': self.anonymize_state
                }
            return kill_result
        except Exception as e:
            self.log.error(f"parse_kill_line(): Error: {e.__class__.__name__} {e}")
            return {"result": "", "data": None}

    def parse_death_line(self, line:str, curr_user:str):
        """Parse death event."""
        try:
            death_result = {"result": "", "data": {}}

            if not self.check_exclusion_scenarios(line):
                death_result["result"] = "exclusion"
                return death_result

            split_line = line.split(' ')

            kill_time = split_line[0].strip('\'')
            killer = split_line[12].strip('\'')

            death_result["result"] = "killed"
            death_result["data"] = {
                'time': kill_time,
                'player': killer,
                'victim': curr_user,
                'game_mode': self.game_mode,
            }
            return death_result
        except Exception as e:
            self.log.error(f"parse_death_line(): Error: {e.__class__.__name__} {e}")
            return {"result": "", "data": None}

    def check_ignored_victims(self, ignored_victim_rules, line:str) -> bool:
        """Check if any ignored victims are present in the given line."""
        for data in ignored_victim_rules:
            ignore_type = data["type"]
            if ignore_type == "substring" and data["value"].lower() in line.lower():
                return True
            elif ignore_type == "startsWith" and line.lower().startswith(data["value"].lower()):
                return True
            elif ignore_type == "regex" and re.search(data["value"], line):
                return True
        return False

    # NOTE: This is a synomous function used in GrimReaperBot - Changes or enhancements should be mirrored to it
    def convert_string(self, data_map, src_string:str, fuzzy_search=bool) -> str:
        """Get the best human readable string from the established data maps"""
        try:
            if fuzzy_search:
                fuzzy_found_dict = {}
                for key, value in data_map.items():
                    pts = fuzz.ratio(key, src_string)
                    if pts >= 90:
                        fuzzy_found_dict[value] = pts
        
                if len(fuzzy_found_dict) > 0:
                    # Sort the fuzzy matches by their score and return the best match
                    sorted_fuzzy = dict(sorted(fuzzy_found_dict.items(), key=lambda item: item[1], reverse=True))
                    return list(sorted_fuzzy.keys())[0]
            else:
                best_key_match = ""
                for key in data_map.keys():
                    # if src_string contains key
                    if src_string.startswith(key):
                        if len(key) > len(best_key_match):
                            best_key_match = key
                if best_key_match:
                    return data_map[best_key_match]
        except Exception as e:
            print(f"Error in data_map_utils.convert_string: {e}")
        return src_string

    def find_rsi_handle(self) -> str:
        """Get the current user's RSI handle."""
        acct_str = "<Legacy login response> [CIG-net] User Login Success"
        sc_log = open(self.log_file_location, "r")
        lines = sc_log.readlines()
        for line in lines:
            if -1 != line.find(acct_str):
                line_index = line.index("Handle[") + len("Handle[")
                if 0 == line_index:
                    self.log.error("RSI Handle not found. Please ensure the game is running and the log file is accessible.")
                    self.gui.api_status_label.config(text="Key Status: Error", fg="yellow")
                    return "N/A"
                potential_handle = line[line_index:].split(' ')[0]
                return potential_handle[0:-1]
        self.log.error("RSI Handle not found. Please ensure the game is running and the log file is accessible.")
        self.gui.api_status_label.config(text="Key Status: Error", fg="yellow")
        return "N/A"

    def find_rsi_geid(self) -> str:
        """Get the current user's GEID."""
        acct_kw = "AccountLoginCharacterStatus_Character"
        sc_log = open(self.log_file_location, "r")
        lines = sc_log.readlines()
        for line in lines:
            if -1 != line.find(acct_kw):
                return line.split(' ')[11]
                
    def update_kd_ratio(self) -> None:
        """Update KDR."""
        self.log.debug(f"update_kd_ratio(): Kills={self.kill_total}, Deaths={self.death_total}")
        if self.kill_total == 0 and self.death_total == 0:
            kd_display = "--"
        elif self.death_total == 0:
            kd_display = "∞"
        else:
            kd = self.kill_total / self.death_total
            kd_display = f"{kd:.2f}"
        # Update the KD label in the GUI
        if hasattr(self.gui, 'kd_ratio_label'):
            self.gui.kd_ratio_label.config(text=f"KD Ratio: {kd_display}", fg="#00FFFF")

    def handle_player_death(self) -> None:
        """Handle KDR when user dies."""
        self.curr_killstreak = 0
        self.death_total += 1
        # ... other updates ...
        self.update_kd_ratio()

    def handle_player_kill(self) -> None:
        """Handle KDR when user gets a kill."""
        self.curr_killstreak += 1
        if self.curr_killstreak > self.max_killstreak:
            self.max_killstreak = self.curr_killstreak
        self.kill_total += 1
        # ... other updates ...
        self.update_kd_ratio()