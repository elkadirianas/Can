import multiprocessing
import time
import json
import random
import os
import math
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC_NAME = "player_metrics"

class PlayerProcess(multiprocessing.Process):
    """ Simule un joueur professionnel avec données physiologiques avancées """
    def __init__(self, player_id):
        super().__init__()
        self.player_id = player_id
        self.running = True
    
    def run(self):
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print(f"✅ Joueur {self.player_id} connecté (Capteurs actifs).")
        
        # État initial
        speed = 0.0
        heart_rate = 60.0
        fatigue = 0.0
        x, y = 50.0, 50.0 # Centre du terrain
        sprint_count = 0
        total_distance = 0.0
        
        last_time = time.time()

        try:
            while self.running:
                current_time = time.time()
                dt = current_time - last_time
                last_time = current_time

                # 1. SIMULATION CINÉMATIQUE (Mouvement)
                # On change la vitesse cible aléatoirement (Marche, Trot, Course, Sprint)
                target_speed = random.choices(
                    [0, 5, 15, 25, 32], 
                    weights=[0.1, 0.4, 0.3, 0.15, 0.05]
                )[0]
                
                # Inertie : La vitesse ne change pas instantanément
                # Formule : v_new = v_old + (target - v_old) * facteur
                prev_speed = speed
                speed = speed + (target_speed - speed) * 0.2
                acceleration = (speed - prev_speed) / 1.0 # m/s^2 (simplifié car dt=1s)

                # Position (Random Walk contraint dans le terrain 0-100)
                x += random.uniform(-2, 2) * (speed / 10)
                y += random.uniform(-2, 2) * (speed / 10)
                x = max(0, min(100, x))
                y = max(0, min(100, y))

                # Distance
                dist_step = (speed * 1000 / 3600) * 1.0 # m/s * 1s
                total_distance += dist_step

                if speed > 25.0 and prev_speed <= 25.0:
                    sprint_count += 1

                # 2. SIMULATION PHYSIOLOGIQUE
                # Le coeur suit l'effort + la fatigue accumulée
                target_hr = 60 + (speed * 4) + (fatigue * 0.5)
                heart_rate = heart_rate + (target_hr - heart_rate) * 0.1
                
                # La fatigue augmente avec l'effort intense, baisse au repos
                if speed > 20:
                    fatigue = min(100, fatigue + 0.8)
                elif speed < 5:
                    fatigue = max(0, fatigue - 0.1)

                # 3. ÉVÉNEMENTS (Chocs)
                impact_g = 0.0
                if random.random() < 0.01: # 1% de chance de choc par seconde
                    impact_g = random.uniform(3.0, 12.0) # De petit choc à gros tacle

                # 4. CONSTRUCTION DU PAYLOAD
                data = {
                    "player_id": self.player_id,
                    "timestamp": current_time,
                    # Métriques Physiques
                    "speed_kmh": round(speed, 2),
                    "acceleration_ms2": round(acceleration, 2),
                    "total_distance_m": round(total_distance, 1),
                    "sprint_count": sprint_count,
                    # Métriques Physio
                    "heart_rate_bpm": int(heart_rate),
                    "fatigue_index": round(fatigue, 1),
                    # Métriques Tactiques / Spatial
                    "position": {"x": int(x), "y": int(y)},
                    # Alertes
                    "impact_g": round(impact_g, 2)
                }
                
                producer.send(TOPIC_NAME, value=data)
                time.sleep(1) # Fréquence 1Hz

        except Exception as e:
            print(f"Erreur Joueur {self.player_id}: {e}")
        finally:
            producer.close()
            print(f"🟥 Joueur {self.player_id} déconnecté.")

def main():
    print("--- CAN 2025: ADVANCED PLAYER SIMULATOR ---")
    active_players = {}

    while True:
        try:
            cmd = input("Coach (add <id> / remove <id> / list)> ").strip().split()
            if not cmd: continue
            action = cmd[0].lower()
            
            if action == "add":
                try:
                    pid = int(cmd[1])
                    if pid not in active_players:
                        p = PlayerProcess(pid)
                        p.start()
                        active_players[pid] = p
                    else: print("Déjà sur le terrain.")
                except: print("ID invalide.")
            
            elif action == "remove":
                try:
                    pid = int(cmd[1])
                    if pid in active_players:
                        active_players[pid].terminate()
                        del active_players[pid]
                    else: print("Inconnu.")
                except: print("ID invalide.")
            
            elif action == "list":
                print(f"Actifs: {list(active_players.keys())}")
                
        except KeyboardInterrupt:
            print("\nArrêt du match.")
            for p in active_players.values(): p.terminate()
            break

if __name__ == "__main__":
    main()
