import csv, hashlib, os, sys, random
path, rows = sys.argv[1], int(sys.argv[2])
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 7
rng = random.Random(seed)
with open(path, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['reference', 'moisture', 'thickness', 'temperature', 'airflow', 'minutes'])
    for i in range(rows):
        m = round(rng.uniform(20, 60), 2)
        w.writerow([f'HX-BIG-{i+1:07d}', m, round(rng.uniform(1, 8), 2), round(rng.uniform(50, 100), 2),
                    round(rng.uniform(1, 5), 2), round(m * 1.6 + rng.gauss(0, 3), 2)])
print(len(open(path, 'rb').read()), hashlib.sha256(open(path, 'rb').read()).hexdigest())
