import matplotlib.pyplot as plt
import pandas as pd
import os
import sys

summary_file = "report/experiment_summary.csv"

if not os.path.exists(summary_file):
    print(f"No summary file found at {summary_file}. Run your experiments first!")
    sys.exit(1)

df = pd.read_csv(summary_file)

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10))

ax1.bar(df["Config"], df["Throughput"], color="skyblue")
ax1.set_title("System Throughput vs Configuration")
ax1.set_ylabel("Throughput (Mbps)")
ax1.tick_params(axis="x", rotation=45)

ax2.plot(df["Config"], df["Max_Latency"], marker="o", color="salmon", linewidth=2)
ax2.set_title("Max Latency vs Configuration")
ax2.set_ylabel("Max Latency (seconds)")
ax2.tick_params(axis="x", rotation=45)

plt.tight_layout()

os.makedirs("report", exist_ok=True)
plt.savefig("report/performance_graphs.png")
print("Graphs saved successfully to report/performance_graphs.png")
