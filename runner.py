import os
from trade_log_atm_v1 import process_file

# === CONFIGURATION ===
CSV_FOLDER = r"C:\Users\Axxela\Desktop\atm strategy final\module_test"
OUTPUT_FOLDER = r"C:\Users\Axxela\Desktop\atm strategy final\module_test"
STOP_LOSS_MULTIPLIER = 1.2  # <-- Change this as needed

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

files = [f for f in os.listdir(CSV_FOLDER) if f.endswith(".xlsx")]
print(files)

for file in files:
    file_path = os.path.join(CSV_FOLDER, file)
    print(f"🔁 Processing {file} with SL multiplier: {STOP_LOSS_MULTIPLIER}")
    result_df = process_file(file_path, sl_multiplier=STOP_LOSS_MULTIPLIER)

    if not result_df.empty:
        output_path = os.path.join(OUTPUT_FOLDER, file.replace(".xlsx", f"_trades_SL{STOP_LOSS_MULTIPLIER}.xlsx"))
        result_df.to_excel(output_path, index=False)
        print(f"✅ Saved: {output_path}")
    else:
        print(f"⚠️ No trades found for {file}")
