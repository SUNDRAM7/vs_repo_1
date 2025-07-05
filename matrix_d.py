import pandas as pd
import numpy as np

# Load the Excel file
df = pd.read_excel("Book1.xlsx")  # Replace with your actual file path
print(df.columns.tolist())

# Extract tp and sl values from the 'sl_tp' column
df[['sl', 'tp']] = df['sl_tp'].str.extract(r'sl(\d+)_t(\d+)').astype(float)

# Pivot data to create matrix format for each metric
metrics = ['Sharpe', 'Win rate', 'Total PnL', 'Max Drawdown']
tp_vals = [50, 60, 70, 80, 90, 100]
sl_vals = [50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150]

with pd.ExcelWriter("metric_matrices.xlsx") as writer:
    for metric in metrics:
        # Create matrix for each metric
        matrix = pd.DataFrame(index=tp_vals, columns=sl_vals)
        metric_df = df[df['Metric'] == metric]
        
        for _, row in metric_df.iterrows():
            tp = int(row['tp'])
            sl = int(row['sl'])
            if tp in tp_vals and sl in sl_vals:
                matrix.loc[tp, sl] = row['Value']
        
        # Write matrix to individual sheet
        matrix.index.name = 'tp/sl'
        matrix.to_excel(writer, sheet_name=metric.replace(" ", "_"))

print("✅ Matrices successfully written to 'metric_matrices.xlsx'")