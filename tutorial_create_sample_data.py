import pandas as pd
from faasr.faasr import faasr_put_file, faasr_log

def create_sample_data(folder, output1, output2):

    # --- Create two dataframes ---
    df1 = pd.DataFrame({
        "v1": [e for e in range(1, 11)],
        "v2": [e**2 for e in range(1, 11)],
        "v3": [e**3 for e in range(1, 11)]
    })

    df2 = pd.DataFrame({
        "v1": [e for e in range(1, 11)],
        "v2": [2*e for e in range(1, 11)],
        "v3": [3*e for e in range(1, 11)]
    })

    # --- Save as CSV ---
    df1.to_csv("df1.csv", index=False)
    df2.to_csv("df2.csv", index=False)

    # --- Upload CSV to S3 (FaaSr default bucket) ---
    faasr_put_file(local_file="df1.csv", remote_folder=folder, remote_file=output1)
    faasr_put_file(local_file="df2.csv", remote_folder=folder, remote_file=output2)

    # --- Log message ---
    msg = f"Function create_sample_data finished; outputs written to folder {folder} in default S3 bucket"
    faasr_log(msg)
