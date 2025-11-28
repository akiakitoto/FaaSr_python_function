import pandas as pd
from faasr.faasr import faasr_get_file, faasr_put_file, faasr_log

def compute_sum(folder, input1, input2, output):
    """
    Download two CSV files from S3, compute their element-wise sum,
    and upload the result back to S3.

    Parameters
    ----------
    folder : str
        S3 folder name where the input/output files are stored
    input1 : str
        Name of the first input CSV file in S3
    input2 : str
        Name of the second input CSV file in S3
    output : str
        Name of the output CSV file to be written back to S3
    """

    # ---- 1. Download input files from S3 ----
    faasr_get_file(remote_folder=folder, remote_file=input1, local_file="input1.csv")
    faasr_get_file(remote_folder=folder, remote_file=input2, local_file="input2.csv")

    # ---- 2. Read CSV and compute sum ----
    df1 = pd.read_csv("input1.csv")
    df2 = pd.read_csv("input2.csv")

    # pandas は DataFrame 同士の加算を自動で要素ごとに行う
    df_out = df1 + df2

    # 一時ローカルファイルを作成
    df_out.to_csv("output.csv", index=False)

    # ---- 3. Upload result back to S3 ----
    faasr_put_file(local_file="output.csv", remote_folder=folder, remote_file=output)

    # ---- 4. Log ----
    msg = f"Function compute_sum finished; output written to {folder}/{output} in default S3 bucket"
    faasr_log(msg)
