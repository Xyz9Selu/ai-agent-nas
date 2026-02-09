import io
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine


# 1. 认证初始化
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# 数据库连接
# engine = create_engine('postgresql://user:pass@host:port/db')

def get_data_from_drive(file_id):
    """
    根据文件 ID 自动判断是原生 Sheet 还是 XLSX，并统一导出为 CSV 流
    """
    file_metadata = drive_service.files().get(fileId=file_id, fields='mimeType, name').execute()
    mime_type = file_metadata.get('mimeType')
    
    fh = io.BytesIO()
    
    if mime_type == 'application/vnd.google-apps.spreadsheet':
        # 如果是 Google Sheet，导出为 CSV
        request = drive_service.files().export_media(fileId=file_id, mimeType='text/csv')
    else:
        # 如果是 XLSX，直接下载（稍后用 pandas 转 csv 或直接读，但为了统一逻辑，建议此处统一）
        request = drive_service.files().get_media(fileId=file_id)
        
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    fh.seek(0)
    
    # 将二进制流转为 DataFrame (CSV 或 Excel)
    if mime_type == 'application/vnd.google-apps.spreadsheet':
        # CSV 读取不需要 header，为了匹配你之前的 raw_data 逻辑
        df_raw = pd.read_csv(fh, header=None, dtype=str).fillna("")
    else:
        df_raw = pd.read_excel(fh, header=None, dtype=str).fillna("")
        
    return df_raw.values.tolist()

def clean_and_process(raw_data):
    # --- 你的原始逻辑：寻找表头 ---
    header_idx = 0
    for i, row in enumerate(raw_data):
        # 你的逻辑：找到第一个有 5 列以上数据的行
        actual_content = [cell for cell in row if str(cell).strip() != '']
        if len(actual_content) > 5:
            header_idx = i
            break
            
    header = raw_data[header_idx]

    # --- 你的原始逻辑：清洗数据 (Wash Data) ---
    valid_col_idx = [i for i, h in enumerate(header) if str(h).strip() != '']
    filtered_header = [str(header[i]).strip() for i in valid_col_idx]

    # 这里的 filtered_data 包含了你的两个关键过滤条件：
    # 1. 忽略空行
    # 2. 忽略重复的表头行 (row[first_col] != header[0])
    filtered_data = [
        [row[i] for i in valid_col_idx]
        for row in raw_data[header_idx + 1:]
        if any(str(cell).strip() for cell in row) and str(row[valid_col_idx[0]]) != filtered_header[0]
    ]

    # --- 你的原始逻辑：处理重复列名 ---
    seen = {}
    final_header = []
    for h in filtered_header:
        if h in seen:
            seen[h] += 1
            final_header.append(f'{h}_{seen[h]}')
        else:
            seen[h] = 0
            final_header.append(h)

    # 生成最终 DataFrame
    df = pd.DataFrame(filtered_data, columns=final_header)
    return df

# --- 执行流程 ---
FILE_ID = '1NH_cNjmhwoaOCpEMCq27cxhtXxQL23LR'
raw_list = get_data_from_drive(FILE_ID)
final_df = clean_and_process(raw_list)

print(f"处理完成，有效行数: {len(final_df)}")
# final_df.to_sql('your_table', engine, if_exists='replace', index=False, chunksize=1000)