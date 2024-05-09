import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import pandas as pd
def download_csv_files(url, directory):
    # ディレクトリの作成（存在しない場合）
    os.makedirs(directory, exist_ok=True)  
    # ページにアクセス
    response = requests.get(url)
    response.raise_for_status()  # HTTPエラーがある場合は例外を発生させる    
    # BeautifulSoupを使用してHTMLを解析
    soup = BeautifulSoup(response.text, 'html.parser')    
    # CSVファイルのリンクを見つける
    links = soup.find_all('a', href=True)
    csv_links = [link['href'] for link in links if link['href'].endswith('.csv') and 'area_jyukyu_jisseki' in link['href']]
    
    # 各CSVファイルをダウンロード
    for csv_link in csv_links:
        # 絶対URLを生成
        download_url = urljoin(url, csv_link)
        
        # CSVファイルをダウンロード
        try:
            csv_response = requests.get(download_url)
            csv_response.raise_for_status()
            
            # ファイル名の抽出
            file_name = csv_link.split('/')[-1]
            file_path = os.path.join(directory, file_name)
            
            # CSVファイルを保存
            with open(file_path, 'wb') as f:
                f.write(csv_response.content)
            print(f"Downloaded and saved {file_name} to {directory}")
        except requests.exceptions.HTTPError as e:
            print(f"Failed to download {csv_link}: {e}")

def combine_csv_files_with_headers(directory):
    # ディレクトリ内の全CSVファイルを見つける
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    
    # データフレームリスト
    dfs = []
    
    # カスタムヘッダー名
    headers = [
        "DATE_TIME", "エリア需要〔MWh〕", "原子力〔MWh〕", "火力〔MWh〕", "水力〔MWh〕", 
        "地熱〔MWh〕", "バイオマス〔MWh〕", "太陽光実績〔MWh〕", "太陽光抑制量〔MWh〕", 
        "風力実績〔MWh〕", "風力抑制量〔MWh〕", "揚水等〔MWh〕", "連系線〔MWh〕"
    ]
    
    # 各CSVファイルをDataFrameとして読み込み
    for file_path in csv_files:
        df = pd.read_csv(
                        file_path,header=None, skiprows=2, encoding="shift-jis",
                        thousands=',',
                        parse_dates=[0],  # 1列目の日付時刻をパース
                        dtype={col: 'float' for col in range(1, 13)}  # 2列目から13列目までをfloatとして読み込む
    )
        df.columns = headers
        dfs.append(df)
    
    # 全データを結合
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # 日付時刻でソート
    combined_df['DATE_TIME'] = pd.to_datetime(combined_df['DATE_TIME'])
    combined_df.sort_values('DATE_TIME', inplace=True)
    
    # 結合したデータをCSVファイルとして保存
    combined_file_path = os.path.join(directory, "combined_data.csv")
    combined_df.to_csv(index=False)
    print(f"All data combined and sorted by date-time, saved to {combined_file_path}")

# URLとディレクトリを設定
url = 'https://www.kyuden.co.jp/td_area_jukyu/jukyu.html'
directory = 'Kyushu_Jukyu'

# ダウンロードと結合の関数をコメントアウト
# download_csv_files(url, directory)
combine_csv_files_with_headers(directory)