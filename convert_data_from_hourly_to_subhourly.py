import pandas as pd
from datetime import timedelta

def process_csv_data(filepath):
    # CSVファイルを読み込む
    df = pd.read_csv(
        filepath, 
        parse_dates=[0]  # 1列目の日付時刻をパース
    )
    
    # 数値データの列を確認する
    expected_float_columns = df.columns[1:]  # 日付時刻列を除くすべての列
    problem_columns = []

    for col in expected_float_columns:
        if df[col].dtype not in ['float64', 'int64']:
            problem_columns.append(col)
    
    # 問題がある列が存在する場合は報告
    if problem_columns:
        print("問題のある列が見つかりました：", problem_columns)
        for col in problem_columns:
            print(f"{col}: 最初の5つの値 - {df[col].head()}")
    else:
        print("すべての数値列が適切な型で読み込まれています。")

       # 数値データの列だけをfloatとして扱うためにdtypeを設定する
    float_columns = df.columns[1:]  # 日付時刻列を除く全ての列
    for col in float_columns:
        df[col] = df[col].astype(float)
    
    # 新しいDataFrameを作成するための空のリスト
    new_rows = []
    
    # 各行でループ処理
    for _, row in df.iterrows():
        date_time = row['DATE_TIME']
        half_values = row[float_columns] / 2  # 値を半分にする、明確に数値列だけを選択
        
        # 現在の時間の行を追加
        new_rows.append([date_time] + list(half_values))
        
        # 30分後の時間の行を追加
        new_rows.append([date_time + timedelta(minutes=30)] + list(half_values))
    
    # 新しいDataFrameを作成
    new_df = pd.DataFrame(new_rows, columns=df.columns)
    
    # 日付時刻でソート
    new_df.sort_values('DATE_TIME', inplace=True)
    
    # 日付ごとに1から48までの時間帯を割り当て
    new_df['TIME_SLOT'] = new_df.groupby(new_df['DATE_TIME'].dt.date).cumcount() + 1
    
    # 新しいファイルとして保存
    new_df.to_csv('processed_data.csv', index=False)
    return new_df

# この関数の呼び出し
processed_data = process_csv_data('combined_data.csv')

