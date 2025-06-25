import pandas as pd
import numpy as np
import re
from pathlib import Path
from collections import defaultdict

def generate_epu_reports():

    import pandas as pd
    import numpy as np
    import re
    from pathlib import Path
    from collections import defaultdict

    BASE_DIR = Path.cwd() / '整合結果' / 'EPU匯出結果'
    print(f"📂 掃描資料夾（遞迴）: {BASE_DIR}")
    if not BASE_DIR.exists():
        print("❌ 錯誤：找不到 EPU匯出結果 資料夾。")
        exit()

    monthly_data = defaultdict(lambda: defaultdict(list))

    filename_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})[_-](自由時報|聯合報|中時)_EPU檢查結果\.xlsx")

    for file in BASE_DIR.rglob("*.xlsx"):
        match = filename_pattern.search(file.name)
        if not match:
            print(f"⚠️ 檔名不符規則，略過：{file.name}")
            continue

        date_str, media = match.groups()
        try:
            report_date = pd.to_datetime(date_str)
        except Exception:
            print(f"⚠️ 日期格式錯誤，略過：{date_str} | {file.name}")
            continue

        if media == '自由時報':
            media = '自由'
        elif media == '聯合報':
            media = '聯合'

        month_key = report_date.strftime("%Y-%m")

        try:
            df_raw = pd.read_excel(file, header=None)
        except Exception as e:
            print(f"❌ 無法讀取 {file.name}：{e}")
            continue

        符合數 = 不符合數 = None
        for _, row in df_raw.iterrows():
            text = str(row[0])
            if "✔" in text:
                try:
                    符合數 = int(str(row[1]).strip())
                except:
                    pass
            elif "✘" in text:
                try:
                    不符合數 = int(str(row[1]).strip())
                except:
                    pass
            if 符合數 is not None and 不符合數 is not None:
                break

        if 符合數 is None or 不符合數 is None:
            print(f"⚠️ 無法擷取符合與不符合 EPU 數量：{file.name}")
            continue

        total = 符合數 + 不符合數
        df_day = pd.DataFrame([{
            '日期': report_date,
            '符合EPU': 符合數,
            '總新聞篇數': total
        }])

        monthly_data[month_key][media].append(df_day)
        print(f"✅ 已加入：{file.name}｜符合：{符合數}／總數：{total}")

    for month, media_dict in monthly_data.items():
        print(f"📅 處理月份：{month}")
        merged_media_dfs = {}

        for media in ['自由', '中時', '聯合']:
            if media not in media_dict:
                print(f"⚠️ {media} 在 {month} 無資料")
                continue
            merged = pd.concat(media_dict[media])
            grouped = merged.groupby('日期', as_index=False).sum()
            grouped['Yit'] = grouped['符合EPU'] / grouped['總新聞篇數'].replace(0, np.nan)
            grouped['Zt'] = grouped['Yit']
            mean_Zt = grouped['Zt'].mean(skipna=True)
            std_Yit = grouped['Yit'].std(skipna=True)
            grouped = pd.concat([grouped, pd.DataFrame([{
                '日期': pd.NaT,
                '符合EPU': np.nan,
                '總新聞篇數': np.nan,
                'Yit': std_Yit,
                'Zt': np.nan,
                'normalized EPU': np.nan
            }])], ignore_index=True)
            grouped = pd.concat([grouped, pd.DataFrame([{
                '日期': pd.NaT,
                '符合EPU': np.nan,
                '總新聞篇數': np.nan,
                'Yit': np.nan,
                'Zt': mean_Zt,
                'normalized EPU': np.nan
            }])], ignore_index=True)
            grouped['normalized EPU'] = grouped['Zt'] * 100 / mean_Zt
            merged_media_dfs[media] = grouped
            print(f"✅ {media} 完成彙整，共 {len(grouped)-2} 天資料")

        if all(k in merged_media_dfs for k in ['自由', '中時', '聯合']):
            df1 = merged_media_dfs['自由'].dropna(subset=['日期'])
            df2 = merged_media_dfs['中時'].dropna(subset=['日期'])
            df3 = merged_media_dfs['聯合'].dropna(subset=['日期'])
            summary = df1[['日期', 'Yit']].rename(columns={'Yit': 'Y1t'})
            summary = summary.merge(df2[['日期', 'Yit']].rename(columns={'Yit': 'Y2t'}), on='日期', how='outer')
            summary = summary.merge(df3[['日期', 'Yit']].rename(columns={'Yit': 'Y3t'}), on='日期', how='outer')
            summary = summary.sort_values('日期')
            summary['日期'] = pd.to_datetime(summary['日期']).dt.date
            summary['Zt'] = (summary['Y1t'] + summary['Y2t'] + summary['Y3t']) / 3
            summary['EPU index'] = summary['Zt'] * 100 / summary['Zt'].mean(skipna=True)
            summary.loc[len(summary)] = [pd.NaT, None, None, None, summary['Zt'].mean(skipna=True), None]
            print("✅ 已建立總覽分頁")
        else:
            summary = pd.DataFrame()
            print("❌ 無法建立總覽（報社資料不齊）")

    output_path = Path.cwd() / f"EPU_{month}.xlsx"
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            for media in ['自由', '中時', '聯合']:
                if media in merged_media_dfs:
                    merged_media_dfs[media].to_excel(writer, sheet_name=media, index=False)
            if not summary.empty:
                summary.to_excel(writer, sheet_name='總覽', index=False)

    print(f"💾 已輸出：{output_path}")



if __name__ == "__main__":
    import time
    while True:
        print("🕒 執行 EPU 報表產生...")
        generate_epu_reports()
        print("✅ 執行完成，等待 3 小時...\n")
        time.sleep(3 * 60 * 60)
