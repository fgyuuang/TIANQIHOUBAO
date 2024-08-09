import os
import pandas as pd
from datetime import datetime


def merge_and_sort_csv_files(folder_path):
    for subfolder_name in os.listdir(folder_path):
        subfolder_path = os.path.join(folder_path, subfolder_name)

        if os.path.isdir(subfolder_path):
            csv_files = [f for f in os.listdir(subfolder_path) if f.endswith('.csv')]

            if not csv_files:
                print(f"警告: 子文件夹 '{subfolder_name}' 中没有 CSV 文件")
                continue

            dfs = []
            for csv_file in csv_files:
                csv_path = os.path.join(subfolder_path, csv_file)
                try:
                    df = pd.read_csv(csv_path, encoding='GBK')
                    if df.empty:
                        print(f"警告: 文件 '{csv_file}' 是空的")
                        continue
                    dfs.append(df)
                except pd.errors.EmptyDataError:
                    print(f"警告: 文件 '{csv_file}' 为空或格式错误")
                except UnicodeDecodeError:
                    print(f"警告: 文件 '{csv_file}' 使用了无法解码的编码")
                except Exception as e:
                    print(f"警告: 读取文件 '{csv_file}' 时出现错误: {e}")

            if not dfs:
                print(f"警告: 子文件夹 '{subfolder_name}' 中的所有 CSV 文件都无法读取")
                continue

            combined_df = pd.concat(dfs, ignore_index=True)

            if '日期' not in combined_df.columns:
                print(f"警告: 子文件夹 '{subfolder_name}' 中的合并文件没有 '日期' 列")
                continue

            try:
                combined_df['日期'] = pd.to_datetime(combined_df['日期'], errors='coerce')
                if combined_df['日期'].isnull().any():
                    print(f"警告: 子文件夹 '{subfolder_name}' 中的 '日期' 列有无效日期")

                combined_df = combined_df.sort_values(by='日期')
            except Exception as e:
                print(f"警告: 排序 '日期' 列时出错: {e}")
                continue

            # 计算期望的日期范围
            start_date = datetime(2011, 1, 1)
            end_date = datetime.now()
            expected_dates = pd.date_range(start=start_date, end=end_date).to_list()

            # 获取实际的日期范围
            actual_dates = combined_df['日期'].dropna().unique()
            actual_dates = pd.to_datetime(actual_dates).tolist()
            actual_dates.sort()  # 修正：使用列表的 sort 方法

            if len(expected_dates) != len(actual_dates) or not set(expected_dates).issubset(set(actual_dates)):
                print(f'{len(expected_dates)} {len(actual_dates)}')
                print(f"警告: 子文件夹 '{subfolder_name}' 中的日期数量与期望的不符")

            output_path = os.path.join(folder_path, f"{subfolder_name}.csv")
            combined_df.to_csv(output_path, index=False)
            print(f"合并并排序后的 CSV 文件已保存为 '{output_path}'")

# 使用示例
parent_folder = 'C:\code\Python Project\文本分析\旅游数据\TIANQIHOUBAO\海南'
merge_and_sort_csv_files(parent_folder)
