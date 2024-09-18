# Defining class for ETL
import os
import pandas as pd


class ETLPipeline:
    def __init__(self, source_file, target_file):
        self.source_file = source_file
        self.target_file = target_file

    def extract_data(self):
        dataframes = {}  # Dictionary to store DataFrames with file names (without extensions) as keys

        for dirname, _, filenames in os.walk(self.source_file):
            for filename in filenames:
                file_path = os.path.join(dirname, filename)
                name, ext = os.path.splitext(filename)  # Get the file name without extension

                try:
                    # Read the file into a DataFrame
                    df = pd.read_csv(file_path)
                    dataframes[name] = df  # Use the file name (without extension) as the key
                except Exception as e:
                    print(f"Failed to read {file_path}: {e}")

        return dataframes

    def transform_data(self, data: dict):

        # Removing outlier price and item count per day (based on logic has been mentioen in DQC)

        for key in data.keys():
            if key == 'sales_train':
                # Create a mask for negative values
                mask_neg_price = data[key].item_price < 0
                mask_neg_count = data[key].item_cnt_day < 0

                combined_mask = mask_neg_price | mask_neg_count
                data[key] = data[key][~ combined_mask]

        # Merge datasets
        merged_df = pd.merge(data['sales_train'], data['items'], on='item_id', how='left')
        merged_df = pd.merge(merged_df, data['item_categories'], on='item_category_id', how='left')
        merged_df = pd.merge(merged_df, data['shops'], on='shop_id', how='left')

        # Converting date -> datatime and creat new feature [month , year]

        merged_df['date'] = pd.to_datetime(merged_df['date'], format='%d.%m.%Y')

        merged_df['month'] = merged_df['date'].dt.month
        merged_df['year'] = merged_df['date'].dt.year

        # Aggregate sales data to monthly level
        monthly_sales = merged_df.sort_values(by='date').groupby(
            ['date_block_num', 'shop_id', 'item_id', 'item_category_id'], as_index=False)
        monthly_sales = monthly_sales.agg({
            'item_price': ['std', 'mean'],
            'item_cnt_day': ['sum', 'mean', 'count'],
            'item_name': 'first',
            'item_category_name': 'first'
        })
        monthly_sales.columns = ['_'.join(col).strip('_') for col in monthly_sales.columns.values]

        # Renaming specific columns after aggregation
        monthly_sales = monthly_sales.rename(columns={
            'item_cnt_day_sum': 'item_cnt_month',
            'item_cnt_day_mean': 'item_cnt_month_mean',
            'item_cnt_day_count': 'transaction_month',
            'item_name_first': 'item_name',
            'item_category_name_first': 'item_category_name'
        })

        return monthly_sales

    def load_data(self, data):
        # Code to load the transformed data into the target
        data.to_csv(self.target_file, index=False)

    def run(self):
        extracted_data = self.extract_data()
        transformed_data = self.transform_data(extracted_data)
        self.load_data(transformed_data)

