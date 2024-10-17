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
        # Handling duplicates and outliers
        for key in data.keys():
            # Outlier removal for 'sales_train'
            if key == 'sales_train':
                # Create a mask for negative item prices
                mask_neg_price = data[key].item_price < 0
                data[key] = data[key][~mask_neg_price]

                # Aggregate duplicates by summing 'item_cnt_day'
                data[key] = data[key].groupby(
                    ['date', 'date_block_num', 'shop_id', 'item_id', 'item_price'], as_index=False
                ).agg({'item_cnt_day': 'sum'})

        # Merge datasets
        merged_df = pd.merge(data['sales_train'], data['items'], on='item_id', how='left')
        merged_df = pd.merge(merged_df, data['item_categories'], on='item_category_id', how='left')
        merged_df = pd.merge(merged_df, data['shops'], on='shop_id', how='left')

        # Convert 'date' to datetime and create new features: 'month', 'year'
        merged_df['date'] = pd.to_datetime(merged_df['date'], format='%d.%m.%Y')
        merged_df['month'] = merged_df['date'].dt.month
        merged_df['year'] = merged_df['date'].dt.year

        '''
        # Since further investigation is required regarding time series, 
        # the aggregation of item_cnt_day has been postponed until after the exploratory data analysis (EDA).

        # Code for monthly aggregation:
        # monthly_sales = merged_df.sort_values(by='date').groupby(
        #    ['date_block_num', 'shop_id', 'item_id'], as_index=False
        # ).agg({
        #    'item_price': ['std', 'mean'],
        #    'item_cnt_day': ['sum', 'mean', 'count'],
        #    'item_name': 'first',
        #    'item_category_name': 'first'
        # })
        # monthly_sales.columns = ['_'.join(col).strip('_') for col in monthly_sales.columns.values]

        # Renaming specific columns after aggregation
        # monthly_sales = monthly_sales.rename(columns={
        #    'item_cnt_day_sum': 'item_cnt_month',
        #    'item_cnt_day_mean': 'item_cnt_month_mean',
        #    'item_cnt_day_count': 'transaction_month',
        #    'item_name_first': 'item_name',
        #    'item_category_name_first': 'item_category_name'
        # })
        '''

        return merged_df  # Make sure to return the transformed DataFrame

    def load_data(self, data):
        # Save the transformed data to the target file
        data.to_csv(self.target_file, index=False)

    def run(self):
        # Run the full ETL pipeline
        extracted_data = self.extract_data()
        transformed_data = self.transform_data(extracted_data)
        self.load_data(transformed_data)
