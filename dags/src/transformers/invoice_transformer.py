import pandas as pd

class InvoiceTransformer():

    def __init__(self) -> None:
        pass

    def correct_invoice_dates_based_on_invoice_number(self, df : pd.DataFrame) -> pd.DataFrame:
        for key, value in df.groupby(['invoice_number'], dropna=True):
            df_most_occ_val = value[['invoice_date']].mode()
            most_occ_val = df_most_occ_val['invoice_date'].item()
            df.loc[df['invoice_number'] == key, 'invoice_date'] = most_occ_val
        return df

    def correct_customer_names_based_on_tax_rate(self, df : pd.DataFrame) -> pd.DataFrame:
        for key, value in df.groupby(['tax_rate'], dropna=True):
            df_most_occ_val = value[['customer_name']].mode()
            most_occ_val = df_most_occ_val['customer_name'].item()
            df.loc[df['tax_rate'] == key, 'customer_name'] = most_occ_val
        return df

    def correct_negative_prices(self, df : pd.DataFrame) -> pd.DataFrame:
        df['price'] = df['price'].abs()
        return df

    def correct_item_names(self, df : pd.DataFrame) -> pd.DataFrame:
        df.loc[df['price'] == 20, 'item_name'] = "Widget E"
        return df

    def correct_negative_quantities(self, df : pd.DataFrame) -> pd.DataFrame:
        df['quantity'] = df['quantity'].abs()
        return df

    def correct_tax_rates_based_on_customer_name(self, df : pd.DataFrame) -> pd.DataFrame:
        for key, value in df.groupby(['customer_name'], dropna=True):
            df_most_occ_val = value[['tax_rate']].mode()
            most_occ_val = df_most_occ_val['tax_rate'].item()
            df.loc[df['customer_name'] == key, 'tax_rate'] = most_occ_val
        return df

    def calculate_non_existing_total_sums(self, df : pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df['total_amount'].isnull()]
        price_before_taxes = filtered_df['quantity'] * filtered_df['price']
        price_after_taxes = price_before_taxes + price_before_taxes * filtered_df['tax_rate']
        df.loc[df['total_amount'].isnull(), 'total_amount'] = price_after_taxes
        return df

    def execute(self, df : pd.DataFrame) -> pd.DataFrame:
        df = self.correct_invoice_dates_based_on_invoice_number(df)
        df = self.correct_customer_names_based_on_tax_rate(df)
        df = self.correct_negative_prices(df)
        df = self.correct_item_names(df)
        df = self.correct_negative_quantities(df)
        df = self.correct_tax_rates_based_on_customer_name(df)
        df = self.calculate_non_existing_total_sums(df)
        return df
