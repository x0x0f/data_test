from clickhouse_driver import Client
from main_pandas import column_names, query
import pandas as pd


if __name__ == "__main__":
    client = Client(
        '116.202.64.188',
        user='challenge',
        password='leadsChallenge2021',
        database='challenge'
    )

    # Запрос соединяет 3 родственные таблицы в одну.
    response = client.execute(query)
    df = pd.DataFrame().from_records(response, columns=column_names)

    df2 = df.copy()

    changed_by_user_rule = df['changed_by_user_id'] != 0
    status_rule = df['status'] == 'approved'
    date_2021_first_quart_rule = (df['price_start_date'] >= '2021-01-01 00:00:00') & \
                                 (df['price_start_date'] <= '2021-03-31 23:59:59')

    filtered_df_2021_first_quart = df[changed_by_user_rule & status_rule & date_2021_first_quart_rule]

    date_2020_rule_1 = (df['price_start_date'] >= '2020-01-01 00:00:00') & \
                       (df['price_start_date'] <= '2020-12-31 23:59:59')

    date_2020_rule_2 = (df['price_start_date'] < '2020-01-01 00:00:00') & \
                       (df['price_end_date'] >= '2020-01-01 00:00:00')

    filtered_df_2020 = df2[changed_by_user_rule & status_rule & (date_2020_rule_1 | date_2020_rule_2)]

    uniq_df = pd.concat([filtered_df_2021_first_quart, filtered_df_2020])
    uniq_df = uniq_df.reset_index(drop=True)
    uniq_df_group = uniq_df.groupby(list(df.columns))
    index = [x[0] for x in uniq_df_group.groups.values() if len(x) == 1]
    uniq_df.reindex(index)

    extra_quarter_revenue = sum(uniq_df.revenue - uniq_df.default_revenue)
    print(extra_quarter_revenue)


