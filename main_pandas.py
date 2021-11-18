from clickhouse_driver import Client
import pandas as pd


# Запрос соединяет 3 родственные таблицы в одну.
query = '''
    SELECT offer_id, goal_id, affiliate_id, status, revenue, default_revenue, price_start_date, price_end_date, changed_by_user_id
    FROM personal_payout_date_intervals ppd 
        INNER JOIN personal_payouts_log ppl ON 
            ppd.log_id = ppl.id
        INNER JOIN conversions con ON 
            ppd.offer_id = con.offer_id AND
            ppd.goal_id = con.goal_id AND
            ppd.affiliate_id = con.affiliate_id
'''
column_names = [
    'offer_id',
    'goal_id',
    'affiliate_id',
    'status',
    'revenue',
    'default_revenue',
    'price_start_date',
    'price_end_date',
    'changed_by_user_id'
]


if __name__ == "__main__":
    client = Client(
        '116.202.64.188',
        user='challenge',
        password='leadsChallenge2021',
        database='challenge'
    )

    # Загружаем в pandas полученные данные.
    # фильтруем все ряды где changed_by_user_id не равно 0,
    # status конверсии равен approved и значение price_start_date
    # находится между '2021-01-01 00:00:00' и '2021-03-31 23:59:59'.
    # Вычисляем дополнительный доход и суммируем его.
    response = client.execute(query)
    df = pd.DataFrame().from_records(response, columns=column_names)

    changed_by_user_rule = df['changed_by_user_id'] != 0
    status_rule = df['status'] == 'approved'
    date_rule = (df['price_start_date'] >= '2021-01-01 00:00:00') & (df['price_start_date'] <= '2021-03-31 23:59:59')
    filtered_df = df[changed_by_user_rule & status_rule & date_rule]

    extra_quarter_revenue = sum(filtered_df.revenue - filtered_df.default_revenue)
    print(extra_quarter_revenue)

