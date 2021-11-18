from clickhouse_driver import Client

# Запрос фильтрует все ряды где changed_by_user_id не равно 0,
# status конверсии равен approved и значение price_start_date
# находится между '2021-01-01 00:00:00' и '2021-03-31 23:59:59'.
# Находит значения связок (offer_id, goal_id, affiliate_id) которых не было в 2020 году.
# Вычисляет дополнительный доход и суммирует его.
query = '''
    SELECT SUM(revenue - default_revenue)
    FROM
        (
        SELECT offer_id, goal_id, affiliate_id, revenue, default_revenue
        FROM personal_payout_date_intervals ppd
            INNER JOIN personal_payouts_log ppl ON
                ppd.log_id = ppl.id
            INNER JOIN conversions con ON
                ppd.offer_id = con.offer_id AND
                ppd.goal_id == con.goal_id AND
                ppd.affiliate_id = con.affiliate_id
        WHERE
            ppl.changed_by_user_id != 0 AND
            con.status = 'approved' AND
            ppd.price_start_date BETWEEN '2021-01-01 00:00:00' AND '2021-03-31 23:59:59'
        )
    WHERE (offer_id, goal_id, affiliate_id, revenue, default_revenue) NOT IN
        (
        SELECT offer_id, goal_id, affiliate_id, revenue, default_revenue
        FROM personal_payout_date_intervals ppd
            INNER JOIN personal_payouts_log ppl ON
                ppd.log_id = ppl.id
            INNER JOIN conversions con ON
                ppd.offer_id = con.offer_id AND
                ppd.goal_id == con.goal_id AND
                ppd.affiliate_id = con.affiliate_id
        WHERE
            ppl.changed_by_user_id != 0 AND
            con.status = 'approved' AND
            ((ppd.price_start_date BETWEEN '2020-01-01 00:00:00' AND '2020-12-31 23:59:59') OR
            (ppd.price_start_date < '2020-01-01 00:00:00' AND ppd.price_end_date >= '2020-01-01 00:00:00'))
        )
'''


if __name__ == "__main__":
    client = Client(
        '116.202.64.188',
        user='challenge',
        password='leadsChallenge2021',
        database='challenge'
    )

    response = client.execute(query)
    print(response[0][0])
