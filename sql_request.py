import datetime, time
import datedelta
import pandas as pd
import datetime
import re, ast
from sqlalchemy import create_engine


def get_table_sql():
    '''
    current_date = datetime.today()
    c_day = current_date.day
    c_month = current_date.month
    c_year = current_date.year
    date = f"""STR_TO_DATE(CONCAT('{c_day}','.',{c_month},'.',{c_year}), '%%d.%%m.%%Y')"""
    '''
    
    try:
        DOC = open(r'Z:\Аналитический отдел\Харайкин М.А\Python\connection str RIES.txt')
    except FileNotFoundError:
        DOC = open(r'C:\Users\ws-tmn-an-15\Desktop\Харайкин М.А\Python документы\connection str RIES.txt')
    CONNECT = DOC.read()
    CONNECT = ast.literal_eval(re.sub(r'\n','', CONNECT))
    
    db_connection = create_engine(f"mysql+pymysql://{CONNECT['user_temp']}:{CONNECT['password_temp']}@{CONNECT['host1']}/{CONNECT['database']}")

    df_query = pd.read_sql("""
                        SELECT cities.name as "Город"
                        , ot.ticket_id as "Номер заявки на покупку/продажу", DATE_FORMAT(ot.created,'%%d.%%m.%%y') AS "дата создания заявки на покупку/продажу", DATE_FORMAT(ot.closed,'%%d.%%m.%%y') AS "дата закрытия заявки на покупку/продажу", ost_ticket_types.type_name AS "типзаявки на покупку/продажу", ot.status AS " статус заявки"
                        -- , ecp.phone as "Телефон клиента"
                        , (select concat(last_name, " ", first_name," ", middle_name)
                        from exist_clients 
                        where id = ot.client_id) as 'ФИО клиента'
                        , (SELECT ecp1.phone
                        FROM exist_client_phones ecp1
                        WHERE ec.id = ecp1.client_id
                        and ecp1.phone IS NOT NULL
                        and NOT ecp1.phone LIKE '%%undefined%%'
                        LIMIT 0,1) "Телефон клиента 1"
                        , (SELECT ecp1.phone
                        FROM exist_client_phones ecp1
                        WHERE ec.id = ecp1.client_id
                        and ecp1.phone IS NOT NULL
                        and NOT ecp1.phone LIKE '%%undefined%%'
                        LIMIT 1,1) "Телефон клиента 2"
                        , (SELECT ecp1.phone
                        FROM exist_client_phones ecp1
                        WHERE ec.id = ecp1.client_id
                        and ecp1.phone IS NOT NULL
                        and NOT ecp1.phone LIKE '%%undefined%%'
                        LIMIT 2,0) "Телефон клиента 3"

                        , u.id as "id риэлтора", u.fio as "ФИО риэлтора", (SELECT q.fio FROM users q WHERE u.manager_id = q.id) AS 'ФИО менеджера', objects.sold_date as "дата продажи"
                        -- , ecp.client_id

                        FROM ost_ticket ot
                        LEFT JOIN cities on ot.city_id = cities.id
                        LEFT JOIN users u ON ot.staff_id = u.id
                        LEFT JOIN ost_ticket_types on ot.type_id = ost_ticket_types.type_id
                        LEFT JOIN exist_clients ec ON ot.client_id=ec.id
                        -- LEFT JOIN exist_client_phones ecp ON ec.id = ecp.client_id
                        LEFT JOIN objects ON ot.object_id = objects.id

                        where ot.created >= "2022-01-01"
                        and (ot.city_id = 23 or ot.city_id = 155)
                        and ot.type_id in (3,7,11,14)
                        -- AND ot.ticket_id = "29915860"
                        AND (SELECT ecp1.phone
                        FROM exist_client_phones ecp1
                        WHERE ec.id = ecp1.client_id
                        and ecp1.phone IS NOT NULL
                        and NOT ecp1.phone LIKE '%%undefined%%'
                        LIMIT 0,1) IS NOT NULL

                        and u.fio IS NOT NULL
                        and NOT u.fio LIKE '%%TEST%%' 
                        AND NOT u.fio LIKE '%%ries%%' 
                        AND NOT u.fio LIKE '%%тест%%' 
                        AND NOT u.fio LIKE '%%РИЭС%%'
                        -- and ecp.phone IS NOT NULL
                        -- and ecp.phone NOT IN (89999999999)
                        -- and objects.sold_date IS NOT NULL
                        -- and ot.ticket_id in (36700580)
                        -- and ot.staff_id <> 0

                        -- GROUP BY ot.ticket_id
                        -- ORDER BY ot.created
                        -- LIMIT 100000


                           """, con=db_connection, 
                           #parse_dates={'дата создания заявки на покупку/продажу': {'format': '%d/%m/%y'},
                           #                                     'дата закрытия заявки на покупку/продажу': {'format': '%d/%m/%y'}}
                           )
    
    return df_query