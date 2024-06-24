import pwinput, psycopg2, requests
from base import DBManager


class DBPostgres(DBManager):
    """ Класс для работы с БД Postgress с дефолтными настройками """

    def __init__(self):
        """ Первичная настройка БД """
        self.__password = pwinput.pwinput(prompt='Enter password for default user as postgres: ', mask='')

        try:
            conn = psycopg2.connect(user="postgres", password=self.__password)
            cur = conn.cursor()
            cur.close()
            conn.close()

        except (Exception, psycopg2.Error) as error:
            print("Ошибка при инициализации с PostgreSQL: ", error)
            exit(-1)

    def get_companies_and_vacancies_count(self):
        try:
            conn = psycopg2.connect(user="postgres", password=self.__password)
            cur = conn.cursor()
            cur.execute(''' select e.name, count (*) as vacancies_count
                            from employers as e
                            left join vacancies as v on e.employer_id = v.employer_id
                            group by e.name
                            order by vacancies_count desc 
                        ''')
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при работе с PostgreSQL: ", error)
            exit(-3)

    def get_vacancies_with_higher_salary(self, avg):
        try:
            conn = psycopg2.connect(user="postgres", password=self.__password)
            cur = conn.cursor()
            cur.execute(''' select name, url, salary_min, salary_max 
                                  from vacancies 
                                  where salary_min+salary_max > %s 
                        ''', (2*avg,))
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при работе с PostgreSQL: ", error)
            exit(-6)

    def get_vacancies_with_keyword(self, keyword):
        try:
            conn = psycopg2.connect(user="postgres", password=self.__password)
            cur = conn.cursor()
            cur.execute(''' select name, url from vacancies 
                                  where name like %s 
                            ''', ('%'+keyword+'%',))
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при работе с PostgreSQL: ", error)
            exit(-7)

    def get_all_vacancies(self, company_name):
        try:
            conn = psycopg2.connect(user="postgres", password=self.__password)
            cur = conn.cursor()
            cur.execute(''' select v.name, v.url, v.salary_min, v.salary_max 
                            from vacancies as v
                            left join employers as e on e.employer_id = v.employer_id
                            where e.name = %s 
                        ''', (company_name,))
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при работе с PostgreSQL: ", error)
            exit(-4)

    def get_avg_salary(self):
        try:
            conn = psycopg2.connect(user="postgres", password=self.__password)
            cur = conn.cursor()
            cur.execute('''select avg(salary_min), avg(salary_max) from vacancies
                            where salary_min+salary_max > 0
                        ''')
            res = sum(cur.fetchone())//2
            cur.close()
            conn.close()
            return res
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при работе с PostgreSQL: ", error)
            exit(-5)

    def db_reset(self):
        try:
            conn = psycopg2.connect(user="postgres", password=self.__password)
            cur = conn.cursor()

            cur.execute("DROP TABLE IF EXISTS vacancies")
            cur.execute("DROP TABLE IF EXISTS employers")
            cur.execute('''
                        CREATE TABLE employers (
                            employer_id integer primary key unique, 
                            name varchar(255) not null,
                            url  varchar(255) not null)
                        ''')

            cur.execute('''
                        CREATE TABLE vacancies (
                            vacancy_id integer primary key unique, 
                            name varchar(255) not null,
                            url  varchar(255) not null,
                            salary_min float,
                            salary_max float,
                            employer_id integer,
                            foreign key (employer_id) references employers(employer_id) on delete cascade)
                        ''')

            conn.commit()
            cur.close()
            conn.close()

        except (Exception, psycopg2.Error) as error:
            print("Ошибка при сбросе PostgreSQL: ", error)
            exit(-2)

    def db_add_company(self, name):
        url = 'https://api.hh.ru/employers?currency=RUR&text='+name+'&per_page=100&page='
        k = 0

        while True:
            res = requests.get(url + str(k))
            if not res.ok: break
            items = res.json()['items']
            for itm in items:
                if itm['open_vacancies'] > 0:
                    try:
                        conn = psycopg2.connect(user="postgres", password=self.__password)
                        cur = conn.cursor()
                        cur.execute(''' insert into employers (employer_id,name,url) 
                                            values (%s,%s,%s) returning *''',
                                        (itm['id'], itm['name'], itm['url']))
                        conn.commit()
                        cur.close()
                        conn.close()
                    except (Exception, psycopg2.Error):
                        pass

                    resp = requests.get('https://api.hh.ru/vacancies?employer_id='+itm['id']).json()['items']
                    for item in resp:
                        salary_min, salary_max = 0, 0
                        if item['salary'] is not None:
                            if item['salary']['currency'] != 'RUR': continue
                            if item['salary']['from'] is not None: salary_min = item['salary']['from']
                            if item['salary']['to'] is not None: salary_max = item['salary']['to']
                        try:
                            conn = psycopg2.connect(user="postgres", password=self.__password)
                            cur = conn.cursor()
                            cur.execute(''' insert into vacancies (vacancy_id, name, url, 
                                                    salary_min, salary_max, employer_id) 
                                                    values (%s,%s,%s,%s,%s,%s) returning *''',
                                  (item['id'], item['name'], item['url'], salary_min, salary_max, itm['id']))
                            conn.commit()
                            cur.close()
                            conn.close()
                        except (Exception, psycopg2.Error):
                            pass

            k += 1
