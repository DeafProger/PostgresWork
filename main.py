# Fifth CourseWork. Written by Valentin Ustinov AKA @DeafProger.
import db_manager


def main():
    pg = db_manager.DBPostgres()

    while True:
        print('''\nВыберите действие:
        0: Выйти из программы.
        1: Создать/сбросить БД.
        2: Добавить компании в БД.
        3: Получить из БД список всех компаний и число различных вакансий у каждой компании
        4: Получить все вакансии по названию компании из п.3
        5: Получить среднюю зарплату по всем вакансиям
        6: Получить список вакансий, зарплата которых выше средней по всем вакансиям
        7: Получить список вакансий, в названии которых содержится поисковое слово''')

        user_input = input()
        if not user_input.isdigit(): continue
        dig = int(user_input)

        if dig == 7:
            keyword = input('Введите слово для поиска вакансий: ')
            for item in pg.get_vacancies_with_keyword(keyword):
                print(item)

        if dig == 6:
            for item in pg.get_vacancies_with_higher_salary(pg.get_avg_salary()):
                print(item)

        if dig == 5: print(pg.get_avg_salary())

        if dig == 4:
            user_input = input('Введите название компании из п.3: ')
            for item in pg.get_all_vacancies(user_input):
                print(item[0],item[1],item[2],item[3])

        if dig == 3:
            for item in pg.get_companies_and_vacancies_count():
                print(item[0]+':', item[1], 'вакансий')

        if dig == 2:
            user_input = input('Введите список компаний, разделяемых запятыми: ')
            # by example: 'альфа банк, тинькофф банк, 2 гис, московский банк'
            print('Ждите...')

            for employer in user_input.split(','):
                pg.db_add_company(employer)

        if dig == 1: pg.db_reset()

        if dig == 0: break
    

if __name__ == '__main__':
    main()
