from abc import ABC, abstractmethod


class DBManager(ABC):
    """ Класс для работы с БД """

    @abstractmethod
    def get_companies_and_vacancies_count(self):
        pass

    def get_vacancies_with_higher_salary(self, avg):
        pass

    def get_vacancies_with_keyword(self, keyword):
        pass

    def get_all_vacancies(self, company_name):
        pass

    def db_add_company(self, name):
        pass

    def get_avg_salary(self):
        pass

    def db_reset(self):
        pass

