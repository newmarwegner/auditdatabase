from sqlalchemy import create_engine
from decouple import config


class Postgres:
    def __init__(self):
        self.db_connection_url = config('dburl')
        self.conn = None
        self.schema_name = 'audit'
        self.table = 'logged_actions'

    def conn_postgres(self):
        """
        Function to connect with postgresql database considering url inside of .env file
        :return: return a connection with postgresql
        """
        self.conn = create_engine(self.db_connection_url)
        return self.conn

    def schemaname(self, schema):
        """
        :param schema: schema with audit logs
        :return: schema_name to be use on processes
        """
        self.schema_name = schema

    def select_postgres(self, days=0, columns='*'):
        """
        Function to return all rows of audit_logs postgres considering inserts, updates and deletes
        :param days: Last days to filter. The 0 parameter return all rows
        :param columns: Can be used to another selection in postgres, to audit_logs need to be *
        :return: Return all rows selected
        """
        if columns != '*':
            columns = ','.join(columns)

        select = f"select {columns} from {self.schema_name}.{self.table}"

        if days != 0:
            select = select + f" where action_tstamp > current_date - interval '{days} days'"
        self.conn_postgres()

        return self.conn.execute(select).fetchall()

    def auditoperation_postgres(self, choice, days=0):
        """
        :param days: Amount of days to filter
        :param choice: Insert 'I", Update 'U', Delete 'D"
        :return: List filtered with param choice given
        """
        return [t for t in self.select_postgres(days) if t[5] == choice]

    def concat_alters(self, days):
        """
        Function to concat in list all updates, deletes, and inserts find in audit_logs
        :param days: Days to be used as filter on last days
        :return: List of concat operation in a specific filter dates
        """
        update = self.auditoperation_postgres('U', days)
        delete = self.auditoperation_postgres('D', days)
        inserts = self.auditoperation_postgres('I', days)

        return update + delete + inserts

    def list_to_dictionary(self, list_alters):
        """
        Function to get a list to be convert as dictionary final result
        :param list_alters: param from function concat_alters
        :return: a list with table names, primary keys and operations
        """
        list_to_dictionary = []
        for i in list_alters:
            table_name = '.'.join(i[1:3])
            if not i[5] == 'D':
                values = i[7][1:-2].split(',')
                list_to_dictionary.append([table_name, values[0], i[5]])
            else:
                values = i[6][1:-2].split(',')
                list_to_dictionary.append([table_name, values[0], i[5]])

        return list_to_dictionary


    def get_alters(self, list_alters):
        """
        Function to get a dictionary of table names and primary key of inserts, deletes and updates
        :param list_alters: List of all inserts, deletes and updates concat
        :return: Dictionary with schemaname, table, operation and primary keys
        """
        list_to_dictionary = self.list_to_dictionary(list_alters)
        dicionario = {}
        for i in list_to_dictionary:
            key = i[0]
            id_pk = i[1]
            op = i[2]
            if key not in dicionario:
                dicionario[key] = {
                    op: [str(id_pk)]
                }
            elif op not in dicionario[key]:
                dicionario[key][op] = [str(id_pk)]
            else:
                dicionario[key][op].append(str(id_pk))

        return dicionario


if __name__ == '__main__':
    pg = Postgres()
    list_alters = pg.concat_alters(20)
    tables = pg.get_alters(list_alters)
    for item in tables.items():
        print(item)
