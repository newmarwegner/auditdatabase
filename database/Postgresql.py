from sqlalchemy import create_engine
from decouple import config


class Postgres:
    def __init__(self):
        self.db_connection_url = config('dburl')
        self.conn = None
        self.schema_name = 'audit'
        self.table = 'logged_actions'
    
    def conn_postgres(self):
        self.conn = create_engine(self.db_connection_url)
        return self.conn
    
    def schemaname(self, schema):
        self.schema_name = schema
    
    def select_postgres(self, days=0, columns='*'):
        
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



if __name__ == '__main__':
    pg = Postgres()
    for i in pg.auditoperation_postgres('I', 7):
        print(i)
    #TODO: Create function to join inserts and updates
