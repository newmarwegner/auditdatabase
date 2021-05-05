import sqlalchemy
from database.Postgresql import Postgres


def test_conn_sqlalchemy():
    """Confirm a Postgresql database connection after create engine"""
    pg = Postgres()
    
    assert type(pg.conn_postgres()) == sqlalchemy.engine.base.Engine


def test_select_postgres_table():
    """Return a Select a Postgresql table in list sequence"""
    # columns = ['schemaname', 'tablename']
    pg = Postgres()
    
    assert pg.select_postgres() is not None


def test_need_update():
    """Return list of update features"""
    pg = Postgres()
    choice = 'U'
    result = set([i[5] for i in pg.auditoperation_postgres(choice)])
    
    assert result == {'U'} or result == set()


def test_need_insert():
    """Return list of update features"""
    pg = Postgres()
    choice = 'I'
    result = set([i[5] for i in pg.auditoperation_postgres(choice)])
    
    assert result == {'I'} or result == set()


def test_need_delete():
    """Return list of update features"""
    pg = Postgres()
    choice = 'D'
    result = set([i[5] for i in pg.auditoperation_postgres(choice)])
    
    assert result == {'D'} or result == set()
