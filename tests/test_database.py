import sqlalchemy
from sqlalchemy import Table, MetaData
from database.postgresql import Postgres


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


def test_if_exists_sync_table():
    """Confirm if sync table was created correctly on postgres"""
    pg = Postgres()
    meta = MetaData(pg.conn_postgres())
    pg.create_sync_table()
    variable = Table('sync_portal', meta, schema='audit')
    
    assert type(variable) == sqlalchemy.sql.schema.Table


def test_inserts_portal():
    """test if insert was send to postgres"""
    pg = Postgres()
    result = [
        {
            'wfs': {
                "workspace": "geonode",
                "layer_name": 'v_xyz_abc',
                "total_features": 1234,
                "readed": 123,
                "feature_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
                "failed": True,
                "fail_log": ['teste']
            },
            'esri_fl': {
                "feature_layer": '/Hosted/Teste_result',
                "id": '0',
                "total_added": 1,
                "added_ids": [11],
                "total_updated": 2,
                "updated_ids": [22, 33],
                "total_deleted": 3,
                "deleted_ids": [44, 55, 66],
                "failed": True,
                "fail_log": [1, 13, 'teste']
            }
        }
    ]
    
    assert type(pg.insert_sync_result(result)) == sqlalchemy.engine.cursor.LegacyCursorResult
