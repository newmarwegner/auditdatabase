# Audit tables postgres and register sync  
Script to be use for audit inserts, updates or deletes on postgres tables.

## Steps to execute:
1. Open terminal
2. Clone Repo
3. Create a virtualenv (python 3.8.8)  
4. Go into /auditdatabase
5. Copy ~.env-sample of contrib folder into auditdatabase folder and rename to .env
6. Fill the parameter in .env file   
7. Install the requirements packages
8. Create audit schema and tables
9. Run tests with pytest   
10. Begin use Postgresql.py module

## Codes
```
git clone https://github.com/newmarwegner/auditdatabase.git
cd /auditdatabase
python -m venv .venv
mv /auditdatabase/contrib/~.env-sample .env
source .venv/bin/activate
pip install -r requirements.txt
```
