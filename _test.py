from dbmasta.authorization import Authorization
from dbmasta.async_db_client import AsyncDataBase
from dotenv import load_dotenv
import os
load_dotenv()



async def test():
    
    USERNAME = os.getenv("DB_USERNAME")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    DB = os.getenv("DB_DATABASE")
    
    # test async
    auth = Authorization(
        username= USERNAME,
        password= PASSWORD,
        host= HOST,
        default_database= DB,
        engine= "asyncmy",
    )
    db = AsyncDataBase(auth=auth, engine_config={"pool_recycle": 3600})
    try:
        dbr = await db.run("show tables;", 'accounting')
        for row in dbr:
            print(row)
    except Exception as e:
        print(e)
    finally:
        await db.kill_engines()