### description

# in 24hours, create app which which meets following specs

Create postgres table, every time row value of specific column is updated send pg_notification. Run notification consumer, on every notification alter given row in database (update timestamp field with current date) and query all results in table, write every single result to a file ./output.log

requirements: 
 * Linux os (tested with debian 10)
 * python 3 (tested with 3.7)
 * Postgresql (tested with 11.7)

 howto run app (socket,select, threading):
  * install python dependencies from requirements.txt
  * setup database (./db_env.txt)
  * edit db user/pw and db server location in ./chall/constants.py (DSN)
  * execute ./run_app.py, the script will create needed table and start listening for the notifications on 'item_change' channel.
  * optionally, run ./run_create_update_records.py in diferent terminal for bulk create and update of records (trigger notification). 

 howto run app (asyncio):
  * same steps as above
  * enable ASYNC_CONSUMER in ./chall/constants.py (ASYNC_CONSUMER=True)
  * specify DSN_AIOPG in ./chall/constants.py



