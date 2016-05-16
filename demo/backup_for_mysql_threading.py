import json
from base.log import log 
from base.util import schedule
from fserver.domain.backup.backup_base_threading import backup_base

class backup_for_mysql(backup_base):
    
    def __init__(self, db_args = None, meta_db_pool = None):
        super(backup_for_mysql, self).__init__(db_args=db_args, meta_db_pool=meta_db_pool)
        self.db_parameters = {}

    def init_env(self):
        self.init_job()

    def init_job(self):
        schedule_tmp = schedule.Scheduler()
        first_running_job_dict_tmp = {}
        result = self.meta_db_conn.execute('select * from backup_task_list where db_id = %(db_id)s ' % self.db_args).fetchone()
        if not result:
            raise Exception('db_id:%(db_id)s have no backup job' %self.db_args)
        job_id = result['id']
        parameter = json.loads(result['parameter'])
        parameter['job_id'] = job_id 
        time = parameter["time"]
        schedule_tmp.every(interval=1).days.at('{time}'.format(time=time)).do(self.job_func,parameter)
        if parameter['first_running']:
            first_running_job_dict_tmp[parameter['job_name']] = parameter
        
        self.first_running_job_dict = first_running_job_dict_tmp
        self.schedule = schedule_tmp

    def main(self):
        try:
            if self.first_run:
                self.fetch_meta_conn()
                self.create_monitor_db_pool()
                self.fetch_monitor_conn()
                self.init_env()
            self.scheduler_job()
        except Exception as e:
            log.info(str(e))
        else:
            self.first_run_changed()