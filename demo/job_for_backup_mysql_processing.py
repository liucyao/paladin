import os
import time
import traceback 
from base.conf import global_info
from base.util import encryption
from base.util.flush_job_info import flush_job_info
from base.protocal import protocal_info
from fserver.domain.job.job_base_processing import job_base


class job_for_backup_mysql(job_base):

    def __init__(self, message):
        super(job_for_backup_mysql, self).__init__(message=message)
        self.flush_backup_job_info = flush_job_info(self.extend_args['job_id']) 

    def init_resource(self):
        self.fetch_meta_conn()
        self.fetch_monitor_db_conn()
        self.init_ssh_channel()
        self.init_sftp_channel()
        self.parse_extend_args()

    def release_resource(self):
        self.release_meta_conn()
        self.release_monitor_db_conn()
        self.release_sftp_channel()
        self.release_ssh_channel()

    def parse_extend_args(self):
        local_packet_dir = global_info.local_packet_dir 
        remote_packet_dir = global_info.remote_packet_dir 
        self.ctime = time.strftime('%Y%m%d%H%M%S',time.localtime()) 
        self.packet_name = os.popen("ls %s|grep xtrabackup" %local_packet_dir).read().strip()
        self.local_packet_file_path = "%s/%s" %(local_packet_dir, self.packet_name)
        self.remote_packet_file_path = "%s/%s" %(remote_packet_dir, self.packet_name)
        if not self.packet_name:
            raise Exception('xtrabckup packet is not exists under %s' %local_packet_dir)
        
        back_task_res = self.meta_db_conn.execute("select policy,cnf_path,days,backup_path from backup_task_list where db_id=%(db_id)s " %self.db_args).fetchone()
        if not back_task_res:
            raise Exception("instance:%(db_id)s have no backup task."%self.db_args)
        back_task_dict = dict(back_task_res) 
        self.policy = back_task_dict['policy']
        self.bak_dir = back_task_dict['backup_path'] 
        self.days = back_task_dict['days']
        self.db_args['cnf_path'] = back_task_dict['cnf_path']
        self.xtra_tar_name = 'xtrabackup'+'_'+str(self.db_args['dbport'])+'_'+self.ctime+'.tar.gz'
        self.xtra_tar_path = self.bak_dir.rstrip('/')+'/'+self.xtra_tar_name
        self.db_args['xtra_file_path'] = self.xtra_tar_path[:-7]
        self.dump_tar_name = 'mysqldump'+'_'+str(self.db_args['dbport'])+'_'+self.ctime+'.gz'
        self.dump_tar_path = self.bak_dir.rstrip('/')+'/'+self.dump_tar_name
        self.db_args['dump_file_path'] = self.dump_tar_path
        self.get_back_user()

    def init_sftp_channel(self):
        try:
            self.sftp = self.ssh.open_sftp()
        except:
            raise

    def release_sftp_channel(self):
        try:
            self.sftp.close()
        except:
            traceback.print_exc()

    def is_xtrabackup_installed(self):
        stdint, stdout, stderr = self.ssh.exec_command('innobackupex --version')
        if stderr.read():
            return False 
        else:
            return True

    def perl_rpm_libs(self):
        libs_list = ['perl-Time-HiRes','perl-DBD-MySQL']
        for lib in libs_list:
            stdin, stdout, stderr = self.ssh.exec_command("rpm -qa %s" %lib)
            if not stdout.read():
                self.ssh.exec_command("yum -y install %s" %lib)
    
    def get_back_user(self):
        back_user_res = self.meta_db_conn.execute("select user,passwd from q_db_user where ip='%(dbip)s' and dbport=%(dbport)s and user='dbbackup' and host='127.0.0.1' "%self.db_args).fetchone()
        if back_user_res:
            self.db_args['bak_user'] = back_user_res['user']
            self.db_args['bak_user_pwd'] = encryption.decrypte(encrypted_string=back_user_res['passwd'])
            self.db_args['ctime'] = self.ctime
        else:
            raise Exception("not set backup user:dbbackup on instance:%(db_id)s" %self.db_args)

    def install_xtrabackup(self):
        try:
            self.sftp.put(localpath=self.local_packet_file_path, remotepath=self.remote_packet_file_path)
            stdin, stdout, stderr = self.ssh.exec_command("tar zxvf %s -C /opt && rm -rf %s" %(self.remote_packet_file_path, self.remote_packet_file_path))
            self.perl_rpm_libs()
            self.ssh.exec_command("ln -s /opt/%s/bin/innobackupex /usr/bin/innobackupex" %self.packet_name[0:-7])
        except:
            raise

    def flush_back_done_info(self, bak_dict):
        flush_back_sql = '''insert into backup_done_list(dbip,dbport,start_time,end_time,target_path,packet_name,backup_size,binlog_file,binlog_pos,lsn,task_type,backup_method,backup_type,is_ok,prepare_status) \
                         values ('%(dbip)s',%(dbport)s,'%(start_time)s',NOW(),'%(tar_path)s','%(tar_name)s','%(bakSize)s','%(binlog_file)s',%(binlog_pos)s,%(bakLsn)s,'%(task_type)s','%(method)s','%(backup_type)s','%(is_ok)s','%(prepare_status)s')''' %(bak_dict)
        self.meta_db_conn.execute(flush_back_sql)

    def xtrabackup(self):
        if not self.is_xtrabackup_installed():
            self.install_xtrabackup()
        
        bak_dict = {}
        bak_dict['dbip'] = self.db_args['dbip']
        bak_dict['dbport'] = self.db_args['dbport']
        bak_dict['method'] = 'xtrabackup'
        bak_dict['task_type'] = 'auto'
        bak_dict['backup_type'] = 'full'
        bak_dict['start_time'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        
        innoback_cmd = "innobackupex --user=%(bak_user)s --password=%(bak_user_pwd)s --port=%(dbport)s --host=127.0.0.1 --defaults-file=%(cnf_path)s --defaults-group=mysqld%(dbport)s --no-timestamp %(xtra_file_path)s"\
                    %(self.db_args)
        stdin, stdout, stderr = self.ssh.exec_command(innoback_cmd)
        back_call_back = stderr.read()
        if back_call_back.split(':')[-1].strip('\n').lstrip(' ') == "completed OK!":
            bak_dict['is_ok'] = 'Y'
            prepare_cmd = "innobackupex --apply-log --user=%(bak_user)s --password=%(bak_user_pwd)s --port=%(dbport)s --host=127.0.0.1 --defaults-file=%(cnf_path)s --defaults-group=mysqld%(dbport)s %(xtra_file_path)s"\
                    %(self.db_args)
            stdin, stdout, stderr = self.ssh.exec_command(prepare_cmd)
            prepare_call_back = stderr.read()
            if prepare_call_back.split(':')[-1].strip('\n').lstrip(' ') == "completed OK!":
                bak_dict['prepare_status'] = 'Y'
                stdin, stdout, stderr = self.ssh.exec_command("cd %s && tar cfz %s %s" %(self.bak_dir,self.xtra_tar_name,self.xtra_tar_name[:-7]))
                stdout.read()
                stdin, stdout, stderr = self.ssh.exec_command("du -sh %s|awk '{print $1}'" %self.xtra_tar_path, get_pty=True)
                bak_dict['bakSize'] = stdout.read().strip('\n').strip('\r')
                stdin, stdout, stderr = self.ssh.exec_command("cat %s/xtrabackup_checkpoints|grep to_lsn|awk '{print $3}'" %self.xtra_tar_path[:-7])
                bak_dict['bakLsn'] = stdout.read().strip('\n')
                stdin, stdout, stderr = self.ssh.exec_command("cat %s/xtrabackup_binlog_info|awk '{print $1,$2}'" %self.xtra_tar_path[:-7])
                binlog_file_pos = stdout.read().strip('\n').split(' ')
                bak_dict['tar_name'] = self.xtra_tar_name
                bak_dict['tar_path'] = self.xtra_tar_path
                bak_dict['binlog_file'] = binlog_file_pos[0]
                bak_dict['binlog_pos'] = binlog_file_pos[1]
                self.ssh.exec_command("cd %s && rm -rf %s" %(self.bak_dir,self.xtra_tar_name[:-7])) 
            else:
                bak_dict['prepare_status'] = 'N'
                bak_dict['bakSize'] = 0
                bak_dict['bakLsn'] = 0
                bak_dict['tar_name'] = ''
                bak_dict['tar_path'] = ''
                bak_dict['binlog_file'] = ''
                bak_dict['binlog_pos'] = 0
        else:
            bak_dict['is_ok'] = 'N'
            bak_dict['prepare_status'] = 'N'
            bak_dict['bakSize'] = 0
            bak_dict['bakLsn'] = 0
            bak_dict['tar_name'] = ''
            bak_dict['tar_path'] = ''
            bak_dict['binlog_file'] = ''
            bak_dict['binlog_pos'] = 0
        self.flush_back_done_info(bak_dict)
   
    def mysqldump(self):
        variables = self.monitor_db_conn.execute("show variables like 'basedir' ").fetchone()
        self.db_args['basedir'] = variables['Value']
        dump_sql = "%(basedir)s/bin/mysqldump -u%(bak_user)s -p%(bak_user_pwd)s -h127.0.0.1 -P%(dbport)s --single-transaction --master-data=2 --events --all-databases|gzip>%(dump_file_path)s" %(self.db_args)
        stdin, stdout, stderr = self.ssh.exec_command(dump_sql)
        
        bak_dict = {}
        bak_dict['dbip'] = self.db_args['dbip']
        bak_dict['dbport'] = self.db_args['dbport']
        bak_dict['method'] = 'mysqldump'
        bak_dict['task_type'] = 'auto'
        bak_dict['backup_type'] = 'full'
        bak_dict['start_time'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        if not stderr.read():
            bak_dict['is_ok'] = 'Y'
            stdin, stdout, stderr = self.ssh.exec_command("du -sh %s|awk '{print $1}'" %self.dump_tar_path)
            bak_dict['bakSize'] = stdout.read()
            bak_dict['tar_name'] = self.dump_tar_name
            bak_dict['tar_path'] = self.dump_tar_path 
        else:
            bak_dict['is_ok'] = 'N'
            bak_dict['bakSize'] = 0
            bak_dict['tar_name'] = ''
            bak_dict['tar_path'] = ''
        bak_dict['prepare_status'] = ''
        bak_dict['bakLsn'] = 0
        bak_dict['binlog_file'] = ''
        bak_dict['binlog_pos'] = 0
        self.flush_back_done_info(bak_dict)   

    def backup_mysql(self):
        try:
            self.init_resource()
            if self.policy == 1:
                self.xtrabackup()
            elif self.policy == 2:
                self.mysqldump()
            elif self.policy == 3:
                self.xtrabackup()
                self.mysqldump()
            else:
                raise Exception('not supper backup policy!')
            self.release_resource()
        except Exception as e:
            traceback.print_exc()
            self.flush_backup_job_info.flush_metadb_status_for_backup_job(job_status=protocal_info.ERROR_JOB_STATUS,job_status_detail=str(e))
        else:
            self.flush_backup_job_info.flush_metadb_status_for_backup_job()

    def job(self):
        self.backup_mysql()