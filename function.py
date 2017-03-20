# coding=utf-8
import os
import hashlib
import sqlite3
import time
import datetime
import sys
from common_lib import get_dir_depth


gDataBaseFileName = 'hash_record_db.db'
gRecordDirDepth = 3
gStoreDataBasePath = os.path.join(os.getcwd(), gDataBaseFileName)


class SqlLibHandle:
    def __init__(self):
        self.init_flag = False
        self.sql_count = 0
        self.set_max_buf_sql_cnt = 200
        pass

    def load(self, db_file_path):
        if not os.path.exists(db_file_path):
            self.dbHandle = self.init_db(db_file_path)
        else:
            self.dbHandle = sqlite3.connect(db_file_path)
        self.dbHandle.text_factory = str
        self.init_flag = True
        pass

    def init_db(self, db_file_path):
        db_handle = sqlite3.connect(db_file_path)
        db_handle.execute('CREATE TABLE hash_record(\
          full_file_path TEXT PRIMARY KEY ,\
          hash_code CHAR ,\
          description TEXT ,\
          file_size INT,\
          same_count INT)')
        db_handle.commit()
        db_handle.execute('CREATE INDEX hash_record_index on hash_record(full_file_path, hash_code)')

        db_handle.execute('CREATE TABLE scan_record(\
          folder_path TEXT PRIMARY KEY ,\
          depth INT ,\
          scan_time DATETIME,\
          state INT)')
        db_handle.commit()
        db_handle.execute('CREATE INDEX scan_record_index on scan_record(folder_path)')

        db_handle.commit()
        return db_handle
        pass

    def store_file_record(self, full_file_path, hash_code, description=None):
        if not self.init_flag:
            self.err_output('load db first')
            return
        if 'nt' == os.name:
            full_file_path = unicode(full_file_path.decode('gbk'))
        try:
            dup_file_list = self.check_hash_dup(full_file_path, hash_code)
            dup_file_list = list()
            same_count = len(dup_file_list)
            if same_count != 0:
                same_count += 1
            for dup_file_path in dup_file_list:
                self.update_file_same_count(dup_file_path, same_count)
            stat = os.stat(full_file_path)
            file_size = stat.st_size
            self.dbHandle.execute('INSERT OR REPLACE INTO hash_record VALUES (?,?,?,?,?)',
                                (full_file_path, hash_code, description, file_size, same_count))
        except sqlite3.DataError:
            self.err_output('DataError')
            return
        self.sql_count += 1
        if self.sql_count >= self.set_max_buf_sql_cnt:
            self.sql_count = 0
            self.dbHandle.commit()
        pass

    def update_file_same_count(self, file_path, same_count):
        self.dbHandle.execute('update hash_record set same_count = (?) \
                                where hash_record.full_file_path == (?)',
                              (same_count, file_path))
        self.dbHandle.commit()
        pass

    def do_commit(self):
        if self.sql_count > 0:
            self.dbHandle.commit ()
        pass

    def check_hash_dup(self, file_path, hash_code):
        dup_file_list = list()
        con = self.dbHandle.execute('select * from hash_record where hash_record.hash_code == (?) \
                                  and hash_record.full_file_path != (?)', (hash_code, file_path))
        if con.rowcount != 0:
            dup_tuple_list = con.fetchall()
            for tuple_row in dup_tuple_list:
                (file_path,_,_,_,_) = tuple_row
                dup_file_list.append(file_path)
                print 'append same [%s]' % file_path.decode('utf-8')
        return dup_file_list

        pass

    # state 0:scanning 1:already scaned
    def store_scan_record(self, full_folder_path, state):
        if not self.init_flag:
            self.err_output('load db first')
            return False
        depth = get_dir_depth(full_folder_path)
        time_now = datetime.datetime.now()
        self.dbHandle.execute('INSERT OR REPLACE INTO scan_record VALUES (?,?,?,?)',
                              (full_folder_path, depth, time_now, state))
        self.dbHandle.commit()
        pass

    def get_scan_record(self, full_folder_path):
        con = self.dbHandle.execute('select * from scan_record where scan_record.folder_path == (?)', (full_folder_path,))
        ret_dict = dict()
        if con.rowcount != 0:
            (full_folder_path, depth, time_now, state) = con.fetchall()[0]
            ret_dict['full_folder_path'] = full_folder_path
            ret_dict['depth'] = depth
            ret_dict['time_now'] = time_now
            ret_dict['state'] = state
            return ret_dict
        return None


        pass


    # always return list
    def get_scan_record_scanning(self):
        if not self.init_flag:
            self.err_output('load db first')
            return list()
        con = self.dbHandle.execute('select * from scan_record where scan_record.state = 0')
        ret_list = con.fetchall()
        print type(ret_list)
        print ret_list

        pass

    def err_output(self, str_info):
        print str_info
        pass

    def get_by_hash(self):
        pass

    def get_by_file(self, file_path):
        resault = self.dbHandle.execute('select * from hash_record where hash_record.full_file_path=?',
                                        file_path)
        ret_list_dict = list()
        if resault.rowcount:
            ret = resault.fetchall()
            print type(ret)
            print ret
        pass

    def example(self):
        db_handle = SqlLibHandle()
        hash_handle = HashHandle()
        db_path = os.path.join('other', 'hash_record_db.db')
        db_handle.load(db_path)
        for dir_path, dir_names, file_names in os.walk(os.getcwd()):
            for file_name in file_names:
                full_file_path = os.path.join(dir_path, file_name)
                hash_code = hash_handle.get_file_hash(full_file_path)
                db_handle.store_file_record(full_file_path, hash_code)
                db_handle.store_scan_record(full_file_path, 0)
        last_scan = db_handle.get_scan_record_scanning()

    def check_record(self):
        pass


class HashHandle:
    def __init__(self):
        self.scan_depth = -1
        self.full_scan_max_size = 10*1024*1024
        self.sample_size = self.full_scan_max_size // 2
        pass

    def get_file_hash(self, file_path):
        if os.path.exists(file_path):
            stat = os.stat(file_path)
            file_size = stat.st_size
            m = hashlib.md5()
            with open(file_path, 'rb') as fd:
                if file_size >= self.full_scan_max_size:
                    buf = fd.read(self.sample_size)
                    m.update(buf)
                    if file_size > self.full_scan_max_size *2:
                        tail_size = self.full_scan_max_size
                    else:
                        tail_size = file_size - self.full_scan_max_size
                    fd.seek(-tail_size, os.SEEK_END)
                    buf = fd.read(self.sample_size)
                    m.update(buf)
                else:
                    buf = fd.read(self.sample_size)
                    m.update(buf)
            return m.hexdigest()
        else:
            self.err_output('file not exist! [%s]' % file_path)

    def err_output(self, err_msg):
        print err_msg
        pass


def scan_folder_recursion(folder_path):
    global gDataBaseFileName
    global gRecordDirDepth
    record_dir_depth = gRecordDirDepth
    folder_list = list()
    if type(folder_path) == list:
        for folder in folder_path:
            folder_list.append(folder)
    else:
        folder_list.append(folder_path)
    db_handle = SqlLibHandle()
    hash_handle = HashHandle()
    db_handle.load(gDataBaseFileName)

    for folder_scan in folder_list:
        print 'begin scan [%s]' % folder_scan
        dir_depth = get_dir_depth(folder_scan)
        if dir_depth < record_dir_depth:
            db_handle.store_scan_record(folder_scan, 0)
        start_time = time.time()
        files_count = 0
        for dir_path, dir_names, file_names in os.walk(folder_path):
            print 'scan [%s] , subdir cnt [%d] files cnt [%d]' % (dir_path, len(dir_names), len(file_names))
            dir_depth = get_dir_depth(dir_path)
            if dir_depth == record_dir_depth:
                db_handle.store_scan_record(dir_path, 0)
            for file_name in file_names:
                full_file_path = os.path.join(dir_path, file_name)
                tmp_time_rec = time.time()
                hash_code = hash_handle.get_file_hash(full_file_path)
                #print 'get hash time use [%f]' % (time.time() - tmp_time_rec)
                tmp_time_rec = time.time ()
                db_handle.store_file_record(full_file_path, hash_code)
                #print 'store into time use [%f]' % (time.time () - tmp_time_rec)
                files_count += 1
            db_handle.do_commit()
            if dir_depth == record_dir_depth:
                db_handle.store_scan_record(dir_path, 1)
        dir_depth = get_dir_depth(folder_scan)
        if dir_depth < record_dir_depth:
            db_handle.store_scan_record(folder_scan, 1)
        rec_time = time.time() - start_time
        print 'scan [%s] done, total file count [%d], time use [%f]' % (folder_scan, files_count, rec_time)
    pass


if __name__ == '__main__':
    if len(sys.argv) == 2:
        scan_folder_recursion(sys.argv[1])
        pass



