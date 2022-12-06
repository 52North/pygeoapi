#MongoDB Job-Manager
from pygeoapi.process.manager.base import BaseManager
from pygeoapi.util import JobStatus
from pymongo import MongoClient

class MongoDBManager(BaseManager):
    def __init__(self, manager_def):
        super().__init__(manager_def)
        self.is_async = True

    def _connect(self):
        #connection in CFG would be something like mongodb://localhost:27017/
        try:
            client = MongoClient(self.connection)
            self.db = client
            return True
        except:
            return False

    def destroy(self):
        try:
            self.db.close()
            return True
        except:
            return False

    def get_jobs(self, status=None):
        try:
            database = self.db.job_manager_pygeoapi
            collection = database.jobs

            return collection
        except:
            return False

    def add_job(self, job_metadata):
        pass

    def update_job(self, job_id, update_dict):
        pass

    def delete_job(self, job_id):
        pass

    def get_job(self, job_id):
        pass

    def get_job_result(self, job_id):
        pass

    def __repr__(self):
        return '<MongoDBManager> {}'.format(self.name)
