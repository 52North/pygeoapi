# =================================================================
#
# Authors: Martin Pontius <m.pontius@52north.org>
#          Eike Hinderk Jürrens <e.h.juerrens@52north.org>
#
# Copyright (c) 2022 52°North Spatial Information Research GmbH
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

#
# TODO list below
#
# - decide if job results should be stored in sqlite or file system

import io
import json
import logging
import os

import sqlite3

from elasticsearch_dsl import Q

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.process.manager.base import BaseManager
from pygeoapi.util import JobStatus

LOGGER = logging.getLogger(__name__)


class SQLiteManager(BaseManager):
    """SQLite Manager"""

    def __init__(self, manager_def):
        """
        Initialize object

        :param manager_def: manager definition

        :returns: `pygeoapi.process.manager.base.BaseManager`
        """

        super().__init__(manager_def)
        self.is_async = True

    def _connect(self):

        """
        connect to manager

        :returns: `bool` of status of result
        """
        try:
            self.db = sqlite3.connect(self.connection)
        except sqlite3.OperationalError as err:
            LOGGER.error("Could not connect to database '{}': {}".format(self.connection, err))
            return False
        try:
            self.db.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    identifier          TEXT    PRIMARY KEY,
                    process_id          TEXT    NOT NULL,
                    job_start_datetime  TEXT,
                    job_end_datetime    TEXT,
                    status              TEXT,
                    location            TEXT,
                    mimetype            TEXT,
                    message             TEXT,
                    progress            INTEGER NOT NULL DEFAULT 0
                )''')
            self.db.commit()
        except sqlite3.OperationalError as err:
            LOGGER.error("Could not create schema in database '{}': {}".format(self.connection, err))
            return False
        return True

    def destroy(self):
        """
        Destroy manager

        :returns: `bool` status of result
        """

        try:
            self.db.execute("DELETE FROM jobs")
            self.db.commit()
        except sqlite3.OperationalError as err:
            LOGGER.error("Could not clear table jobs in database '{}': {}".format(self.connection, err))
            return False
        self.db.close()
        return True

    def get_jobs(self, status=None):
        """
        Get jobs

        :param status: job status (accepted, running, successful,
                       failed, results) (default is all)

        :returns: 'list` of job dicts
        """

        self._connect()
        query = 'SELECT * FROM jobs'
        if status:
            jobs_result = self.db.execute(query + " WHERE status IN (:status)", {'status': status}).fetchall()
        else:
            jobs_result = self.db.execute(query).fetchall()
        self.db.close()

        # TODO maybe add error handling
        if len(jobs_result) == 0:
            return []
        else:
            jobs_list = []
            for row in jobs_result:
                jobs_list.append({
                    "identifier": row[0],
                    "process_id": row[1],
                    'job_start_datetime': row[2],
                    'job_end_datetime': row[3],
                    'status': row[4],
                    'location': row[5],
                    'mimetype': row[6],
                    'message': row[7],
                    'progress': row[8]
                })
            return jobs_list

    def get_job(self, job_id):
        """
        Get a single job

        :param job_id: job identifier

        :returns: `dict`  # `pygeoapi.process.manager.Job`
        """

        self._connect()
        job_result = self.db.execute('''
            SELECT
                identifier,
                process_id,
                job_start_datetime,
                job_end_datetime,
                status,
                location,
                mimetype,
                message,
                progress
            FROM jobs
            WHERE identifier IN (:job_id)''',
            {'job_id': job_id}).fetchone()
        self.db.close()
        if not job_result:
            return None
        else:
            return {
                'identifier': job_result[0],
                'process_id': job_result[1],
                'job_start_datetime': job_result[2],
                'job_end_datetime': job_result[3],
                'status': job_result[4],
                'location': job_result[5],
                'mimetype': job_result[6],
                'message': job_result[7],
                'progress': job_result[8]
            }

    def add_job(self, job_metadata):
        """
        Add a job

        By default job_end_datetime, location and mimetype are none,
        hence, we ignore them.

        See base.py:166-177

        :param job_metadata: `dict` of job metadata

        :returns: identifier of added job
        """

        self._connect()
        doc_id = self.db.execute('''
            INSERT INTO jobs (
                identifier,
                process_id,
                job_start_datetime,
                status,
                message,
                progress)
            VALUES (
                :identifier,
                :process_id,
                :job_start_datetime,
                :status,
                :message,
                :progress
            )
            ''',
            job_metadata
            )
        self.db.commit()
        self.db.close()

        return job_metadata['identifier']

    def update_job(self, job_id, update_dict):
        """
        Updates a job

        :param job_id: job identifier
        :param update_dict: `dict` of property updates

        :returns: `bool` of status result
        """

        column_list = ""
        for key, value in update_dict.items():
            if value:
                column_list += key + "= :" + key + ","
        query = "UPDATE jobs SET {} WHERE identifier IN (:job_id)".format(column_list[:-1])
        update_dict["job_id"] = job_id

        self._connect()
        self.db.execute(query, update_dict)
        self.db.commit()
        self.db.close()

        return True

    def delete_job(self, job_id):
        """
        Deletes a job

        :param job_id: job identifier

        :return `bool` of status result
        """
        # delete result file if present
        job_result = self.get_job(job_id)
        if job_result:
            location = job_result.get('location', None)
            if location and self.output_dir is not None:
                os.remove(location)

        self._connect()
        try:
            self.db.execute("DELETE FROM jobs WHERE identifier IN (:job_id)", { "job_id": job_id})
            self.db.commit()
        except sqlite3.OperationalError as err:
            LOGGER.error("Could not delete job '{}' from table jobs in database '{}': {}".format(
                job_id, self.connection, err))
            return False
        self.db.close()

        return True

    def get_job_result(self, job_id):
        """
        Get a job's status, and actual output of executing the process

        :param jobid: job identifier

        :returns: `tuple` of mimetype and raw output
        """

        job_result = self.get_job(job_id)
        if not job_result:
            # job does not exist
            return None

        location = job_result.get('location', None)
        mimetype = job_result.get('mimetype', None)
        job_status = JobStatus[job_result['status']]

        if not job_status == JobStatus.successful:
            # Job is incomplete
            return (None,)
        if not location:
            # Job data was not written for some reason
            # TODO log/raise exception?
            return (None,)

         # TODO use encoding and json.load only if mimetype is requiring it, else load binary
        if mimetype is FORMAT_TYPES[F_JSON]:
            with io.open(location, 'r', encoding='utf-8') as filehandler:
                result = json.load(filehandler)
        else:
            with io.open(location, 'rb') as filehandler:
                result = filehandler.read()

        return mimetype, result

    def __repr__(self):
        return '<SQLiteManager> {}'.format(self.name)
