
# =================================================================
#
# Authors: Eike Hinderk Jürrens <e.h.juerrens@52north.org>
#
# Copyright (c) 2022 52North Spatial Information Research GmbH
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the 'Software'), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
import pytest

# required because of https://github.com/flexxui/pscript/issues/50#issuecomment-750875154
import base64
base64.encodestring = base64.encodebytes
base64.decodestring = base64.decodebytes

from pygeoapi.process.manager.sqlite_ import SQLiteManager
from pygeoapi.util import JobStatus

job_1_metadata = {
    'identifier': 'test_1_identifier',
    'process_id': 'test_1_process_id',
    'job_start_datetime': '1111-11-11T12:45:10.378373Z',
    'job_end_datetime': None,
    'status': JobStatus.accepted.value,
    'location': None,
    'mimetype': None,
    'message': 'test-1-message',
    'progress': 11
}

job_2_metadata = {
    'identifier': 'test_2_identifier',
    'process_id': 'test_2_process_id',
    'job_start_datetime': '2222-02-02T12:45:10.378373Z',
    'job_end_datetime': None,
    'status': JobStatus.running.value,
    'location': None,
    'mimetype': None,
    'message': 'test-2-message',
    'progress': 22
}


@pytest.fixture()
def config(tmpdir, tmp_path):
    output_dir = tmpdir.mkdir('pygeoapi-process-outputs/')
    connection = tmp_path / 'test.sqlite3'
    return {
        'name': 'SQLite',
        'connection': connection,
        'output_dir': output_dir
    }


def test_get_jobs_is_empty(config):
    manager = SQLiteManager(config)

    assert len(manager.get_jobs()) == 0


def test_add_and_get_job(config):
    manager = SQLiteManager(config)

    job_id = manager.add_job(job_1_metadata)

    added_job = manager.get_job(job_id)

    assert job_id == job_1_metadata['identifier']

    _assert_job_equality(added_job, job_1_metadata)


def _assert_job_equality(job_a, job_2):
    assert job_a['identifier'] == job_2['identifier']
    assert job_a['process_id'] == job_2['process_id']
    assert job_a['job_start_datetime'] == job_2['job_start_datetime']
    assert job_a['job_end_datetime'] == job_2['job_end_datetime']
    assert job_a['status'] == job_2['status']
    assert job_a['location'] == job_2['location']
    assert job_a['mimetype'] == job_2['mimetype']
    assert job_a['message'] == job_2['message']
    assert job_a['progress'] == job_2['progress']


def test_update_job(config):
    manager = SQLiteManager(config)

    job_id = manager.add_job(job_1_metadata)
    manager.add_job(job_2_metadata)

    updated_msg = 'updated-test-1-message'
    manager.update_job(job_id, {'message': updated_msg})

    updated_job = manager.get_job(job_id)

    assert updated_job['message'] == updated_msg
    assert updated_job['identifier'] == job_1_metadata['identifier']
    assert updated_job['process_id'] == job_1_metadata['process_id']
    assert updated_job['job_start_datetime'] == job_1_metadata['job_start_datetime']
    assert updated_job['job_end_datetime'] == job_1_metadata['job_end_datetime']
    assert updated_job['status'] == job_1_metadata['status']
    assert updated_job['location'] == job_1_metadata['location']
    assert updated_job['mimetype'] == job_1_metadata['mimetype']
    assert updated_job['progress'] == job_1_metadata['progress']


def test_get_jobs_with_status(config):
    manager = SQLiteManager(config)

    manager.add_job(job_1_metadata)
    manager.add_job(job_2_metadata)

    jobs = manager.get_jobs(status=JobStatus.accepted.value)
    assert len(jobs) == 1
    _assert_job_equality(jobs[0], job_1_metadata)


def test_delete_job(config):
    manager = SQLiteManager(config)

    manager.add_job(job_1_metadata)
    manager.add_job(job_2_metadata)

    manager.delete_job(job_1_metadata['identifier'])

    jobs = manager.get_jobs()
    assert len(jobs) == 1
    _assert_job_equality(manager.get_job(jobs[0]['identifier']), job_2_metadata)
