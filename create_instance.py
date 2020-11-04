#!/usr/bin/env python

# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example of using the Compute Engine API to create and delete instances.

Creates a new compute engine instance and uses it to apply a caption to
an image.

    https://cloud.google.com/compute/docs/tutorials/python-guide

For more information, see the README.md under /compute.
"""

import argparse
import os
import time
from google.oauth2 import service_account
import googleapiclient.discovery
from six.moves import input


class CloudAPI:
    def __init__(self):
        self.credential_path = "amogh-batwal-afae20c40eeb.json"
        self.scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self.credentials = service_account.Credentials.from_service_account_file(self.credential_path,
                                                                                 scopes=self.scopes)
        self.service = googleapiclient.discovery.build('compute', 'v1', credentials=self.credentials)
        self.project = "amogh-batwal"
        self.zone = "us-central1-a"

    # [START create_instance]
    def create_instance(self, name, startupscript):
        image_response = self.service.images().getFromFamily(project=self.project,
                                                             family='mapreduce').execute()
        # image_response = compute.images().getFromFamily(project='amogh-batwal', family="ubuntu-1804-lts").execute()
        source_disk_image = image_response['selfLink']

        # Configure the machine
        machine_type = "zones/%s/machineTypes/n1-standard-1" % self.zone
        startup_script = open(
            os.path.join(
                os.path.dirname(__file__), startupscript), 'r').read()


        config = {
            'name': name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                    }
                }
            ],

            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],

            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }]
            }
        }

        return self.service.instances().insert(
            project=self.project,
            zone=self.zone,
            body=config).execute()
    # [END create_instance]

    # [START list_instances]
    def list_instances(self, compute, project, zone):
        result = compute.instances().list(project=project, zone=zone).execute()
        return result['items'] if 'items' in result else None
    # [END list_instances]

    # [START delete_instance]
    def delete_instance(self, name):
        return self.service.instances().delete(
            project=self.project,
            zone=self.zone,
            instance=name).execute()
    # [END delete_instance]

    # [START wait_for_operation]
    def wait_for_operation(self, operation):
        print('Waiting for operation to finish...')
        while True:
            result = self.service.zoneOperations().get(
                project=self.project,
                zone=self.zone,
                operation=operation).execute()

            if result['status'] == 'DONE':
                print("done.")
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)
    # [END wait_for_operation]

    def get_ip(self, instance_name):
        return instance_name['networkInterfaces'][0]['accessConfigs'][0]['natIP']

    def all_instances(self):
        return self.list_instances(self.service, self.project, self.zone)

    # [START run]
    def main(self, startupscript, instance_name, wait=True):
        print('Creating instance.')

        operation = self.create_instance(instance_name, startupscript)
        self.wait_for_operation(operation['name'])

        instances = self.list_instances(self.service, self.project, self.zone)

        print('Instances in project %s and zone %s:' % (self.project, self.zone))
        for instance in instances:
            print(' - ' + instance['name'])
            # print("{0}, IP address: {1}".format(instance, self.get_ip(instance)))

        if wait:
            input()
        else:
            time.sleep(60)

        # print('Deleting instance.')

        # operation = self.delete_instance(instance_name)
        # self.wait_for_operation(operation['name'])

        return
