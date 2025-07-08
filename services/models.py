from django.db import models

# Create your models here.
class ServiceInstanceRecord:
    def __init__(self, lean_control_service_id, jira_backlog_id, service_id, service_name,
                 app_id, app_name, instance_id, instance_name, environment, install_type):
        self.lean_control_service_id = lean_control_service_id
        self.jira_backlog_id = jira_backlog_id
        self.service_id = service_id
        self.service_name = service_name
        self.app_id = app_id
        self.app_name = app_name
        self.instance_id = instance_id
        self.instance_name = instance_name
        self.environment = environment
        self.install_type = install_type