from django.db import models

# Create your models here.
class ServiceInstanceRecord:
    def __init__(
            self,
            lcs_id,
            jira,
            sid,
            sname,
            aid,
            aname,
            iid,
            iname,
            env,
            install_type,
            parent_app_id=None,
            app_type=None,
            app_tier=None,
            arch_type=None
    ):
        self.lean_control_service_id = lcs_id
        self.jira_backlog_id = jira
        self.service_id = sid
        self.service_name = sname
        self.app_id = aid
        self.app_name = aname
        self.instance_id = iid
        self.instance_name = iname
        self.environment = env
        self.install_type = install_type
        self.parent_app_id = parent_app_id
        self.application_type = app_type
        self.application_tier = app_tier
        self.architecture_type = arch_type
