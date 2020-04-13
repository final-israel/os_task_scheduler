# os_task_scheduler
Library for spinning up Openstack stacks and scheduling tasks to be perfomed on them. This library attempts to create stacks to meet the job demand striking a balance between avilable resource quotas and jobs waiting execution. Adopting a greedy approach - keep create stacks until all jobs are done or until all avilable resources are consumed (stack error reason indicate limit reached)

